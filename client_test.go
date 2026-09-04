package socketmode

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// fakeSlack stands in for Slack: it serves apps.connections.open and then
// speaks Socket Mode over a real WebSocket. Everything here runs against an
// actual connection rather than a mock, so the ping/pong and reconnect paths
// are exercised for real.
type fakeSlack struct {
	t *testing.T

	httpSrv *httptest.Server
	wsSrv   *httptest.Server

	mu        sync.Mutex
	acks      []ackMessage
	opens     int
	conns     int
	openError string // when set, apps.connections.open fails with this code

	// onConnect runs on the server side once a client connects.
	onConnect func(ws *websocket.Conn)
}

func newFakeSlack(t *testing.T) *fakeSlack {
	t.Helper()
	f := &fakeSlack{t: t}

	up := websocket.Upgrader{}
	f.wsSrv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()

		f.mu.Lock()
		f.conns++
		f.mu.Unlock()

		// Slack greets first.
		_ = ws.WriteJSON(map[string]any{
			"type": "hello", "num_connections": 1,
		})

		if f.onConnect != nil {
			f.onConnect(ws)
		}

		// Drain acknowledgements until the client goes away.
		for {
			var m ackMessage
			if err := ws.ReadJSON(&m); err != nil {
				return
			}
			f.mu.Lock()
			f.acks = append(f.acks, m)
			f.mu.Unlock()
		}
	}))

	f.httpSrv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.opens++
		errCode := f.openError
		f.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		if errCode != "" {
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": errCode})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":  true,
			"url": "ws" + strings.TrimPrefix(f.wsSrv.URL, "http"),
		})
	}))

	t.Cleanup(func() {
		f.httpSrv.Close()
		f.wsSrv.Close()
	})
	return f
}

// client builds a Client wired to the fake, with timings tightened so tests
// finish in milliseconds rather than seconds.
func (f *fakeSlack) client(t *testing.T, tune func(*Options)) *Client {
	t.Helper()
	opt := Options{
		AppToken:          "xapp-test",
		ClientPingTimeout: 300 * time.Millisecond,
		ServerPingTimeout: 900 * time.Millisecond,
		ReconnectStep:     20 * time.Millisecond,
		MaxReconnectDelay: 60 * time.Millisecond,
		Logger:            NewLogger(io.Discard, LogDebug),
		HTTPClient:        f.httpSrv.Client(),
	}
	if tune != nil {
		tune(&opt)
	}
	c, err := New(opt)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Point apps.connections.open at the fake.
	c.opt.HTTPClient = &http.Client{
		Transport: rewriteHost{to: f.httpSrv.URL, base: f.httpSrv.Client().Transport},
	}
	return c
}

// rewriteHost sends every request to the fake server regardless of URL.
type rewriteHost struct {
	to   string
	base http.RoundTripper
}

func (r rewriteHost) RoundTrip(req *http.Request) (*http.Response, error) {
	u := *req.URL
	target, _ := http.NewRequest(req.Method, r.to, req.Body)
	u.Scheme, u.Host = target.URL.Scheme, target.URL.Host
	req = req.Clone(req.Context())
	req.URL = &u
	req.Host = u.Host
	base := r.base
	if base == nil {
		base = http.DefaultTransport
	}
	return base.RoundTrip(req)
}

func (f *fakeSlack) ackCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.acks)
}

func (f *fakeSlack) connCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.conns
}

// waitFor polls until cond holds or the deadline passes.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// ── connection lifecycle ────────────────────────────────────────────────

func TestConnectsAndReportsLifecycle(t *testing.T) {
	f := newFakeSlack(t)
	c := f.client(t, nil)

	var mu sync.Mutex
	var seen []string
	for _, name := range []string{EventConnecting, EventAuthenticated, EventConnected} {
		n := name
		c.On(n, func(Event) {
			mu.Lock()
			seen = append(seen, n)
			mu.Unlock()
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Start(ctx)

	waitFor(t, "the three lifecycle events", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) == 3
	})

	mu.Lock()
	got := strings.Join(seen, ",")
	mu.Unlock()
	want := strings.Join([]string{EventConnecting, EventAuthenticated, EventConnected}, ",")
	if got != want {
		t.Errorf("lifecycle order was %q; want %q", got, want)
	}

	if info := c.ConnectionInfo(); len(info) == 0 {
		t.Error("connection info was not captured")
	}
}

// ── one envelope, three deliveries ──────────────────────────────────────
//
// This mirrors the Node client exactly: an events_api envelope arrives under
// its inner Slack type, under its envelope type, and under EventSlack.

func TestEnvelopeIsDeliveredThreeWays(t *testing.T) {
	f := newFakeSlack(t)
	f.onConnect = func(ws *websocket.Conn) {
		_ = ws.WriteJSON(map[string]any{
			"type":        "events_api",
			"envelope_id": "env-1",
			"payload": map[string]any{
				"event": map[string]any{
					"type": "message", "text": "hello", "channel": "C1", "ts": "1.0",
				},
			},
			"accepts_response_payload": false,
		})
	}

	c := f.client(t, nil)

	var mu sync.Mutex
	hits := map[string]Event{}
	record := func(key string) Handler {
		return func(e Event) {
			mu.Lock()
			hits[key] = e
			mu.Unlock()
		}
	}
	c.On("message", record("inner"))
	c.On(EnvelopeEventsAPI, record("envelope"))
	c.On(EventSlack, record("all"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Start(ctx)

	waitFor(t, "all three deliveries", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(hits) == 3
	})

	mu.Lock()
	defer mu.Unlock()
	for _, key := range []string{"inner", "envelope", "all"} {
		e := hits[key]
		if e.EnvelopeID != "env-1" {
			t.Errorf("%s: envelope id was %q", key, e.EnvelopeID)
		}
		if e.Type != EnvelopeEventsAPI {
			t.Errorf("%s: type was %q; want %q", key, e.Type, EnvelopeEventsAPI)
		}
		if e.InnerType != "message" {
			t.Errorf("%s: inner type was %q; want message", key, e.InnerType)
		}
	}

	// The raw payload survives intact, including fields no struct declares.
	var inner map[string]any
	if err := json.Unmarshal(hits["inner"].Inner, &inner); err != nil {
		t.Fatalf("inner event was not valid JSON: %v", err)
	}
	if inner["text"] != "hello" {
		t.Errorf("inner event lost its text: %v", inner)
	}
}

// ── acknowledgement ─────────────────────────────────────────────────────

func TestAckReachesSlack(t *testing.T) {
	f := newFakeSlack(t)
	f.onConnect = func(ws *websocket.Conn) {
		_ = ws.WriteJSON(map[string]any{
			"type": "interactive", "envelope_id": "env-ack",
			"payload": map[string]any{"actions": []any{}},
		})
	}

	c := f.client(t, nil)
	c.On(EnvelopeInteractive, func(e Event) {
		if err := e.Ack(); err != nil {
			t.Errorf("Ack failed: %v", err)
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Start(ctx)

	waitFor(t, "the acknowledgement", func() bool { return f.ackCount() > 0 })

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.acks[0].EnvelopeID != "env-ack" {
		t.Errorf("acknowledged %q; want env-ack", f.acks[0].EnvelopeID)
	}
}

// ── Slack asking us to reconnect ────────────────────────────────────────
//
// 🚨 Slack recycles connections every few hours. Treating that as a failure
//
//	rather than as routine is how a client ends up flapping.

func TestReconnectsWhenSlackAsks(t *testing.T) {
	f := newFakeSlack(t)
	var once sync.Once
	f.onConnect = func(ws *websocket.Conn) {
		once.Do(func() {
			_ = ws.WriteJSON(map[string]any{
				"type": "disconnect", "reason": "refresh_requested",
			})
		})
	}

	c := f.client(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Start(ctx)

	waitFor(t, "a second connection", func() bool { return f.connCount() >= 2 })
}

// ── unrecoverable errors stop, recoverable ones retry ───────────────────

func TestStopsOnUnrecoverableAuthError(t *testing.T) {
	f := newFakeSlack(t)
	f.openError = "invalid_auth"

	c := f.client(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := c.Start(ctx)
	if err == nil {
		t.Fatal("Start returned no error on invalid_auth")
	}
	var se *Error
	if !errorsAs(err, &se) || se.Code != ErrPlatform {
		t.Fatalf("error was %v; want a platform error", err)
	}
	if f.opens != 1 {
		t.Errorf("apps.connections.open was called %d times; retrying an invalid token is pointless", f.opens)
	}
}

func TestRetriesRecoverableOpenError(t *testing.T) {
	f := newFakeSlack(t)
	f.openError = "service_unavailable"

	c := f.client(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	_ = c.Start(ctx)

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.opens < 2 {
		t.Errorf("apps.connections.open was called %d times; a temporary failure should be retried", f.opens)
	}
}

// ── reconnect delay is capped ───────────────────────────────────────────
//
// 🔑 This is the behaviour the Node client lacks: its delay is step × failures
//
//	with no ceiling, so a long outage leaves it asleep well past recovery.

func TestReconnectDelayIsCapped(t *testing.T) {
	f := newFakeSlack(t)
	c := f.client(t, func(o *Options) {
		o.ReconnectStep = 10 * time.Millisecond
		o.MaxReconnectDelay = 25 * time.Millisecond
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, failures := range []int{1, 2, 5, 100, 10000} {
		start := time.Now()
		if !c.waitBeforeRetry(ctx, failures) {
			t.Fatal("wait was interrupted")
		}
		elapsed := time.Since(start)
		if elapsed > 200*time.Millisecond {
			t.Fatalf("after %d failures the wait was %s; the cap did not apply", failures, elapsed)
		}
	}
}

// ── Disconnect ends Start ───────────────────────────────────────────────

func TestDisconnectStopsStart(t *testing.T) {
	f := newFakeSlack(t)
	c := f.client(t, nil)

	connected := make(chan struct{})
	var once sync.Once
	c.On(EventConnected, func(Event) { once.Do(func() { close(connected) }) })

	done := make(chan error, 1)
	go func() { done <- c.Start(context.Background()) }()

	select {
	case <-connected:
	case <-time.After(3 * time.Second):
		t.Fatal("never connected")
	}

	if err := c.Disconnect(); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Start returned %v; a requested shutdown is not an error", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Start did not return after Disconnect")
	}
}

// ── a panicking handler does not take the client down ───────────────────

func TestHandlerPanicIsContained(t *testing.T) {
	f := newFakeSlack(t)
	f.onConnect = func(ws *websocket.Conn) {
		for i := 0; i < 2; i++ {
			_ = ws.WriteJSON(map[string]any{
				"type": "interactive", "envelope_id": "env-panic",
				"payload": map[string]any{},
			})
		}
	}

	c := f.client(t, nil)
	var mu sync.Mutex
	calls := 0
	c.On(EnvelopeInteractive, func(Event) {
		mu.Lock()
		calls++
		mu.Unlock()
		panic("handler exploded")
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Start(ctx)

	// The second delivery only arrives if the first panic was contained.
	waitFor(t, "the handler to be called twice", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return calls >= 2
	})
}

// ── Events channel ──────────────────────────────────────────────────────

func TestEventsChannel(t *testing.T) {
	f := newFakeSlack(t)
	f.onConnect = func(ws *websocket.Conn) {
		_ = ws.WriteJSON(map[string]any{
			"type": "events_api", "envelope_id": "env-chan",
			"payload": map[string]any{"event": map[string]any{"type": "app_mention"}},
		})
	}

	c := f.client(t, nil)
	events := c.Events()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Start(ctx)

	deadline := time.After(3 * time.Second)
	for {
		select {
		case e := <-events:
			if e.Type == EnvelopeEventsAPI {
				if e.InnerType != "app_mention" {
					t.Errorf("inner type was %q; want app_mention", e.InnerType)
				}
				return
			}
		case <-deadline:
			t.Fatal("the envelope never arrived on the channel")
		}
	}
}

// ── config ──────────────────────────────────────────────────────────────

func TestAppTokenIsRequired(t *testing.T) {
	if _, err := New(Options{}); err == nil {
		t.Error("New accepted an empty AppToken")
	}
}

// errorsAs is a tiny shim so the test file needs no extra import.
func errorsAs(err error, target **Error) bool {
	for err != nil {
		if e, ok := err.(*Error); ok {
			*target = e
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

// ── forced reconnect ────────────────────────────────────────────────────
//
// 🔑 The watchdogs only see the transport. A socket can stay healthy while the
//
//	events you expect never arrive, and only an end-to-end check notices that.
//	Reconnect is how a caller acts on such a check.

func TestReconnectOpensAFreshConnection(t *testing.T) {
	f := newFakeSlack(t)
	c := f.client(t, nil)

	connected := make(chan struct{}, 4)
	c.On(EventConnected, func(Event) {
		select {
		case connected <- struct{}{}:
		default:
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Start(ctx)

	select {
	case <-connected:
	case <-time.After(3 * time.Second):
		t.Fatal("never connected")
	}
	if !c.Connected() {
		t.Error("Connected reported false while connected")
	}

	if !c.Reconnect() {
		t.Fatal("Reconnect reported no connection to drop")
	}
	waitFor(t, "a second connection", func() bool { return f.connCount() >= 2 })
}

func TestReconnectWithNoConnection(t *testing.T) {
	f := newFakeSlack(t)
	c := f.client(t, nil)
	if c.Reconnect() {
		t.Error("Reconnect reported success with no connection open")
	}
	if c.Connected() {
		t.Error("Connected reported true before starting")
	}
}
