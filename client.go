// Package socketmode connects to Slack over Socket Mode.
//
// It is a Go port of the Node package `@slack/socket-mode`, with the same
// scope: open the socket, keep it open, acknowledge what arrives, and hand the
// events to you. It does not store anything, filter anything, or decide what a
// message means. Those are your program's job.
//
// The shape will be familiar if you are coming from Node:
//
//	client, _ := socketmode.New(socketmode.Options{AppToken: os.Getenv("SLACK_APP_TOKEN")})
//
//	client.On(socketmode.EventConnected, func(socketmode.Event) {
//	    log.Println("connected")
//	})
//	client.On(socketmode.EventSlack, func(e socketmode.Event) {
//	    e.Ack()
//	    log.Println(e.Type, e.InnerType)
//	})
//
//	client.Start(ctx) // blocks
//
// If you would rather receive on a channel, [Client.Events] gives you one.
//
// # What this adds over the Node client
//
// Reconnect delay is capped. The Node client waits five seconds times the
// number of consecutive failures with no ceiling, so a long outage leaves it
// sleeping for minutes after the network returns.
//
// Everything takes a context, so a shutdown is immediate rather than waiting
// out whatever timer happens to be running.
package socketmode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Default timings, matching the Node client except where noted.
const (
	DefaultClientPingTimeout = 5 * time.Second
	DefaultServerPingTimeout = 30 * time.Second

	// DefaultReconnectStep is the delay added per consecutive failure.
	DefaultReconnectStep = 5 * time.Second

	// DefaultMaxReconnectDelay caps the wait between attempts.
	//
	// 🔑 The Node client has no equivalent: its delay is step × failures and
	//	grows without limit. Twelve failed attempts there means a one-minute
	//	sleep, thirty means two and a half. Recovery arrives long after the
	//	network does.
	DefaultMaxReconnectDelay = 30 * time.Second
)

const connectionsOpenURL = "https://slack.com/api/apps.connections.open"

// Options configures a Client. Only AppToken is required.
type Options struct {
	// AppToken is the app-level token, starting with xapp-, carrying the
	// connections:write scope.
	AppToken string

	// AutoReconnect defaults to true. Disconnects are routine in Socket Mode —
	// Slack recycles connections every few hours — so turning this off leaves
	// you disconnected before long.
	AutoReconnect *bool

	// ClientPingTimeout is how long to wait for a pong before declaring the
	// connection dead. Defaults to 5s. The client pings at a third of this.
	ClientPingTimeout time.Duration

	// ServerPingTimeout is how long to wait for Slack's ping. Defaults to 30s.
	ServerPingTimeout time.Duration

	// ReconnectStep is added to the delay per consecutive failure. Defaults to 5s.
	ReconnectStep time.Duration

	// MaxReconnectDelay caps that delay. Defaults to 30s. Set a large value to
	// match the Node client's unbounded growth.
	MaxReconnectDelay time.Duration

	// PingPongLogging logs every ping and pong at debug level. Noisy.
	PingPongLogging bool

	// Logger receives client output. Defaults to stderr at info level.
	Logger Logger

	// LogLevel applies to the default logger only.
	LogLevel LogLevel

	// HTTPClient calls apps.connections.open. Defaults to a 30s client.
	HTTPClient *http.Client

	// Dialer opens the WebSocket. Set it for proxies or custom TLS.
	Dialer *websocket.Dialer

	// EventBufferSize sizes the channel returned by Events. Defaults to 64.
	EventBufferSize int
}

func (o *Options) applyDefaults() {
	if o.ClientPingTimeout <= 0 {
		o.ClientPingTimeout = DefaultClientPingTimeout
	}
	if o.ServerPingTimeout <= 0 {
		o.ServerPingTimeout = DefaultServerPingTimeout
	}
	if o.ReconnectStep <= 0 {
		o.ReconnectStep = DefaultReconnectStep
	}
	if o.MaxReconnectDelay <= 0 {
		o.MaxReconnectDelay = DefaultMaxReconnectDelay
	}
	if o.HTTPClient == nil {
		o.HTTPClient = &http.Client{Timeout: 30 * time.Second}
	}
	if o.EventBufferSize <= 0 {
		o.EventBufferSize = 64
	}
	if o.AutoReconnect == nil {
		t := true
		o.AutoReconnect = &t
	}
	if o.LogLevel == "" {
		o.LogLevel = LogInfo
	}
	if o.Logger == nil {
		o.Logger = NewLogger(os.Stderr, o.LogLevel)
	}
}

// Client is a Socket Mode connection. Safe for concurrent use.
type Client struct {
	opt Options
	log Logger

	mu       sync.RWMutex
	handlers map[string][]Handler
	events   chan Event
	current  *conn

	// stop is closed by Disconnect to end Start without an error.
	stopOnce sync.Once
	stop     chan struct{}

	// ConnectionInfo from the last successful apps.connections.open.
	connInfo json.RawMessage
}

// New builds a client. It does not connect; call Start for that.
func New(opt Options) (*Client, error) {
	if opt.AppToken == "" {
		return nil, errors.New("socketmode: AppToken is required (the xapp- token with connections:write)")
	}
	opt.applyDefaults()
	return &Client{
		opt:      opt,
		log:      opt.Logger,
		handlers: map[string][]Handler{},
		stop:     make(chan struct{}),
	}, nil
}

// On registers a handler.
//
// The name is either a lifecycle event (EventConnected and friends), an
// envelope type (EnvelopeInteractive), an inner Slack event type ("message",
// "app_mention"), or EventSlack to receive everything.
//
// 🚨 Handlers run on the read loop, one at a time, in arrival order.
//
//	Blocking in one stops the client from reading the next frame, and a read
//	loop that stalls loses the connection to the server watchdog. Acknowledge
//	first, then hand slow work to a goroutine of your own.
func (c *Client) On(event string, h Handler) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.handlers[event] = append(c.handlers[event], h)
}

// Events returns a channel carrying every event, as an alternative to On.
//
// The channel is created on first call and buffered by Options.EventBufferSize.
// The same blocking rule applies: a full buffer stops the read loop, so drain
// it promptly.
func (c *Client) Events() <-chan Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.events == nil {
		c.events = make(chan Event, c.opt.EventBufferSize)
	}
	return c.events
}

// ConnectionInfo returns the raw apps.connections.open response from the most
// recent successful connection, or nil.
func (c *Client) ConnectionInfo() json.RawMessage {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connInfo
}

// Start connects and blocks until the context ends, Disconnect is called, or an
// unrecoverable error occurs.
//
// With AutoReconnect on, transport failures are handled internally and never
// returned. What does come back is a token problem, a revoked app, or a similar
// condition that retrying cannot fix.
func (c *Client) Start(ctx context.Context) error {
	failures := 0

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-c.stop:
			return nil
		default:
		}

		c.emit(Event{Type: EventConnecting})

		url, raw, err := c.openConnection(ctx)
		if err != nil {
			var se *Error
			if errors.As(err, &se) && se.Code == ErrPlatform {
				c.log.Errorf("cannot open a connection: %v", err)
				return err
			}
			failures++
			if !c.waitBeforeRetry(ctx, failures) {
				return nil
			}
			continue
		}

		c.mu.Lock()
		c.connInfo = raw
		c.mu.Unlock()
		c.emit(Event{Type: EventAuthenticated, Body: raw})

		conn, err := dial(ctx, url, c.opt, c.log)
		if err != nil {
			c.log.Warnf("could not connect: %v", err)
			c.emit(Event{Type: EventError, Err: err})
			failures++
			if !c.waitBeforeRetry(ctx, failures) {
				return nil
			}
			continue
		}

		failures = 0
		c.mu.Lock()
		c.current = conn
		c.mu.Unlock()
		c.emit(Event{Type: EventConnected})

		readErr := c.readLoop(ctx, conn)

		c.mu.Lock()
		c.current = nil
		c.mu.Unlock()
		conn.close()
		c.emit(Event{Type: EventDisconnected})

		select {
		case <-ctx.Done():
			return nil
		case <-c.stop:
			return nil
		default:
		}

		if !*c.opt.AutoReconnect {
			return readErr
		}
		if readErr != nil {
			c.log.Warnf("connection lost: %v", readErr)
		}
		failures++
		c.emit(Event{Type: EventReconnecting})
		if !c.waitBeforeRetry(ctx, failures) {
			return nil
		}
	}
}

// Disconnect ends Start and closes the current connection.
func (c *Client) Disconnect() error {
	c.emit(Event{Type: EventDisconnecting})
	c.stopOnce.Do(func() { close(c.stop) })

	c.mu.RLock()
	conn := c.current
	c.mu.RUnlock()
	if conn != nil {
		conn.close()
	}
	return nil
}

// Reconnect drops the current connection so the reconnect loop opens a fresh
// one. Start keeps running.
//
// 🔑 Use it when something outside this package decides the connection is no
//
//	longer useful. The built-in watchdogs only see the transport: they catch a
//	socket that stopped carrying frames, but not one that carries frames while
//	the events you expect never arrive. An end-to-end check — post a marker,
//	wait for it to come back over the socket — catches that, and this is how it
//	acts on the answer.
//
// Returns false if there was no connection to drop.
func (c *Client) Reconnect() bool {
	c.mu.RLock()
	conn := c.current
	c.mu.RUnlock()
	if conn == nil {
		return false
	}
	c.log.Infof("reconnecting on request")
	conn.close()
	return true
}

// Connected reports whether a connection is currently open.
func (c *Client) Connected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.current != nil
}

// Ack acknowledges an envelope by id. Event.Ack is usually more convenient.
func (c *Client) Ack(envelopeID string, payload any) error {
	c.mu.RLock()
	conn := c.current
	c.mu.RUnlock()
	if conn == nil {
		return newError(ErrSendWhileDisconnected, "no connection", nil)
	}

	msg, err := json.Marshal(ackMessage{EnvelopeID: envelopeID, Payload: payload})
	if err != nil {
		return fmt.Errorf("socketmode: encode acknowledgement: %w", err)
	}
	if err := conn.write(msg); err != nil {
		return newError(ErrWebsocket, "send acknowledgement", err)
	}
	return nil
}

// waitBeforeRetry sleeps before the next attempt. It reports false if the wait
// was interrupted by shutdown.
func (c *Client) waitBeforeRetry(ctx context.Context, failures int) bool {
	d := time.Duration(failures) * c.opt.ReconnectStep
	if d > c.opt.MaxReconnectDelay {
		d = c.opt.MaxReconnectDelay
	}
	c.log.Debugf("waiting %s before attempt %d", d, failures+1)

	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	case <-c.stop:
		return false
	}
}

// readLoop reads frames until the connection fails or shutdown is requested.
func (c *Client) readLoop(ctx context.Context, conn *conn) error {
	// Close the socket when the caller's context ends, which unblocks the read.
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			conn.close()
		case <-c.stop:
			conn.close()
		case <-conn.done():
		case <-done:
		}
	}()

	for {
		data, err := conn.read()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			case <-c.stop:
				return nil
			default:
			}
			return newError(ErrWebsocket, "read failed", err)
		}

		var env envelope
		if err := json.Unmarshal(data, &env); err != nil {
			c.log.Warnf("could not decode a frame, ignoring it: %v", err)
			continue
		}
		c.dispatch(env, conn)
	}
}

// dispatch turns one envelope into handler calls.
//
// An envelope is delivered up to three times, matching the Node client:
// under its inner Slack event type when it is an events_api envelope, under its
// own envelope type, and always under EventSlack.
func (c *Client) dispatch(env envelope, conn *conn) {
	switch env.Type {
	case EnvelopeHello:
		c.log.Debugf("hello from Slack (%d connections)", env.NumConnections)
		return

	case EnvelopeDisconnect:
		// 🚨 Slack recycles connections every few hours. This is routine, not a
		//	failure: close the socket and let the reconnect loop take over.
		c.log.Debugf("Slack asked us to reconnect (%s)", env.Reason)
		conn.close()
		return
	}

	ack := func(payload any) error {
		return c.Ack(env.EnvelopeID, payload)
	}

	e := Event{
		Type:                   env.Type,
		InnerType:              env.innerType(),
		EnvelopeID:             env.EnvelopeID,
		Body:                   env.Payload,
		Inner:                  env.innerEvent(),
		RetryNum:               env.RetryAttempt,
		RetryReason:            env.RetryReason,
		AcceptsResponsePayload: env.AcceptsResponsePayload,
		ack:                    ack,
	}

	if e.InnerType != "" {
		c.emitTo(e.InnerType, e)
	}
	c.emitTo(env.Type, e)
	c.emitTo(EventSlack, e)
}

// emit delivers a lifecycle event.
func (c *Client) emit(e Event) {
	c.emitTo(e.Type, e)
	if e.Type != EventSlack {
		// Lifecycle events also reach anyone listening to everything.
		c.emitTo(EventSlack, e)
	}
}

// emitTo runs the handlers registered for one name, then offers the event to
// the channel if anyone asked for one.
func (c *Client) emitTo(name string, e Event) {
	c.mu.RLock()
	hs := c.handlers[name]
	ch := c.events
	c.mu.RUnlock()

	for _, h := range hs {
		c.safely(name, h, e)
	}

	// The channel receives each event once, under EventSlack, so a consumer
	// reading it does not see the same envelope three times.
	if ch != nil && name == EventSlack {
		select {
		case ch <- e:
		case <-c.stop:
		}
	}
}

// safely runs a handler, surviving a panic inside it.
//
// 🚨 One bad event must not take the client down.
//
//	A handler that panics on an unexpected payload would otherwise kill the
//	process, the supervisor would restart it, and the same event would arrive
//	again — a loop that looks like a silent outage from outside.
func (c *Client) safely(name string, h Handler, e Event) {
	defer func() {
		if r := recover(); r != nil {
			c.log.Errorf("handler for %q panicked, continuing: %v", name, r)
		}
	}()
	h(e)
}

// openConnection asks Slack for a WebSocket URL.
func (c *Client) openConnection(ctx context.Context) (string, json.RawMessage, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, connectionsOpenURL, nil)
	if err != nil {
		return "", nil, fmt.Errorf("socketmode: build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.opt.AppToken)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.opt.HTTPClient.Do(req)
	if err != nil {
		return "", nil, newError(ErrWebsocket, "apps.connections.open failed", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", nil, newError(ErrWebsocket, "read apps.connections.open response", err)
	}

	var body struct {
		OK    bool   `json:"ok"`
		URL   string `json:"url"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return "", nil, newError(ErrWebsocket,
			fmt.Sprintf("apps.connections.open returned non-JSON (HTTP %d)", resp.StatusCode), err)
	}

	if !body.OK {
		msg := fmt.Sprintf("apps.connections.open: %s", body.Error)
		if IsUnrecoverable(body.Error) {
			// Retrying will never work. Surface it and stop.
			return "", nil, newError(ErrPlatform, msg, nil)
		}
		return "", nil, newError(ErrWebsocket, msg, nil)
	}
	if body.URL == "" {
		return "", nil, newError(ErrWebsocket, "apps.connections.open returned no URL", nil)
	}
	return body.URL, raw, nil
}
