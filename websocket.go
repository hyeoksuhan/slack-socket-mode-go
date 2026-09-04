package socketmode

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// conn is one WebSocket connection with health monitoring attached.
//
// 🚨 Two independent watchdogs run here, and both are needed.
//
//	Slack pings the client on its own schedule; if those stop arriving the peer
//	is gone. That is the *server* watchdog, and on its own it takes up to
//	ServerPingTimeout (30s) to notice.
//
//	The client also pings and expects a pong. That is the *client* watchdog, and
//	it catches a half-open connection — one where packets leave and nothing
//	comes back — in about 5 seconds instead of 30.
//
//	Libraries that only wait for server pings look fine until a laptop sleeps or
//	a NAT entry expires, and then sit dead for half a minute per incident.
type conn struct {
	ws  *websocket.Conn
	log Logger
	opt Options

	// closeOnce guards teardown so both watchdogs can call it.
	closeOnce sync.Once
	closed    chan struct{}

	mu           sync.Mutex
	lastPong     time.Time
	pongEverSeen bool

	// writeMu serialises writes. gorilla permits one concurrent writer.
	writeMu sync.Mutex
}

// dial opens a connection to the URL Slack handed out.
func dial(ctx context.Context, url string, opt Options, log Logger) (*conn, error) {
	d := websocket.Dialer{
		HandshakeTimeout: 30 * time.Second,
		Proxy:            http.ProxyFromEnvironment,
	}
	if opt.Dialer != nil {
		d = *opt.Dialer
	}

	ws, resp, err := d.DialContext(ctx, url, nil)
	if err != nil {
		if resp != nil {
			return nil, newError(ErrWebsocket,
				fmt.Sprintf("dial failed with HTTP %d", resp.StatusCode), err)
		}
		return nil, newError(ErrWebsocket, "dial failed", err)
	}

	c := &conn{ws: ws, log: log, opt: opt, closed: make(chan struct{})}

	// Slack's ping resets the server watchdog. Answering with a pong is
	// gorilla's default, and that default must be preserved.
	c.ws.SetPingHandler(func(data string) error {
		if opt.PingPongLogging {
			c.log.Debugf("ping received from Slack (%q)", data)
		}
		c.armServerWatchdog()
		c.writeMu.Lock()
		defer c.writeMu.Unlock()
		err := c.ws.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(10*time.Second))
		if err == websocket.ErrCloseSent {
			return nil
		}
		return err
	})

	c.ws.SetPongHandler(func(data string) error {
		if opt.PingPongLogging {
			c.log.Debugf("pong received from Slack (%q)", data)
		}
		c.mu.Lock()
		c.lastPong = time.Now()
		c.pongEverSeen = true
		c.mu.Unlock()
		return nil
	})

	c.armServerWatchdog()
	go c.pingLoop()
	return c, nil
}

// armServerWatchdog restarts the deadline for hearing from Slack.
//
// gorilla enforces this through the read deadline: if no frame of any kind —
// ping, pong or data — arrives before it, the read fails and the connection
// tears down. That is exactly the behaviour wanted.
func (c *conn) armServerWatchdog() {
	_ = c.ws.SetReadDeadline(time.Now().Add(c.opt.ServerPingTimeout))
}

// pingLoop is the client-side watchdog.
//
// It mirrors the Node client: ping at a third of the pong timeout, give up
// after three unanswered pings, and once a pong has been seen at all, treat a
// gap longer than the timeout as fatal.
func (c *conn) pingLoop() {
	interval := c.opt.ClientPingTimeout / 3
	if interval <= 0 {
		interval = time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()

	unanswered := 0
	for {
		select {
		case <-c.closed:
			return
		case now := <-t.C:
			msg := fmt.Sprintf("ping from client (%d)", now.UnixMilli())

			c.writeMu.Lock()
			err := c.ws.WriteControl(websocket.PingMessage, []byte(msg), now.Add(5*time.Second))
			c.writeMu.Unlock()
			if err != nil {
				c.log.Errorf("could not ping Slack: %v", err)
				c.close()
				return
			}
			if c.opt.PingPongLogging {
				c.log.Debugf("ping sent to Slack: %s", msg)
			}

			c.mu.Lock()
			seen, last := c.pongEverSeen, c.lastPong
			c.mu.Unlock()

			dead := false
			if !seen {
				unanswered++
				dead = unanswered > 3
			} else {
				unanswered = 0
				dead = now.Sub(last) > c.opt.ClientPingTimeout
			}
			if dead {
				c.log.Warnf("no pong within %s, treating the connection as dead", c.opt.ClientPingTimeout)
				c.close()
				return
			}
		}
	}
}

// read returns the next text frame. It blocks until one arrives, the read
// deadline expires, or the connection closes.
func (c *conn) read() ([]byte, error) {
	_, data, err := c.ws.ReadMessage()
	if err != nil {
		return nil, err
	}
	// Any frame proves the peer is alive, so push the deadline out again.
	c.armServerWatchdog()
	return data, nil
}

// write sends one text frame.
func (c *conn) write(data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := c.ws.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return err
	}
	return c.ws.WriteMessage(websocket.TextMessage, data)
}

// close tears the connection down. Safe to call from any goroutine, any number
// of times.
func (c *conn) close() {
	c.closeOnce.Do(func() {
		close(c.closed)

		// Try the courteous close first, then drop the socket regardless.
		// A peer that has already gone will not complete the handshake, and
		// waiting on it is how a "closing" connection ends up stuck forever.
		c.writeMu.Lock()
		_ = c.ws.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
			time.Now().Add(time.Second))
		c.writeMu.Unlock()

		_ = c.ws.Close()
	})
}

// done reports when this connection has been torn down.
func (c *conn) done() <-chan struct{} { return c.closed }
