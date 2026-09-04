package socketmode_test

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	socketmode "github.com/hyeoksuhan/slack-socket-mode-go"
)

// The shortest useful client: connect, acknowledge, react.
func Example() {
	client, err := socketmode.New(socketmode.Options{
		AppToken: os.Getenv("SLACK_APP_TOKEN"), // xapp-…, scope connections:write
	})
	if err != nil {
		log.Fatal(err)
	}

	client.On(socketmode.EventConnected, func(socketmode.Event) {
		log.Println("connected")
	})

	// Only messages. The envelope is also delivered under "events_api" and
	// under EventSlack, so pick the level you want and ignore the others.
	client.On("message", func(e socketmode.Event) {
		// Acknowledge first: Slack redelivers up to three times otherwise.
		if err := e.Ack(); err != nil {
			log.Println("ack:", err)
		}

		var msg struct {
			Channel string `json:"channel"`
			User    string `json:"user"`
			Text    string `json:"text"`
			TS      string `json:"ts"`
		}
		if err := json.Unmarshal(e.Inner, &msg); err != nil {
			log.Println("decode:", err)
			return
		}
		log.Printf("%s in %s: %s", msg.User, msg.Channel, msg.Text)
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Blocks. Reconnects on its own; returns only on shutdown or a token problem.
	if err := client.Start(ctx); err != nil {
		log.Fatal(err)
	}
}

// Receiving on a channel instead of registering handlers.
func Example_channel() {
	client, _ := socketmode.New(socketmode.Options{
		AppToken: os.Getenv("SLACK_APP_TOKEN"),
	})

	events := client.Events()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go client.Start(ctx)

	for e := range events {
		switch e.Type {
		case socketmode.EventConnected:
			log.Println("connected")

		case socketmode.EnvelopeEventsAPI:
			e.Ack()
			log.Println("event:", e.InnerType)

		case socketmode.EnvelopeInteractive:
			e.Ack()
			log.Println("button clicked")
		}
	}
}

// Handling a redelivery. A non-zero RetryNum means the previous acknowledgement
// did not reach Slack in time, and the work may already be done.
func Example_retries() {
	client, _ := socketmode.New(socketmode.Options{
		AppToken: os.Getenv("SLACK_APP_TOKEN"),
	})

	client.On(socketmode.EventSlack, func(e socketmode.Event) {
		e.Ack()

		if e.RetryNum > 0 {
			log.Printf("redelivery %d (%s) of %s — checking before repeating",
				e.RetryNum, e.RetryReason, e.EnvelopeID)
			return
		}
		// first delivery: do the work
	})

	client.Start(context.Background())
}

// Tuning the connection. The defaults match the Node client except for the
// reconnect cap, which Node does not have.
func Example_options() {
	client, _ := socketmode.New(socketmode.Options{
		AppToken: os.Getenv("SLACK_APP_TOKEN"),

		// How long to wait for a pong before giving up on the connection.
		ClientPingTimeout: 5 * time.Second,

		// How long to wait for Slack's own ping.
		ServerPingTimeout: 30 * time.Second,

		// Delay grows by this much per consecutive failure…
		ReconnectStep: 5 * time.Second,

		// …but never past this. Node has no equivalent: its delay grows without
		// limit, so a long outage leaves it asleep well after the network is back.
		MaxReconnectDelay: 30 * time.Second,

		LogLevel: socketmode.LogDebug,
	})

	_ = client
}
