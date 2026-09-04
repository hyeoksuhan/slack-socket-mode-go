package socketmode

import "encoding/json"

// Connection-state event names. Register with On to observe the lifecycle.
//
// These match the strings the Node client emits, so a handler map ported over
// keeps working unchanged.
const (
	EventConnecting    = "connecting"
	EventAuthenticated = "authenticated"
	EventConnected     = "connected"
	EventReconnecting  = "reconnecting"
	EventDisconnecting = "disconnecting"
	EventDisconnected  = "disconnected"

	// EventSlack fires for every envelope Slack sends, whatever its type.
	// This is the one to use when you want a single entry point.
	EventSlack = "slack_event"

	// EventError fires on transport failures the client recovered from.
	EventError = "error"
)

// Envelope types Slack puts on the wire. An envelope is also emitted under its
// own type, so On("interactive", ...) receives only button clicks.
const (
	EnvelopeEventsAPI     = "events_api"
	EnvelopeInteractive   = "interactive"
	EnvelopeSlashCommands = "slash_commands"
	EnvelopeHello         = "hello"
	EnvelopeDisconnect    = "disconnect"
)

// Event is one delivery to a handler.
//
// 🔑 Body and Inner stay as raw JSON on purpose.
//
//	Slack sends differently shaped payloads under the same type, and decoding
//	into a fixed struct silently drops every field the struct does not declare.
//	Unmarshal them yourself into whatever shape you actually need.
type Event struct {
	// Type is the envelope type for payload events (events_api, interactive,
	// slash_commands), or the state name for lifecycle events (connected, …).
	Type string

	// InnerType is the Slack event type inside an events_api envelope —
	// message, app_mention, reaction_added. Empty for other envelopes.
	InnerType string

	// EnvelopeID identifies this delivery. Ack uses it.
	EnvelopeID string

	// Body is the whole payload object.
	Body json.RawMessage

	// Inner is payload.event, present only for events_api.
	Inner json.RawMessage

	// RetryNum is how many times Slack has redelivered this envelope, and
	// RetryReason says why.
	//
	// 🚨 A non-zero RetryNum means your previous acknowledgement did not arrive
	//	in time. The work may already have been done. Check before repeating a
	//	side effect.
	RetryNum    int
	RetryReason string

	// AcceptsResponsePayload is true when Slack will render whatever you pass
	// to Ack — a modal, a menu.
	AcceptsResponsePayload bool

	// Err carries the failure for EventError deliveries.
	Err error

	// ack sends the acknowledgement. Nil for lifecycle events.
	ack func(payload any) error
}

// Ack acknowledges the envelope.
//
// 🚨 Call this **first**, before doing the work.
//
//	Slack redelivers an unacknowledged envelope up to three times over a few
//	minutes, and a receive loop that stalls also loses the connection. Nothing
//	you do afterwards benefits from having waited.
//
// Pass a payload only when AcceptsResponsePayload is set.
func (e Event) Ack(payload ...any) error {
	if e.ack == nil {
		return nil // lifecycle events have nothing to acknowledge
	}
	if len(payload) == 0 {
		return e.ack(nil)
	}
	return e.ack(payload[0])
}

// Handler receives one event.
type Handler func(Event)

// envelope is the wire format Slack sends.
type envelope struct {
	Type                   string          `json:"type"`
	EnvelopeID             string          `json:"envelope_id"`
	Payload                json.RawMessage `json:"payload"`
	AcceptsResponsePayload bool            `json:"accepts_response_payload"`
	RetryAttempt           int             `json:"retry_attempt"`
	RetryReason            string          `json:"retry_reason"`

	// hello and disconnect only
	NumConnections int    `json:"num_connections"`
	Reason         string `json:"reason"`
	DebugInfo      struct {
		Host                      string `json:"host"`
		BuildNumber               int    `json:"build_number"`
		ApproximateConnectionTime int    `json:"approximate_connection_time"`
	} `json:"debug_info"`
}

// innerType digs payload.event.type out of an events_api envelope.
func (e envelope) innerType() string {
	if e.Type != EnvelopeEventsAPI || len(e.Payload) == 0 {
		return ""
	}
	var p struct {
		Event struct {
			Type string `json:"type"`
		} `json:"event"`
	}
	if err := json.Unmarshal(e.Payload, &p); err != nil {
		return ""
	}
	return p.Event.Type
}

// innerEvent returns payload.event verbatim.
func (e envelope) innerEvent() json.RawMessage {
	if e.Type != EnvelopeEventsAPI || len(e.Payload) == 0 {
		return nil
	}
	var p struct {
		Event json.RawMessage `json:"event"`
	}
	if err := json.Unmarshal(e.Payload, &p); err != nil {
		return nil
	}
	return p.Event
}

// ackMessage is what goes back over the socket.
type ackMessage struct {
	EnvelopeID string `json:"envelope_id"`
	Payload    any    `json:"payload,omitempty"`
}
