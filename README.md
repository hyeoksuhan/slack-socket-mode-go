# slack-socket-mode-go

A Go port of [`@slack/socket-mode`](https://www.npmjs.com/package/@slack/socket-mode).

Same job, same scope: open the socket, keep it open, acknowledge what arrives,
hand you the events. It does not store, filter or interpret anything — those
stay your program's business, exactly as in the Node package.

```bash
go get github.com/hyeoksuhan/slack-socket-mode-go
```

Requires Go 1.24 or newer. One dependency: `gorilla/websocket`.

---

## Coming from `@slack/socket-mode`

The names carry over. If you know the Node package, you already know this one.

**Node**

```js
const { SocketModeClient } = require('@slack/socket-mode');

const client = new SocketModeClient({
  appToken: process.env.SLACK_APP_TOKEN,
});

client.on('connected', () => console.log('connected'));

client.on('message', async ({ ack, event }) => {
  await ack();
  console.log(event.user, event.text);
});

await client.start();
```

**Go**

```go
client, err := socketmode.New(socketmode.Options{
    AppToken: os.Getenv("SLACK_APP_TOKEN"),
})
if err != nil {
    log.Fatal(err)
}

client.On(socketmode.EventConnected, func(socketmode.Event) {
    log.Println("connected")
})

client.On("message", func(e socketmode.Event) {
    e.Ack()

    var msg struct{ User, Text string }
    json.Unmarshal(e.Inner, &msg)
    log.Println(msg.User, msg.Text)
})

client.Start(ctx) // blocks
```

### Mapping

| Node | Go |
| --- | --- |
| `new SocketModeClient({appToken})` | `socketmode.New(socketmode.Options{AppToken: …})` |
| `client.on('connected', fn)` | `client.On(socketmode.EventConnected, fn)` |
| `client.on('connecting' \| 'reconnecting' \| 'disconnected', fn)` | `EventConnecting` · `EventReconnecting` · `EventDisconnected` |
| `client.on('authenticated', fn)` | `EventAuthenticated` — `e.Body` holds the raw `apps.connections.open` response |
| `client.on('slack_event', fn)` | `client.On(socketmode.EventSlack, fn)` |
| `client.on('interactive', fn)` | `client.On(socketmode.EnvelopeInteractive, fn)` |
| `client.on('message', fn)` | `client.On("message", fn)` — inner Slack event types work the same |
| `await ack()` | `e.Ack()` |
| `await ack(payload)` | `e.Ack(payload)` |
| `await client.start()` | `client.Start(ctx)` |
| `await client.disconnect()` | `client.Disconnect()` |
| `{ body }` | `e.Body` — raw JSON |
| `{ event }` | `e.Inner` — raw JSON, `events_api` only |
| `{ retry_num, retry_reason }` | `e.RetryNum`, `e.RetryReason` |
| `{ accepts_response_payload }` | `e.AcceptsResponsePayload` |
| `logLevel: LogLevel.DEBUG` | `LogLevel: socketmode.LogDebug` |
| `clientPingTimeout` · `serverPingTimeout` | `ClientPingTimeout` · `ServerPingTimeout` |
| `autoReconnectEnabled: false` | `AutoReconnect: &falseValue` |
| *(no equivalent)* | `MaxReconnectDelay` — see below |
| *(no equivalent)* | `client.Events()` — receive on a channel instead |

**One envelope arrives up to three times**, exactly as in Node: under its inner
Slack event type, under its envelope type, and under `slack_event`. Register at
whichever level suits you and ignore the rest.

```go
client.On("message", h)                        // only messages
client.On(socketmode.EnvelopeEventsAPI, h)     // every events_api envelope
client.On(socketmode.EventSlack, h)            // everything, including lifecycle
```

### Payloads stay as raw JSON

`e.Body` and `e.Inner` are `json.RawMessage`, not structs. Slack sends
differently shaped payloads under the same type, and decoding into a fixed
struct silently drops every field the struct does not declare. Unmarshal into
whatever shape you actually need:

```go
var msg struct {
    Channel  string `json:"channel"`
    User     string `json:"user"`
    Text     string `json:"text"`
    ThreadTS string `json:"thread_ts"`
}
json.Unmarshal(e.Inner, &msg)
```

---

## What this adds

### Reconnect delay is capped

The Node client waits `clientPingTimeout × consecutiveFailures` with no
ceiling. Twelve failed attempts is a one-minute sleep; thirty is two and a half
minutes. During a long outage the delay keeps climbing, so the client stays
asleep well after the network comes back.

`MaxReconnectDelay` (default 30s) puts a floor under recovery time. Set it high
if you want the Node behaviour.

### Everything takes a context

`Start(ctx)` returns as soon as the context ends, including from the middle of a
reconnect wait. Shutdown does not have to wait out whatever timer is running.

### A panicking handler does not take the client down

A handler that panics on an unexpected payload would otherwise kill the process,
the supervisor would restart it, and the same event would arrive again. That
loop looks like a silent outage from outside. Panics are caught, logged, and the
client keeps reading.

### A channel, if you prefer one

```go
events := client.Events()
go client.Start(ctx)

for e := range events {
    switch e.Type {
    case socketmode.EnvelopeEventsAPI:
        e.Ack()
        log.Println(e.InnerType)
    }
}
```

Each envelope reaches the channel once, not three times.

---

## Connection health

Two independent watchdogs run, matching the Node client:

| Watchdog | Default | Catches |
| --- | --- | --- |
| Slack's ping stops arriving | 30s | The peer is gone |
| Our ping goes unanswered | 5s | A half-open connection — packets leave, nothing returns |

The second one matters more than it looks. A laptop that sleeps, or a NAT entry
that expires, leaves a socket that looks connected and delivers nothing.
Waiting only for the server's ping means half a minute of silence per incident.

Slack also recycles connections every few hours and sends a `disconnect`
envelope before it does. That is routine, not a failure: the client closes and
reconnects without reporting an error.

---

## Errors

`Start` returns only for conditions that retrying cannot fix — a revoked token,
a deleted app, a missing scope. Transport failures are handled internally.

```go
if err := client.Start(ctx); err != nil {
    var se *socketmode.Error
    if errors.As(err, &se) && se.Code == socketmode.ErrPlatform {
        // token or app problem; a human has to fix it
    }
}
```

`socketmode.IsUnrecoverable(code)` classifies a Slack error string the same way
the client does internally.

---

## Setting up the app

1. **api.slack.com/apps** → your app → **Socket Mode** → enable it.
2. **Basic Information** → **App-Level Tokens** → generate one with
   `connections:write`. That is the `xapp-` token this package needs.
3. **Event Subscriptions** → subscribe to the events you want.

The app-level token is the only credential this package uses. Calling the Web
API — posting messages, adding reactions — needs a bot token and a separate
client; that is outside this package's scope, as it is in Node.

---

## Testing

```bash
go test ./...
```

The suite runs against a real WebSocket server rather than a mock, so the
handshake, ping/pong, acknowledgement and reconnect paths are all exercised for
real. Slack asking for a reconnect, an unrecoverable auth failure, a panicking
handler and the delay cap each have a test.

---

## Scope

This package is the transport. For the layer above it — routing, storage,
deciding what a message means — Node users reach for Bolt; there is no
equivalent here, and `slack-go/slack` is the usual choice for Web API calls.

## License

MIT. See [LICENSE](LICENSE).
