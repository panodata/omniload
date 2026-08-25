# WebSocket

[WebSocket](https://developer.mozilla.org/en-US/docs/Web/API/WebSockets_API) provides
full-duplex message framing over a single long-lived TCP connection.

omniload consumes WebSocket frames via [mq-bridge](mqbridge.md).

## URI format

```text
websocket+mqb://0.0.0.0:8080
```

> **This source is a server, not a client.** mq-bridge *binds* the authority and ingests the
> frames that clients push to it. The authority is therefore a plain listen address with no
> `ws://` scheme, and omniload does not dial out to an existing WebSocket server.

`--source-table` supplies the HTTP **path** the server serves (e.g. `/ingest`). Common options:
`backlog`, `routed_queue_capacity`, `message_id_header`, `execution_mode`. See
[mq-bridge](mqbridge.md) for the full option reference and delivery semantics.

Because a listening server has no end-of-stream, the run is bounded by the transfer parameters
rather than by the source draining: `max_messages` caps how many frames are ingested and
`idle_timeout_ms` decides how long to wait for the next one before finishing. Set both
deliberately.

## Sample command

```sh
omniload ingest \
    --source-uri 'websocket+mqb://0.0.0.0:8080?max_messages=1000&idle_timeout_ms=30000' \
    --source-table '/ingest' \
    --dest-uri 'duckdb:///websocket.duckdb' \
    --dest-table 'dest.events'
```
