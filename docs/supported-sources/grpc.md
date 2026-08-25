# gRPC

[gRPC](https://grpc.io/) is a high-performance RPC framework built on HTTP/2 and protocol
buffers, with first-class support for server-streaming responses.

omniload consumes from gRPC endpoints via [mq-bridge](mqbridge.md), in one of two modes.

## URI format

```text
grpc+mqb://localhost:50051
```

The authority becomes `http://localhost:50051` and omniload connects as a **client**. For TLS,
pass the URL explicitly (`?url=https://grpc.example.com:443`) or set `tls.required=true` and the
other [`tls.*` keys](mqbridge.md#authentication--tls). To listen instead of dial, set
`?server_mode=true`.

### Bridge mode (default)

With no descriptor, the endpoint speaks mq-bridge's own generated `mqbridge.Bridge` protocol.
This is the mode to use when the peer is **another mq-bridge instance**; it is not a generic
gRPC client, so it will not talk to an arbitrary service. `--source-table` supplies the
**topic**. Options: `consumer_id`, `timeout_ms`, `shared` (multiplex one HTTP/2 channel),
and the HTTP/2 tuning fields (`http2_keepalive_interval_ms`, `max_decoding_message_size`, …).

Bridge mode acknowledges messages, so it gets the same commit-after-load delivery guarantee as
every other mq-bridge transport.

### Dynamic mode (arbitrary service)

To consume from **your own** gRPC service, supply a compiled protobuf descriptor set together
with the service, method, and the JSON request to send:

```text
grpc+mqb://grpc.example.com:443?url=https://grpc.example.com:443
  &descriptor_set_path=proto/events.bin
  &service_name=events.EventService
  &method_name=Tail
  &server_streaming=true
  &request.topic=audit
```

Generate the descriptor with `protoc`, with imports included:

```sh
protoc --descriptor_set_out=proto/events.bin --include_imports -I proto proto/events.proto
```

`--include_imports` is required: the descriptor must be self-contained, because mq-bridge
resolves the message types from that one file and never sees your `.proto` sources. The path is
read by the process running omniload, not by the server, so it must be reachable locally.

`request` is a JSON object, expressed with [dotted query keys](mqbridge.md#authentication--tls)
(`?request.topic=audit` becomes `{"request": {"topic": "audit"}}`). Responses are decoded using
protobuf's canonical JSON representation. Unary and server-streaming methods are supported;
client-streaming methods are rejected.

> **Dynamic mode has no acknowledgement.** A descriptor describes the wire format but not
> broker ack semantics, so a dynamic source has no ACK operation and therefore **cannot offer
> the commit-after-load guarantee** documented for every other mq-bridge transport. Use Bridge
> mode where at-least-once delivery matters, and treat a dynamic gRPC ingest as best-effort.

See [mq-bridge](mqbridge.md) for the full option reference and delivery semantics.

> gRPC is absent from the reduced `mq-bridge-py` wheel used on musl/Alpine and Windows arm64.

## Sample command

```sh
omniload ingest \
    --source-uri 'grpc+mqb://localhost:50051' \
    --source-table 'orders' \
    --dest-uri 'duckdb:///grpc.duckdb' \
    --dest-table 'dest.orders'
```
