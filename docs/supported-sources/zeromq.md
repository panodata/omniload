# ZeroMQ

[ZeroMQ](https://zeromq.org/) is a high-performance asynchronous messaging library built around
brokerless socket patterns (PUSH/PULL, PUB/SUB, REQ/REP).

omniload consumes from ZeroMQ via [mq-bridge](mqbridge.md).

## URI format

```text
zeromq+mqb://localhost:5555?socket_type=pull
```

`--source-table` supplies the **topic** (the SUB filter, for `pub`/`sub` sockets). Common
options: `socket_type` (`pull`/`sub`/`rep`/…), `bind` (bind vs connect), `internal_buffer_size`.
The `tcp://` scheme is assumed; pass `?url=ipc://…` to override. See [mq-bridge](mqbridge.md) for
the full option reference and delivery semantics.

## Sample command

```sh
omniload ingest \
    --source-uri 'zeromq+mqb://localhost:5555?socket_type=pull' \
    --source-table 'events' \
    --dest-uri 'duckdb:///zeromq.duckdb' \
    --dest-table 'dest.events'
```
