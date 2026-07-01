# IBM MQ

[IBM MQ](https://www.ibm.com/products/mq) is IBM's enterprise message-queuing middleware for
reliable point-to-point and publish/subscribe messaging.

omniload consumes from IBM MQ via [mq-bridge](mqbridge.md).

## Installation
Unlike the other mq-bridge transports, IBM MQ needs the proprietary **IBM MQ redistributable
client** present **at runtime** — it is not bundled and cannot be redistributed by omniload.
mq-bridge builds the IBM MQ support into the shipped wheel and loads the client library lazily
(via `dlopen`) on the first connection, so nothing extra is needed to install omniload itself,
but an `ibmmq+mqb://` ingest will fail fast with a non-retryable error if the client is absent.

1. Download and unpack the [IBM MQ redistributable client](https://www.ibm.com/docs/en/ibm-mq/latest?topic=roadmap-mq-downloads)
   for your platform (Linux/Windows; no full server install required).
2. Point the loader at it so it can find `libmqm` at runtime, via **either**:
   - `MQ_INSTALLATION_PATH` — the client's install root (e.g. `/opt/mqm`); or
   - `MQB_IBM_MQ_LIB` — the explicit path to the client library.

```sh
export MQ_INSTALLATION_PATH=/opt/mqm   # e.g. after unpacking the redistributable client
```

See mq-bridge's [configuration guide](https://github.com/marcomq/mq-bridge/blob/main/CONFIGURATION.md)
for the underlying `mqi` client-loading details.

## URI format

```text
ibmmq+mqb://host:1414?queue_manager=QM1&channel=DEV.APP.SVRCONN
```

IBM MQ addresses queue managers as `host(port)`, but you write the familiar `host:port`
authority and omniload translates it (comma-separated for failover, e.g.
`ibmmq+mqb://h1:1414,h2:1414`). `queue_manager` and `channel` are **required** query parameters.
`--source-table` names the target **queue** for point-to-point consumption; pass `?topic=` to
consume in publish/subscribe subscriber mode instead.

Common options: `username` / `password` (channel authentication), `cipher_spec` and `tls.*`
(encrypted connections), `wait_timeout_ms` (consumer poll timeout), `max_message_size`,
`disable_status_inq`. See [mq-bridge](mqbridge.md) for the full option reference and delivery
semantics.

## Sample command

```sh
omniload ingest \
    --source-uri 'ibmmq+mqb://localhost:1414?queue_manager=QM1&channel=DEV.APP.SVRCONN' \
    --source-table 'DEV.QUEUE.1' \
    --dest-uri 'duckdb:///ibmmq.duckdb' \
    --dest-table 'dest.orders'
```
