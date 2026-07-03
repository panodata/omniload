# mq-bridge

[mq-bridge](https://github.com/marcomq/mq-bridge) is a generic, transport-agnostic
message broker binding. omniload uses it to consume messages from
[AMQP](amqp.md), [AWS SQS](sqs.md), [IBM MQ](ibm-mq.md), [MQTT](mqtt.md),
[NATS](nats.md), [ZeroMQ](zeromq.md), or an in-memory transport, and load
them into any omniload destination.

omniload supports mq-bridge as a source.

> Kafka is also available as a **native** source (`kafka://`, see [Apache Kafka](kafka.md)) with
> richer event decoding. Use this mq-bridge variant (`kafka+mqb://`) for durable
> commit-after-load delivery and a single engine shared across all brokers.

## Installation
mq-bridge ships as a native wheel and is bundled with omniload, so no extra install step is
needed:

```sh
pip install omniload
```

> **IBM MQ** is the one exception: it additionally requires the IBM MQ redistributable client to
> be present at runtime. See [IBM MQ → Installation](ibm-mq.md#installation).

## URI format
Each broker is addressed via a compound `<transport>+mqb://` scheme. The broker URL and the
`--source-table` (topic/subject/queue) make up the connection; everything else is passed as
query parameters.

```text
<transport>+mqb://<broker>?<param>=<value>&...
```

| Transport | URI example | Topic-like field (`--source-table`) |
|-----------|-------------|-------------------------------------|
| Kafka     | `kafka+mqb://localhost:9092?group_id=g`   | `topic` |
| NATS      | `nats+mqb://localhost:4222`               | `subject` |
| AMQP      | `amqp+mqb://localhost:5672/vhost`         | `queue` |
| MQTT      | `mqtt+mqb://localhost:1883?qos=1`         | `topic` |
| ZeroMQ    | `zeromq+mqb://localhost:5555?socket_type=pull` | `topic` |
| AWS SQS   | `aws+mqb://?region=us-east-1` (queue via `--source-table`) | `queue_url` |
| IBM MQ    | `ibmmq+mqb://host:1414?queue_manager=QM1&channel=DEV.APP.SVRCONN` | `queue` |
| memory    | `memory+mqb://orders?capacity=4096`       | `topic` |

The `--source-table` value supplies the topic-like field for the transport. An explicit
`?topic=` / `?subject=` / `?queue=` / `?queue_url=` query parameter overrides it.

The authority (the part before `?`) becomes the broker URL; the query string becomes the
endpoint's config fields. **Kafka and NATS** accept a comma-separated host list in the
authority for clusters/replicas (e.g. `kafka+mqb://b1:9092,b2:9092?group_id=g`). **MQTT and
AMQP are single-host** — front multiple brokers with a load balancer or DNS. **AWS SQS** has no
separate connection URL: the queue is named by its full `queue_url`, supplied via
`--source-table` (or `?queue_url=...`, percent-encoded), with `region` discovered from the URL
or `?region=`. **IBM MQ** addresses queue managers as `host(port)`, but you write the familiar
`host:port` authority (comma-separated for failover, e.g. `ibmmq+mqb://h1:1414,h2:1414`) and
omniload translates it. `queue_manager` and `channel` are required query parameters;
`--source-table` names the target queue, or pass `?topic=` to consume in pub/sub subscriber mode.

#### memory: in-process & IPC channels
The memory transport's `url`/`topic` are aliases for a single channel identifier, which may be:

- a bare name — `memory+mqb://orders` (mq-bridge treats it as `memory://orders`);
- a `memory://` URL — `memory+mqb://orders`;
- an `ipc://`, `unix://`, or `pipe://` channel for cross-process delivery. Because these carry
  their own scheme, pass them through `--source-table` (or `?url=`), not the authority:
  `--source-table 'ipc:///tmp/mq.sock'` or `memory+mqb://?url=ipc://my-queue`.

### Connectivity parameters
Any field accepted by the transport's mq-bridge endpoint config can be passed as a query
parameter; it is forwarded verbatim to `mq_bridge.Consumer`, with numeric/boolean fields
coerced from their string form. The consumer-relevant fields per transport:

| Transport | Useful query parameters |
|-----------|-------------------------|
| Kafka  | `group_id` (omit for ephemeral subscriber mode), `username` / `password` (SASL) |
| NATS   | `subject`, `stream`, `prefetch_count`, `no_jetstream` (Core NATS), `subscriber_mode`, `deliver_policy` (`all`/`last`/`new`/`last_per_subject`), `token` |
| AMQP   | `queue`, `exchange`, `prefetch_count`, `subscribe_mode` (fan-out), `no_declare_queue`, `no_persistence`, `username` / `password` |
| MQTT   | `qos` (0/1/2), `protocol` (`v5`/`v3`), `client_id`, `clean_session`, `keep_alive_seconds`, `max_inflight`, `session_expiry_interval`, `queue_capacity` |
| ZeroMQ | `socket_type` (`pull`/`sub`/`rep`/…), `bind` (bind vs connect), `topic` (SUB filter), `internal_buffer_size` |
| AWS SQS | `region`, `access_key` / `secret_key` / `session_token`, `endpoint_url` (e.g. LocalStack), `wait_time_seconds` (long-poll), `binary_payload_mode` |
| IBM MQ | `queue_manager` **(required)**, `channel` **(required)**, `username` / `password`, `cipher_spec`, `topic` (switch to pub/sub subscriber mode), `wait_timeout_ms`, `max_message_size`, `disable_status_inq` |
| memory | `capacity`, `subscribe_mode` (fan-out vs queue), `enable_nack` |

For the authoritative field list per transport, see mq-bridge's
[configuration guide](https://github.com/marcomq/mq-bridge/blob/main/CONFIGURATION.md) and
[`mq-bridge.schema.json`](https://github.com/marcomq/mq-bridge/blob/main/mq-bridge.schema.json).
Fields exposed only to publishers (e.g. `request_reply`, `stream_max_messages`) are accepted but
have no effect on a source. Note that mq-bridge's AWS `max_messages` (the SQS receive batch
size, ≤ 10) is shadowed by omniload's own `max_messages` transfer parameter below and uses the
mq-bridge default.

### Authentication & TLS
Simple credential fields are flat query parameters: `?username=u&password=p` (Kafka SASL /
AMQP / MQTT), `?token=...` (NATS).

TLS lives in a nested `tls` block in mq-bridge's config. Express it with **dotted query keys**,
which expand into the nested block:

```text
kafka+mqb://broker:9093?group_id=g&tls.required=true&tls.ca_file=/etc/ssl/ca.pem
```

becomes `{"kafka": {..., "tls": {"required": true, "ca_file": "/etc/ssl/ca.pem"}}}`. The
supported `tls.*` keys are `required`, `ca_file`, `cert_file` / `key_file` (mTLS),
`cert_password`, and `accept_invalid_certs`. This dotted-key expansion works for any nested
config block, not just `tls`.

### Transfer parameters
These drive the consume loop and are **not** forwarded to the broker config:

- `max_messages`: upper bound of messages drained per run, defaults to 100000.
- `idle_timeout_ms`: how long to wait for new messages before stopping, defaults to 2000.
- `batch_size`: messages fetched per poll, defaults to 500.
- `format`: `json` (default) decodes the payload as JSON; `text` stores the raw text under
  a `value` column. Any other value is rejected.

## Output format
Each message is stored as a row. The decoded payload becomes the top-level columns, plus:

| Column | Type | Description |
|--------|------|-------------|
| `_mqb_id` | VARCHAR | The message's stable source position (Kafka `partition:offset`, NATS stream sequence, AMQP delivery tag). Used as the **merge key**. |
| `_mqb_metadata` | JSON | The message metadata as reported by mq-bridge. |

## Delivery semantics
Delivery is **at-least-once**: each batch's offset is acked only **after** the dlt load has
durably committed. If the load fails, nothing is acked and the broker redelivers the batch on
the next run. Because the resource merges on `_mqb_id`, redelivered messages are deduplicated —
effectively-once.

Acks are per-batch (via mq-bridge's `poll_batch`/`ack` tokens), so only batches that were fully
handed to the load package are acked. `--yield-limit` is therefore safe: a limit that stops
mid-batch leaves that batch un-acked, so it is redelivered on the next run and deduplicated on
`_mqb_id` rather than being silently dropped.

mq-bridge owns both keys behind this guarantee, so two flags are rejected rather than silently
honored: `--incremental-key` (mq-bridge manages incrementality itself) and `--primary-key`
(which would override the `_mqb_id` merge key and break deduplication).

## Sample command

### memory transport to DuckDB
A brokerless smoke test using the in-memory transport:

```sh
omniload ingest \
    --source-uri 'memory+mqb://?topic=orders&capacity=4096' \
    --source-table 'orders' \
    --dest-uri 'duckdb:///mqbridge.duckdb' \
    --dest-table 'dest.orders'
```

### Kafka to PostgreSQL
```sh
omniload ingest \
    --source-uri 'kafka+mqb://localhost:9092?group_id=omniload' \
    --source-table 'orders' \
    --dest-uri 'postgres://postgres:postgres@localhost:5432/?sslmode=disable' \
    --dest-table 'public.orders'
```

The result is a `public.orders` table with the message payload's top-level JSON keys as
columns, plus `_mqb_id` and `_mqb_metadata`.
