# NATS

[NATS](https://nats.io/) is a high-performance messaging system for cloud-native applications,
supporting both Core NATS (fire-and-forget) and JetStream (persistent streams).

omniload consumes from NATS via [mq-bridge](mqbridge.md).

## URI format

```text
nats+mqb://localhost:4222?stream=ORDERS
```

`--source-table` supplies the NATS **subject**. The authority accepts a comma-separated server
list for clusters. Common options: `stream`, `prefetch_count`, `no_jetstream` (use Core NATS),
`subscriber_mode`, `deliver_policy` (`all`/`last`/`new`/`last_per_subject`), `token`. See
[mq-bridge](mqbridge.md) for the full option reference, TLS, and delivery semantics.

## Sample command

```sh
omniload ingest \
    --source-uri 'nats+mqb://localhost:4222?stream=ORDERS' \
    --source-table 'orders.created' \
    --dest-uri 'duckdb:///nats.duckdb' \
    --dest-table 'dest.orders'
```
