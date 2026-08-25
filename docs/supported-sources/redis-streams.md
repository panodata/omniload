# Redis Streams

[Redis Streams](https://redis.io/docs/latest/develop/data-types/streams/) is Redis' append-only
log data type, with consumer groups that track per-consumer delivery and a pending-entries list
(PEL) for messages that were read but never acknowledged.

omniload consumes from Redis Streams via [mq-bridge](mqbridge.md).

## URI format

```text
redis-streams+mqb://localhost:6379?group=omniload
```

mq-bridge names this endpoint `redis_streams`, but a URI scheme cannot contain an underscore
(RFC 3986), so omniload addresses it as `redis-streams+mqb://`. The authority becomes a
`redis://` URL; for TLS, pass the URL explicitly instead — `?url=rediss://host:6379`, which
wins over the authority.

`--source-table` supplies the **stream** key. Common options: `group` (consumer group),
`consumer_name`, `block_ms` (how long `XREADGROUP` blocks), `read_from_start`,
`redelivery_timeout_ms` (reclaim stale PEL entries), `username` / `password`,
`subscriber_mode` (fan-out instead of a shared group). See [mq-bridge](mqbridge.md) for the
full option reference, TLS, and delivery semantics.

> Without a `group`, the consumer reads the stream directly and nothing tracks acknowledgement
> across runs. Set a `group` for the commit-after-load guarantee omniload's mq-bridge source
> is built around.

## Sample command

```sh
omniload ingest \
    --source-uri 'redis-streams+mqb://localhost:6379?group=omniload' \
    --source-table 'orders' \
    --dest-uri 'duckdb:///redis.duckdb' \
    --dest-table 'dest.orders'
```
