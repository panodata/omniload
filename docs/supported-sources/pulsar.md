# Apache Pulsar

[Apache Pulsar](https://pulsar.apache.org/) is a distributed pub/sub and event-streaming
platform with durable, cursor-based subscriptions and multi-tenant topics.

omniload consumes from Pulsar via [mq-bridge](mqbridge.md).

## Installation

Pulsar is **not** built into mq-bridge. It ships as a separate native plugin, which omniload
loads on demand:

```sh
pip install 'omniload[pulsar]'
```

The plugin publishes wheels for macOS (x86_64/arm64), Linux x86_64 (glibc ≥ 2.34), Linux
aarch64 (glibc ≥ 2.38) and Windows x64. It is kept out of the base install — and out of
`omniload[full]` — because that is narrower than omniload's own platform floor; note in
particular that the aarch64 floor rules out Ubuntu 22.04 arm64 (glibc 2.35). There is no sdist,
so an unsupported platform fails at install time with "no matching distribution", and a
`pulsar+mqb://` ingest without the plugin fails fast, naming the extra.

## URI format

```text
pulsar+mqb://localhost:6650?subscription=order-workers
```

`--source-table` supplies the **topic**, which may be a short name or a fully qualified one
(`persistent://public/default/orders`). Options: `subscription` and `initial_position`.

`subscription` is the durable cursor: reusing an existing subscription resumes from its
committed position. `initial_position` (`earliest` / `latest`, default `latest`) applies **only
when the subscription is created** — pointing a run at a subscription that has already read to
the end will not replay it, so use a fresh `subscription` name to re-read a topic's backlog.

See [mq-bridge](mqbridge.md) for the transfer parameters and delivery semantics.

## Sample command

```sh
omniload ingest \
    --source-uri 'pulsar+mqb://localhost:6650?subscription=omniload&initial_position=earliest' \
    --source-table 'persistent://public/default/orders' \
    --dest-uri 'duckdb:///pulsar.duckdb' \
    --dest-table 'dest.orders'
```
