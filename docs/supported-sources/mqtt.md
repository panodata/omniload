# MQTT

[MQTT](https://mqtt.org/) is a lightweight publish/subscribe messaging protocol designed for
constrained devices and low-bandwidth, high-latency networks (IoT).

omniload consumes from MQTT brokers via [mq-bridge](mqbridge.md).

## URI format

```text
mqtt+mqb://localhost:1883?qos=1
```

`--source-table` supplies the MQTT **topic** (wildcards such as `sensors/#` are allowed). Common
options: `qos` (0/1/2), `protocol` (`v5`/`v3`), `client_id`, `clean_session`,
`keep_alive_seconds`, `max_inflight`. See [mq-bridge](mqbridge.md) for the full option reference,
TLS, and delivery semantics.

## Sample command

```sh
omniload ingest \
    --source-uri 'mqtt+mqb://localhost:1883?qos=1' \
    --source-table 'sensors/temperature' \
    --dest-uri 'duckdb:///mqtt.duckdb' \
    --dest-table 'dest.readings'
```
