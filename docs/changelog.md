# Changelog

## in progress

- Connectors: Added source adapter for the XLSX file format

## 2026/07/02 v0.5.0

- Connectors: Updated to Asana client v5
- Core: Started using standard Python logger, removed `--quiet` option
- API: Made option `--source-table` optional to prepare for streaming sources
- Connectors: Added an mq-bridge source for streaming brokers
  (Kafka/NATS/AMQP/MQTT/ZeroMQ/IBM MQ/AWS SQS), via the `<transport>+mqb://`
  URI scheme, with dotted query keys (e.g. `tls.required=true`) for nested
  config such as TLS. Thanks, @marcomq.
- Maintenance: Started using `orjson` across the board
- Connectors: Centralize file-format to reader mapping across all sources.
  Thanks, @hampsterx.

## 2026/06/25 v0.4.0

- Maintenance: Refactored module namespace. If you are using omniload
  as a library, this introduces many breaking changes. However, the new
  layout is much more ergonomic.
- Core: Added lazy-loading adapter module registry, to speed up startup times.

## 2026/06/24 v0.3.0

- Feature: Added embeddable `run_ingest()` Python API. Thanks, @hampsterx.
- Dependencies: Updated to dlt v1.28
- Dependencies: Updated to clickhouse-connect v1

## 2026/06/21 v0.2.0

- Kafka: Expanded `default_msg_processor` into a miniature decoding unit
- dlt: Migrated from `ensure_pendulum_datetime` to `ensure_pendulum_datetime_utc`

## 2026/06/19 v0.1.0

- Dependencies: Permitted installation of SQLAlchemy 2.0
- Dependencies: Relaxed package requirements across the board
- Documentation: Migrated from VitePress to Sphinx
- Runtime: Used tqdm progress bar instead of spinner

## 2026/05/17 v0.0.0

- Imported code from [ingestr v0.14.155]
- Project: Make it a library: Streamlined dependencies, now relaxed and inlined
- Project: Migrated from Hatch to vanilla setuptools
- Project: Migrated from Makefile to Poe the Poet
- Project: Started using `versioningit` for versioning
- Project: Standardized version handling
- OCI: Modernized `Dockerfile`
- Documentation: Trimmed README, copy editing
- CI: Validated on Python 3.14
- OCI: Updated to Python 3.14 and Debian 13 "trixie"
- dlt: Turned off telemetry due to excessive requests
- Runtime: Removed interactive mode `--yes`, replaced with `--dry-run`
- Packaging: Modernized PyPI and OCI publishing

[ingestr v0.14.155]: https://github.com/bruin-data/ingestr/tree/v0.14.155
