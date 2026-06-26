# Changelog

## in progress

- Connectors: Updated to Asana client v5
- Connectors: Added source and target adapters for the Delta Lake table format

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
