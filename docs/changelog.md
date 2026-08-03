# Changelog

## in progress

- Database: Load SQLite and DuckDB databases that live on a remote filesystem,
  addressed by their plain object URI, for example
  `s3://analytics/snapshots/events.duckdb`. Thanks, @hampsterx.
- Filesystem: Treat XLSX and ODS workbooks as multi-table sources by default,
  while preserving one-table loads through explicit worksheet selectors.
  Thanks, @hampsterx.
- Filesystem: Refreshed S3 directory listings between repeated loads so
  file-level incremental runs detect newly uploaded objects.
- Connectors: Added Azure Storage destination support for connection-string
  authentication and custom endpoints.
- Connectors: Reject mixed Azure Storage credential modes instead of silently
  ignoring extra fields in source and destination URIs.
- Filesystem: Fail ingestion when a concrete source path matches no file while
  keeping unmatched glob selections valid. Thanks, @hampsterx.
- Filesystem: Added rsync source connector. Thanks, @oferchen.

## 2026/07/27 v0.8.0

- Core: Added real [SCD2] loading when selecting the `scd2` strategy.
  Thanks, @hampsterx.
- Filesystem: Added file-level incremental loading.
  Thanks, @hampsterx.
- Filesystem: Migrated local filesystem access to Apache Arrow.
- Filesystem: Added support for reading from Databricks, Dropbox,
  FTP, Google Drive, HDFS, OCI, OneDrive, OSS, R2, SharePoint,
  SMB, WebDAV, and WebHDFS.
- Core: Applied a few bits of internal package refactoring.

## 2026/07/16 v0.7.0

- Connectors: Added Azure Blob Storage (`az://`) and Azure Data Lake Storage
  Gen2 (`adls://`, `abfss://`) source and destination adapters, backed by
  `adlfs`. Supports account-key, SAS-token, and service-principal auth.
  Thanks, @hampsterx.
- Connectors: Fixed remote filesystem destinations (Azure Blob, GCS, S3)
  aborting at the end of a load because the required `post_load` hook was
  missing from the shared base class. Thanks, @hampsterx.
- Filesystem: Added `#key=value` reader hint channel to source URIs
  to propagate options to source implementations. Thanks, @hampsterx.
- Filesystem: Added readers for CBOR, MessagePack, XML and YAML formats.
  Thanks, @hampsterx.
- Filesystem: Added readers for Excel workbook (XLSX) and OpenDocument
  spreadsheet (ODS) formats.
- Filesystem: Fixed and documented semantics of append/replace strategy
  and append/full-refresh behaviour. Thanks, @hampsterx.
- Dependencies: Updated to dlt 1.29

## 2026/07/06 v0.6.0

- Connectors: Added `file://` source and destination adapters that
  read and write from/to local CSV / JSONL / Parquet files.
  Thanks, @hampsterx.
- Connectors: Refactored filesystem internals
- Connectors: Migrated filesystem CSV reader from pandas to Polars
- Filesystem readers: Added support for BSON. Thanks, @hampsterx.

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
[SCD2]: https://en.wikipedia.org/wiki/Slowly_changing_dimension#Type_2:_add_new_row
