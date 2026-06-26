# Changelog

## in progress

- Connector: Added source and target adapters for the Delta Lake table format

## 2026/08/18 v0.10.1

- Azure: Scope file-level incremental cursors from connection-string sources to
  the parsed storage account and Blob endpoint. Different accounts using the
  same container and glob no longer share a cursor and silently skip files.
  Existing connection-string cursors reset once on upgrade, so the first run
  reads the selection again and `append` loads may duplicate rows. Use
  `--full-refresh` for that first upgraded load when duplicates are unacceptable.
  Connection strings continue to reach adlfs unchanged.

## 2026/08/15 v0.10.0

- HTTP: Serve `http://` and `https://` through the filesystem family, so an HTTP
  URL reads with the same formats, format hints and reader hints as every other
  transport. New: gzipped documents, whole-JSON documents, XML, YAML, msgpack,
  CBOR, BSON and workbooks; a `#format` or `#key=value` fragment on the URL; and,
  where the server serves byte ranges, reading in ranges instead of downloading
  the whole body first. A signed URL keeps working: the query string is treated as
  part of the address, so an escape that carries meaning (`%2F`) reaches the server
  preserved rather than decoded, and the query stays out of record identity, log
  lines, and the errors discovery and whole-body reads raise. A server that ignores
  `Range` and one that answers chunked with no `Content-Length` are read whole, as
  before. Thanks, @hampsterx.
- HTTP: Four behaviour changes come with that move. A re-run with no
  `--incremental-strategy` now appends rather than replacing, matching the rest of
  the family, and an explicit `append` or `replace` is now honoured;
  `--incremental-key` is now rejected rather than ignored;
  `--filesystem-incremental` is refused, because an HTTP response need not carry a
  `Last-Modified` header and a missing one would read as "just now" and reload
  everything; and connection options can no longer ride the URI query string, since
  that is the address, so they are available only when building the source object
  directly, not through `omniload ingest` or `run_ingest`. Timeouts stay at 30
  seconds and environment proxy and `.netrc` settings are still honoured. Thanks,
  @hampsterx.
- Filesystem: Fixed `csv_headless`, which named columns with a Polars argument
  that *selects* columns instead. Reading a header-less CSV either failed outright
  or returned one shifted row, whether the names came from `--columns` or were
  generated. Thanks, @hampsterx.
- Filesystem: A `#format` fragment on a source URI is now honoured, not only one on
  `--source-table`. Reader hints (`#key=value`) were already read from either.
  Thanks, @hampsterx.
- Filesystem: Discovery no longer fails when a filesystem reports no size for a
  file, which any HTTP response without a `Content-Length` does. Thanks,
  @hampsterx.
- Filesystem: Added a `json` format, so a `.json` file loads on every transport in
  the family. An object becomes one row and an array one row per element, whether
  or not the document is pretty-printed, and a `.json` file that holds
  line-delimited records still loads. `.jsonl` keeps its own strict
  one-record-per-line reader. A URI fragment of `#json` is now read as a format
  hint rather than as literal text. Thanks, @hampsterx.

## 2026/08/12 v0.9.2

- Filesystem: Started honouring `--filesystem-incremental` and `--columns` on every
  transport in the family. They were accepted and then ignored on the fourteen
  schemes that build their resource through the shared locator, among them
  `ftp://`, `smb://`, `oss://`, `oci://`, `webhdfs://`, `dropbox://`,
  `gdrive://`, `sharepoint://` and `dbfs://`. Thanks, @hampsterx.
- Filesystem: Stopped passing omniload's own run parameters to fsspec filesystem
  constructors, which fragmented fsspec's instance cache and would be rejected
  by a backend that validates its keyword arguments. A source URI query
  parameter sharing a name with one of those run options is now ignored rather
  than forwarded; the run parameter already took precedence over it, so no
  connection setting changes meaning. Thanks, @hampsterx.

## 2026/08/05 v0.9.1

- Filesystem: List and read through every registered scheme, including `r2://`,
  `oss://`, `hdfs://`, `smb://`, `ftp://`, `dbfs://`, `oci://` and `webhdfs://`,
  by resolving a file's modification date from the listing the filesystem client
  returns.

## 2026/08/04 v0.9.0

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
- Filesystem: Enabled loading multiple workbook worksheets as tables.
  Thanks, @hampsterx.
- Filesystem: Enabled loading remote SQLite and DuckDB databases.
  Thanks, @hampsterx.

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
