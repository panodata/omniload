# Changelog

## in progress

- **Filesystem: a `dlt_source` implementation now declares the run options it
  consumes, instead of the run subtracting its own vocabulary from every call.**
  `filesystem_incremental` and `column_types` are named parameters on every
  reader in the family now, not names read out of `**kwargs`, so a caller
  passing something else in omniload's own run vocabulary (`page_size`, say)
  gets it forwarded straight to the fsspec/Arrow constructor rather than
  silently dropped. Most backends ignore an unknown keyword; at least one
  (WebDAV) raises. A row-level `--incremental-key` against one of these sources
  now raises for every scheme in the family (some silently accepted it before,
  since the guard was keyed on a value the caller had already cleared).

## 2026/09/10 v0.16.0

- **Filesystem: `filesystem()` and `readers()` accept every keyword dlt's own accept.**
  A call written against `dlt.sources.filesystem` that passes `kwargs`, `client_kwargs`
  or `incremental` by keyword now works against `dlt_filesystem.source.adapter` too,
  where it used to raise `TypeError`. `kwargs` and `client_kwargs` reach the fsspec
  constructor and its native client; an `incremental` cursor carrying `row_order` orders
  the listing by its cursor field, matching dlt for both `last_value_func` directions.
  The three are appended keyword-only rather than placed where dlt has them, so no
  existing positional call changes meaning; the flip side is that compatibility is by
  keyword, since in `filesystem()` dlt's positional slots for `kwargs` and
  `client_kwargs` are where `require_file_match` and `filesystem_incremental` sit.
  `extract_content`'s declared default is now `False`, which is what it has always
  resolved to and what dlt declares; a caller who wants file content in the listing
  passes `extract_content=True`, as before.
  Declaring `incremental` also makes dlt attach its `IncrementalResourceWrapper`, as it
  does to its own filesystem resource, so a caller reading `resource.incremental` gets
  the wrapper: the cursor is `resource.incremental.incremental`, and a resource with no
  cursor applied is that being `None` rather than `resource.incremental` itself. Bound
  clones of one resource now share a cursor, which is what dlt's do today rather than a
  guarantee; build separate resources where the cursors have to be independent.
- **Filesystem: Apache Avro reads on every filesystem source.**
  `file://events/day.avro` loads, as does `s3://bucket/events/day.data#avro` and any
  other source that goes through the shared file readers. Avro is decoded with `polars`,
  which is already a core dependency, so nothing new is installed. Logical types come
  back as themselves: `date` as a date, `timestamp-micros` and `timestamp-millis` as
  UTC-aware datetimes, `time-micros` as a time, `decimal` as a decimal. `#columns=` and
  `#chunksize=` work as they do on the other columnar readers. Avro is read-only: the
  Avro writer available here corrupts a list column that holds an empty list before a
  non-empty one, so registering it would offer an export that fails silently on ordinary
  data. Three Avro schemas have no mapping and are rejected by name rather than loaded
  in part: a `map` field, a field typed `null` outright, and a union of more than two
  branches.
- **Filesystem: `file://` writes CSV and Parquet through Polars.** Both writers built
  their column union by hand, because dlt omits a null key rather than writing it, and
  Polars builds that union itself from every row of the load. What the move changes is
  how a value is spelled, not which rows are written. A boolean writes as `true` / `false` in CSV instead of `True` / `False`,
  which is what the JSON and YAML writers already emit for the same value. A nested
  document writes as JSON (`{"a": 1}`) instead of as a Python repr (`{'a': 1}`), which
  no reader could parse, and binary as the base64 string the JSON writers use instead
  of as `b'hi'`. Under `--loader-file-format parquet`, the one load path that hands a
  writer native values, a time column carries nanosecond rather than microsecond
  precision, a datetime writes ISO 8601 with a `T` separator, and a Parquet decimal
  column is declared with the format's maximum precision rather than the source's,
  which leaves the values and their scale as they were. Some floats are spelled
  differently in CSV without changing value: `1e-05` writes as `0.00001`, `1e-07` as
  `1e-7`, and a NaN as `NaN`. CRLF line endings and Snappy
  Parquet compression are unchanged, and so are the Parquet column types apart from the
  time and decimal changes named above: a reader that reports Arrow types shows
  `large_string` where it showed `string`, which is the Arrow schema travelling alongside
  rather than the file's own, while the decimal annotation itself moves to the format's
  maximum precision and a wider fixed-length store. One case stops
  working: a Parquet export of a decimal column wider than 38 digits of precision, which
  PyArrow held in a 256-bit decimal and Polars cannot hold at all. The same column still
  exports to CSV, JSON, JSONL and YAML, digit for digit.
- **Filesystem: Feather V2 reads on every filesystem source.** `file://events/day.feather`
  (or `.arrow`, or `.ipc`, or a `#feather` hint on any name) loads Apache Arrow IPC data
  through PyArrow, one record batch at a time, so a batched file is never materialized
  whole. `#columns=` and `#chunksize=` work as they do for ORC. Feather V1 is a different
  container and is refused by name rather than reported as a damaged file.
- **Filesystem: `file://` writes Feather V2.** `file://export/users.feather` (or `.arrow`,
  or `.ipc`, or a `#feather` hint) writes one Arrow IPC file. Timezone-aware timestamps,
  times, decimals, nested lists and structs and all-null columns all reach the file as
  themselves when the writer is handed them. What a *load* delivers depends on dlt's
  staging rather than on the writer, and the filesystem page carries that table: the
  default staging writes temporal, decimal and binary columns as text, Parquet staging
  writes nested values as JSON text, a wholly null column is dropped either way, and a
  duration column fails at extract. That is `file://`-wide, not Feather's.
- **Filesystem: the supported-format message names formats, not extensions.** A reader
  registered under several extensions listed all of them, so `.arrow` and `.ipc` would have
  read as separate formats alongside Feather. Only the canonical name is advertised now,
  matching what the write side already did for `.yml`.

## 2026/09/07 v0.15.0

- **Filesystem: `file://` reads back every intermediate format dlt stages.** The staged
  files were typed by running `file(1)` and matching its English output, which is absent on
  Windows, varies by version, and cannot tell gzipped CSV from gzipped JSONL because dlt
  compresses both. So `--loader-file-format csv` failed with a JSON decode error, and an
  uncompressed JSONL intermediate loaded or was refused as an unknown format depending on
  how many rows it held, since `file` names a one-line file differently from a many-line
  one. Both work now: the
  format comes from the name dlt gave the file, CSV is read with the delimiter, encoding and
  line terminator dlt wrote it with, and the staging directory names its own files rather
  than inheriting a `layout` or `extra_placeholders` meant for a user's real filesystem
  destinations. A format with no row-shaped reader, such as `insert_values`, is named in the
  error instead of being misread as JSON, and a destination table in dlt's reserved `_dlt_`
  namespace is refused rather than exported with dlt's own bookkeeping mixed into the rows.
- **Filesystem: `file://` writes YAML.** `file://export/users.yaml` (or `.yml`, or a
  `#yaml` hint) writes one document holding a sequence, one mapping per row, which is
  the shape the `.yaml` reader expands into a table. Keys keep the column order the load
  produced, non-ASCII text is written as itself rather than escaped, and a load with no
  rows writes an empty sequence. Writing YAML needs PyYAML, the same
  `omniload[iterable]` extra that reading it needs.
- **Filesystem: `file://` writes JSON, and the writers are registered rather than
  hand-wired.** `file://export/users.json` (or a `#json` hint) writes one JSON array
  document, which is the shape the `.json` reader expects. Registering a writer is now a
  single record in `dlt_filesystem.target.registry`, the way readers already work: the
  supported-format tuple, the error message and the end-to-end test matrix are all
  derived from it, and a test fails when the documentation names a different set.
  Two writers claiming one format is now an error instead of last-one-wins.
- **Filesystem: written files are UTF-8 whatever the process locale is.** The readers
  decode as UTF-8 unconditionally, so a CSV or JSONL export containing non-ASCII text
  previously failed to write, or wrote bytes it could not read back, on a machine whose
  locale was not UTF-8.
- Verified compatibility with Polars 2
- **CSV: `csv://` now shares the `file://` implementation.** It stays a CSV-only
  scheme, and every existing command still resolves, but reading and writing are
  the local filesystem connector's, so the behaviour changes. On read: values are
  typed rather than all-string (numbers and booleans are inferred; ISO date
  strings stay strings), a row whose fields are all empty is kept as a row of
  nulls instead of being dropped, an empty field loads as null when unquoted and
  as an empty string when quoted (the old reader dropped every empty field, so
  both produced null), and the path grammar gains globs, gzip, split form,
  Windows drive and UNC paths, and `#csv_headless` / `#csv_duckdb` hints.
  A rerun with no `--incremental-strategy` now **appends** rather than replacing;
  pass `--incremental-strategy replace` for the previous behaviour, which a
  destination that implements no append disposition now requires rather than
  merely recommends (MongoDB is one, and rejects the load without it). `merge`,
  `delete+insert` and `scd2` are rejected, because the shared reader exposes no
  incremental or merge key, and `--incremental-key` is rejected for the same
  reason: the row cursor it drove compared raw strings against parsed datetimes
  and crashed when given an interval, so it is removed rather than ported.
  File-level selection by modification time is available through
  `--filesystem-incremental`. A non-CSV selection (`csv://data.jsonl`,
  `csv://feed.dat#parquet`, `csv://book.xlsx`) is now rejected before any file is
  read; use `file://` for those. On write, a load that dlt split across several
  files wrote only the first of them and still exited zero, silently dropping the
  rest; every row now reaches the output file, in a deterministic but not
  necessarily source order. A destination path naming a different known format
  (`csv://out.jsonl`, `#parquet`) is rejected, while an extensionless or
  unrecognized-extension path (`csv://report`, `csv://out.dat`) still writes CSV.
- Filesystem: added a reader for Apache ORC files.
- Filesystem: added local ORC output through `file://`, implemented with PyArrow and
  preserving columns first encountered in later rows.

## 2026/09/01 v0.14.0

- Filesystem: Give the Arrow-backed filesystems (`file://`, `s3://`, `r2://`, `az://`,
  `hdfs://`, `rsync://`) read handles that implement `readinto`. fsspec's `ArrowFile`
  mirrors a fixed method list from the pyarrow stream it wraps and leaves `readinto` off
  it, so any reader that fills a caller-supplied buffer fails. Gzip is where that
  surfaces: fsspec registers isal's `IGzipFile` as its `gzip` codec when `isal` imports
  and the stdlib `GzipFile` only when it does not, and isal decompresses through
  `readinto`. A `.gz` file read through any of those schemes therefore raised
  `AttributeError: 'ArrowFile' object has no attribute 'readinto'` in an environment that
  merely carried the package, and read fine everywhere else. omniload declares no
  dependency on isal, but anything pulling `xopen` into the same environment brings it on
  x86-64 and AArch64.
- Core: Add `--reshape <engine>:<spec>`, restructuring each source document before it
  is loaded. It flattens nested objects into columns, coerces types, and leaves arrays
  as real lists rather than opaque JSON. Three engines:
  `python:<module>:<callable>` (per row, preserves `Decimal`), `jq:<program>` (per row,
  through Tikray) and `polars:<recipes>` (per Arrow batch, compiled through macropipe).
  The jq and Polars engines need the new `omniload[reshape]` extra, and the Polars one
  needs an Arrow-yielding source, which today means MongoDB only. While a reshape is
  active the MongoDB source no longer json-hints top-level arrays, so they land as
  child tables rather than JSON columns. That last part is MongoDB-only: filesystem
  readers register `max_table_nesting = 0`, so a list a reshape produces there stays a
  JSON column.
- MongoDB: Build a `PyMongoArrowContext` per wire batch in the Arrow loaders rather
  than once per cursor. `finish()` leaves the builders it accumulated in place, so the
  shared context replayed each earlier batch as a row of nulls: a collection spanning
  two batches emitted phantom all-null rows, and did so on any primary key. Reachable
  since `data_item_format` became settable per run.
- Connector: Added source and target adapters for the Delta Lake table format

## 2026/08/27 v0.13.0

- mq-bridge: Require mq-bridge-py 0.4.7 (was 0.3.2) and add three transports:
  Redis Streams (`redis-streams+mqb://`), Apache Pulsar (`pulsar+mqb://`) and
  WebSocket (`websocket+mqb://`). Pulsar is not built into
  mq-bridge but ships as a separate native plugin, installed via the new
  `omniload[pulsar]` extra and registered on demand; it stays out of the base
  install (and out of `omniload[full]`) because its wheels require glibc >= 2.34 on
  x86_64, glibc >= 2.38 on aarch64, and omit Windows arm64, all narrower than
  omniload's own platform floor. mq-bridge names its Redis endpoint
  `redis_streams`, but a URI scheme cannot contain an underscore, so omniload
  addresses it as `redis-streams+mqb://`. Note that a WebSocket *source* is a
  server: mq-bridge binds the authority and ingests the frames clients push to it,
  so `--source-table` names the HTTP path served and the run is bounded by
  `max_messages`/`idle_timeout_ms` rather than by end-of-stream.
- mq-bridge: Nack the batches left outstanding at the end of a run instead of only
  closing the consumer, so the broker redelivers them immediately rather than
  after a visibility timeout. This covers both a failed load and a `--yield-limit`
  that truncated a batch mid-yield. On transports with no per-message nack --
  Kafka -- this still only leaves the offset unadvanced, so redelivery waits for
  the next run or rebalance as before. Delivery is unchanged otherwise -- still
  at-least-once, deduplicated on `_mqb_id`.
- Filesystem: build reader transformers from registration records instead of a
  second hand-written adapter list, and expose the DuckDB CSV reader through
  `#csv_duckdb` and `.csv_duckdb`.
- Connectors: Added support for Apache Pulsar, WebSockets, Redis streams.
  Thanks, @marcomq.
- Connectors: Removed gRPC due to found issues. Thanks, @marcomq.

## 2026/08/24 v0.12.0

- HTTP: Support `--filesystem-incremental` when every selected file returns a
  valid `Last-Modified` header. A glob over an HTML directory index obtains the
  timestamp with one metadata request per matched file. A concrete file reuses
  the header already returned during discovery. A missing or malformed header
  stops extraction and names the query-free file URL instead of receiving dlt's
  synthesized current time and reloading on every run.
- HTTP: Resolve `*`, `?` and recursive `**` wildcards against links exposed by a
  HTML directory index. The server's index remains the boundary of what the
  source can discover.
- Table names: One quote-aware parser reads `catalog.schema.table` for every source
  and destination, replacing six ad-hoc conventions that each did something different
  with the same spelling. Names are right-aligned, so a shorter name keeps its shape:
  `table` and `schema.table` are still spelled the way they were, with the MySQL and
  SQLite routing changes below the exception to what they mean.
  Components may be quoted individually with `"`, `` ` `` or `[]`, which is how a
  name containing a dot is written; a whole path wrapped in one pair of backticks is
  refused by name, because each component quotes separately. Every platform declares
  the shape it accepts, so a name it cannot represent is rejected with a message that
  says what the platform takes and where to put the catalog instead, rather than being
  silently truncated or flattened. Thanks, @hampsterx.
- Table names: A three-component name now reaches the destination's catalog. It sets
  `project_id` on BigQuery, `catalog` on Databricks, the database on MotherDuck, and
  `aws_data_catalog` on Athena, which had no way to reach a non-default catalog at
  all. For Postgres, Snowflake, Redshift, MSSQL, Synapse and Trino the catalog is the
  database the connection opens, so the first component selects it: `--dest-table
  reporting.public.orders` connects to `reporting` whatever the URI path said. That is
  a deliberate call, and the URI stays the way to pin one database for a run. DuckDB
  and CrateDB keep their two-level names, since neither can address a second database
  on one connection. Thanks, @hampsterx.
- Table names: Three behaviour changes come with that. A dotted MySQL or SQLite
  destination name is now routed rather than flattened: `--dest-table foo.bar` on
  MySQL writes table `bar` in database `foo`, where it previously created one table
  whose name normalized from the literal string `foo.bar`, and SQLite's `main.widgets`
  no longer becomes a table called `main.widgets` in `main`. Both wrote to the wrong
  place before, so a pipeline relying on the old spelling loads somewhere new. And a
  three-part SQL *source* name is now refused rather than misparsed. It used to be
  cut at the first dot, so `public.order.items` silently became schema `public` and
  table `order.items`; SQL sources are two-level, so it now gets an error naming the
  accepted format and pointing at `--source-uri` for the catalog. A table whose own
  name contains a dot is spelled `public."order.items"`. Thanks, @hampsterx.

## 2026/08/21 v0.11.0

- Azure: Read `az://`, `adls://` and `abfss://` through `pyarrow.fs` instead of
  adlfs, for the per-file open cost every glob load pays. Measured against
  Azurite: opening and reading 300 small blobs costs 2.4 ms/blob through Arrow
  against 6.7 ms/blob through adlfs, and a 64 MB read runs at 456 MB/s against
  193 MB/s. Listing a glob is one recursive request, recursive only when the
  pattern crosses a level. One client serves flat-namespace and Gen2 accounts,
  which it detects itself, and a custom `account_host` now yields both the Blob
  and the Data Lake endpoint so a Gen2 account behind a sovereign-cloud or
  emulator endpoint stays reachable. Every credential mode the connector accepts
  is carried over: account key, SAS token, service principal, and connection
  string, and a connection string's `DfsEndpoint` is honoured as given rather
  than derived. File-level incremental selection and its cursor identity are
  unchanged, so no cursor resets. Writing, and the staging download that serves
  a remote database file, continue to use adlfs. Thanks, @hampsterx.
- Azure: Two behaviour changes come with that move. `api_version` is no longer
  accepted on a source URI and is rejected with a named error, because Arrow
  pins the API version its bundled SDK speaks and offers no override; an
  emulator that enforces the version check needs to be started with
  `--skipApiVersionCheck`. And a source connection string now has to name its
  account: one that identifies the account by endpoint alone (a bare
  `SharedAccessSignature` with a custom `BlobEndpoint` and no `AccountName`) is
  rejected, because the reader takes the storage account as the root of the
  filesystem. Thanks, @hampsterx.

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
