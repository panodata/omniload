(deltalake)=

# Delta Lake

[Delta Lake] is an open table format for data lakes. A Delta Lake table stores
data files together with a transaction log. The transaction log provides the
table metadata and the table version history.

`omniload` supports Delta Lake as a source and destination. It reads Delta Lake
tables with Polars and writes Delta Lake tables with dlt.

## Supported URI schemes

Use a Delta Lake URI scheme for the storage system that contains the Delta Lake
catalog.

| URI scheme | Source | Destination | Description |
| --- | --- | --- | --- |
| `file+delta://` | Yes | Yes | Local file system |
| `s3+delta://` | Yes | Yes | Amazon S3 or an S3-compatible object store |
| `gs+delta://` | Yes | Yes | Google Cloud Storage |
| `az+delta://` | Yes | Yes | Azure Storage |
| `hdfs+delta://` | Yes | Yes | Hadoop Distributed File System |
| `lakefs+delta://` | Yes | Yes | lakeFS |
| `uc+delta://` | Yes | No | Unity Catalog |

The `+delta` suffix selects the Delta Lake connector. `omniload` removes this
suffix before it accesses the storage system.

## URI format

For storage-backed Delta Lake catalogs, use a URI that identifies the catalog
root.

```text
<storage>+delta://<catalog-root>?<storage-option>=<value>
```

The `--source-table` and `--dest-table` values identify a table within that
catalog.

```text
<schema>.<table>
```

`omniload` appends `<schema>/<table>` to the catalog URI when it reads a source
table. It writes the destination table to the dataset named by `<schema>`.

For a local catalog, use the following format.

```text
file+delta:///path/to/catalog
```

For an S3 catalog, use the following format.

```text
s3+delta://<bucket>/<catalog-prefix>?<storage-option>=<value>
```

For a Google Cloud Storage catalog, use the following format.

```text
gs+delta://<bucket>/<catalog-prefix>?<storage-option>=<value>
```

For an Azure Storage catalog, use the following format.

```text
az+delta://<container>/<catalog-prefix>?<storage-option>=<value>
```

For an HDFS catalog, use the following format.

```text
hdfs+delta://<host>:<port>/<catalog-prefix>?<storage-option>=<value>
```

For source reads, `omniload` passes URI query parameters to
[`polars.scan_delta`] as storage options. Use the storage options that apply to
your storage system and authentication method.

For destination writes, `omniload` passes the URI and its query parameters to
the dlt filesystem destination. See the relevant storage guide for
authentication details:

- {ref}`s3`
- {ref}`gcs`
- {ref}`azure-storage`
- {ref}`hdfs`

## Table layout

A storage-backed Delta Lake catalog uses this layout.

```text
<catalog-root>/
└── <schema>/
    └── <table>/
        ├── _delta_log/
        └── part-*.parquet
```

The `_delta_log` directory is required. Do not use a Parquet directory that
does not contain this directory as a Delta Lake table.

## Read a Delta Lake table

The following command reads `analytics.events` from a local Delta Lake catalog
and loads the data into DuckDB.

```sh
omniload ingest \
    --source-uri 'file+delta:///data/delta-catalog' \
    --source-table 'analytics.events' \
    --dest-uri 'duckdb:///analytics.duckdb' \
    --dest-table 'public.events'
```

This command reads the Delta Lake table at:

```text
/data/delta-catalog/analytics/events
```

## Write a Delta Lake table

The following command loads a DuckDB table into a local Delta Lake catalog.

```sh
omniload ingest \
    --source-uri 'duckdb:///analytics.duckdb' \
    --source-table 'public.events' \
    --dest-uri 'file+delta:///data/delta-catalog' \
    --dest-table 'analytics.events'
```

This command creates or updates the Delta Lake table at:

```text
/data/delta-catalog/analytics/events
```

The destination uses the Delta table format. The destination table name must
contain both a schema and a table name.

## Read from object storage

For object storage, use the same table-name format. Add the storage
authentication options to the source URI.

```sh
omniload ingest \
    --source-uri 's3+delta://my-bucket/lakehouse?access_key_id=<access-key-id>&secret_access_key=<secret-access-key>' \
    --source-table 'analytics.events' \
    --dest-uri 'duckdb:///analytics.duckdb' \
    --dest-table 'public.events'
```

The connector reads this Delta Lake table:

```text
s3://my-bucket/lakehouse/analytics/events
```

Use the equivalent `gs+delta://`, `az+delta://`, `hdfs+delta://`, or
`lakefs+delta://` URI for the corresponding storage system.

## Unity Catalog

`uc+delta://` is available as a source only. The connector passes the Unity
Catalog URI to Polars after it removes the `+delta` suffix.

Use a Unity Catalog URI and authentication options that
[`polars.scan_delta`] supports.

## Incremental loading

Do not specify `--incremental-key` for a Delta Lake source. The Delta Lake
source manages its own incremental behavior.

```text
DeltaLake takes care of incrementality on its own, you should not provide incremental_key
```

The connector reads source data in streaming batches. The default batch size is
75,000 rows.

## Limitations

- A storage-backed source table name must use exactly `<schema>.<table>`.
- `uc+delta://` is not available as a destination.
- Delta Lake merge, delete-and-insert, truncate-and-insert, and SCD2 strategies
  are not supported.

[Delta Lake]: https://delta.io/
[`polars.scan_delta`]: https://docs.pola.rs/api/python/stable/reference/api/polars.scan_delta.html
