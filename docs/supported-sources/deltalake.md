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

For destination writes to S3, Google Cloud Storage and Azure Storage, `omniload`
builds storage credentials from the URI query parameters and passes the
query-free URI to the dlt filesystem destination. Supply the options your storage
system requires, or supply none of them and let the environment provide the
credentials. See the relevant storage guide for the option names:

- {ref}`s3`
- {ref}`gcs`
- {ref}`azure-storage`

`hdfs+delta://` and `lakefs+delta://` destinations take no options from the
query; see Limitations.

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

:::{note}
On write, dlt normalizes the schema and table names before it builds the
directory layout. `analytics.events` writes to `analytics/events`, and
`delta-schema.delta-table` writes to `delta_schema/delta_table`. The source does
not normalize; it reads the directory the table name spells. Names that survive
normalization therefore address the same directory on both sides, and a catalog
`omniload` wrote is read back by its normalized name.
:::

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

The Delta Lake source reads the whole table on every run, so there is no cursor
for `--incremental-key` to advance and the source rejects it.

```text
DeltaLake takes care of incrementality on its own, you should not provide incremental_key
```

Each run of a Delta Lake source replaces the destination table with what it
read. Use `--incremental-strategy append` to add the rows to the destination
instead.

The connector reads source data in streaming batches. `--page-size` sets the
batch size.

## Limitations

- A storage-backed table name must name both a schema and a table. Quote a
  component that contains a dot, as in `"my.schema".events`.
- `uc+delta://` is not available as a destination. As a source it addresses the
  table in the URI, so `--source-table` only names the dlt resource.
- The `lakefs+delta://` and `hdfs+delta://` destinations take no storage options
  from the URI query, because dlt models no credentials for those protocols.
- The merge, delete-and-insert, and SCD2 strategies need an incremental or merge
  key, which a whole-table read does not expose.

[Delta Lake]: https://delta.io/
[`polars.scan_delta`]: https://docs.pola.rs/api/python/stable/reference/api/polars.scan_delta.html
