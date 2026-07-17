(azure-data-lake-storage)=

# Azure Data Lake Storage

[Azure Data Lake Storage Gen2](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction) is a set of capabilities built on Azure Blob Storage for big-data analytics. A Gen2 account is a storage account with a hierarchical namespace enabled; it shares the same underlying service and API as Azure Blob Storage.

`omniload` supports Azure Data Lake Storage Gen2 as both a data source and destination through the `adls://` and `abfss://` schemes.

## URI Format

```text
adls://?account_name=<your_account_name>&account_key=<your_account_key>
```

`abfss://` is accepted as well and behaves identically; it is the dominant Gen2 scheme in the Spark, Databricks, and Synapse ecosystems:

```text
abfss://?account_name=<your_account_name>&account_key=<your_account_key>
```

Both schemes are aliases of [Azure Blob Storage](azure-blob-storage.md): they share the same `adlfs` backend, accept the same URI parameters and authentication modes (account key, SAS token, service principal), and resolve to the same `az://` bucket internally. A Gen2 account differs from a plain Blob account only by having its hierarchical namespace enabled, so no separate configuration is required.

**URI Parameters, authentication, and credential URL-encoding are identical to Azure Blob Storage.** See the [Azure Blob Storage](azure-blob-storage.md) page for the full parameter table, the auth-mode rules, and the important note on URL-encoding account keys and SAS tokens.

The `--source-table` parameter specifies the container (filesystem) and file pattern:

```
<container-name>/<file-glob-pattern>
```

## Example: Loading data from ADLS Gen2

```sh
omniload ingest \
    --source-uri 'adls://?account_name=mystorageacct&account_key=dGVzdA%3D%3D' \
    --source-table 'my-filesystem/events/2024/*.parquet' \
    --dest-uri duckdb:///adls_data.duckdb \
    --dest-table 'analytics.events'
```

## Example: Uploading data to ADLS Gen2

```sh
omniload ingest \
    --source-uri 'duckdb:///records.db' \
    --source-table 'public.users' \
    --dest-uri 'abfss://?account_name=mystorageacct&account_key=dGVzdA%3D%3D' \
    --dest-table 'my-filesystem/users'
```

For glob patterns, compressed-file handling, file type hinting, and the full set of examples, see [Azure Blob Storage](azure-blob-storage.md).
