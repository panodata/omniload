(azure-storage)=

# Azure Storage

[Azure Blob Storage] is Microsoft's object storage service for the cloud,
optimized for storing large amounts of unstructured data. Resources are
addressed using the `az://` URL scheme.

[Azure Data Lake Storage Gen2] is a set of capabilities built on Azure Blob
Storage for big-data analytics. A Gen2 account is a storage account with a
hierarchical namespace enabled; it shares the same underlying service and
API as Azure Blob Storage, and uses the `adls://` and `abfss://` URL schemes.

`omniload` supports both Azure Blob Storage and Azure Data Lake Storage as
both data source and destination.

Reading goes through [`pyarrow.fs.AzureFileSystem`], which serves flat-namespace
and hierarchical-namespace (Gen2) accounts from one client, detects which it is
talking to, and lists a glob in a single request. Writing goes through dlt's
filesystem destination, which builds its own [adlfs] client.

## URI format

The URI for connecting to Azure Blob Storage is structured as follows.
```text
az://?account_name=<your_account_name>&account_key=<your_account_key>
```

The URIs for connecting to Azure Data Lake Storage are structured as follows.
```text
adls://?account_name=<your_account_name>&account_key=<your_account_key>
```
`abfss://` is accepted as well and behaves identically; it is the dominant
Gen2 scheme in the Spark, Databricks, and Synapse ecosystems.
```text
abfss://?account_name=<your_account_name>&account_key=<your_account_key>
```

## URI parameters

:account_name:
  Your Azure storage account name (required for account-key, SAS-token, and
  service-principal authentication). Do not supply it with `connection_string`.

:account_key:
  Your storage account access key (account-key auth).

:sas_token:
  A Shared Access Signature token (SAS auth), alternatively to `account_key`.
  Supply it with or without a leading `?`; both are accepted.

:tenant_id, client_id, client_secret:
  Azure AD service-principal credentials (service-principal auth). All three are required together.

:account_host:
  Custom storage endpoint (optional, for sovereign clouds or Azurite), as
  `hostname[:port]` with an optional `http://` or `https://` prefix. It must
  name the storage account, either as the leading label of the host
  (`myaccount.blob.core.chinacloudapi.cn`) or as the last path segment
  (`http://127.0.0.1:10000/myaccount`); anything else is rejected, because the
  reader addresses an account rather than a URL. A host carrying a `.blob.` or
  `.dfs.` label also yields its sibling service endpoint, so a Gen2 account
  behind a custom endpoint stays reachable. Do not supply it with
  `connection_string`, which carries its own endpoints.

:connection_string:
  A complete Azure Storage connection string, as an alternative to the
  account-key, SAS-token, or service-principal fields. URL-encode the whole
  value before placing it in the URI. Connection strings can include custom
  service endpoints for local emulators such as Azurite. The Azure Storage SDK
  parses account-key and SAS forms, explicit `BlobEndpoint` values,
  `DefaultEndpointsProtocol` with `EndpointSuffix`, and
  `UseDevelopmentStorage=true`. Field names are case-insensitive, a trailing
  semicolon is accepted, and malformed or duplicate fields are rejected. A
  `DfsEndpoint` is honoured as given; without one, the Data Lake endpoint is
  derived from the Blob endpoint.

:api_version:
  Not accepted on a source URI. The reader's Azure client pins the API version
  its own SDK ships with and offers no override, so the parameter is rejected
  with a named error rather than accepted and ignored. Destination URIs
  continue to ignore it, and it still reaches adlfs on the
  {ref}`database-files` staging path.

:layout:
  Layout template (optional, destination only).

Supply **one** authentication mode: a `connection_string`, an `account_key`, a
`sas_token`, or the full service-principal triplet (`tenant_id` + `client_id` +
`client_secret`). Supplying fields from multiple modes is rejected as
ambiguous, and a partial service-principal triplet reports the missing field.
When using `connection_string`, omit `account_name`, `account_host`, and all
other credential fields.

A source connection string has to name its account. The reader takes the
storage account as the root of the filesystem, so a string that identifies the
account by endpoint alone (a bare `SharedAccessSignature` plus a custom
`BlobEndpoint` with no `AccountName`) is rejected with a named error; add
`AccountName` to the string, or use `account_name` with `account_key` or
`sas_token`.

:::{warning}
Account keys are base64 (containing `+`, `/`, `=`) and SAS tokens embed their
own `&` and `=` characters. You must URL-encode credential values in the URI
(`+` becomes `%2B`, `/` becomes `%2F`, `=` becomes `%3D`, `&` becomes `%26`).
Unencoded values are mangled when the query string is parsed.
:::

:::{warning}
The release that introduces account-scoped connection-string identities resets
existing `--filesystem-incremental` cursors for connection-string sources. The
first run after upgrading reads the selected files again. With the `append`
strategy this can duplicate rows. If duplicates are unacceptable, use
`--full-refresh` for that first upgraded load to drop the destination resource
and cursor before reloading. This one-time reset prevents different storage
accounts with the same container and glob from silently sharing a cursor.
:::

For example, write to an Azurite container by URL-encoding its connection
string and passing it as the destination URI.

```sh
omniload ingest \
    --source-uri 'file:///local/users.csv' \
    --source-table 'users' \
    --dest-uri 'az://?connection_string=<url_encoded_connection_string>' \
    --dest-table 'my-container/users'
```

:::{note}
A Gen2 account differs from a plain Blob account only by having its
hierarchical namespace enabled, so no separate configuration is required.
:::

:::{note}
The reader's Azure client sends the API version its bundled SDK was built
against. Emulators lag the service, so [Azurite] rejects a newer version by
default; start it with `--skipApiVersionCheck` to read from it.
:::

:::{note}
Building an Azure source in Python rather than through a URI, connection
arguments beyond the ones above are passed to `pyarrow.fs.AzureFileSystem`
directly, so they have to be its keywords. An adlfs-only argument is rejected by
name rather than ignored.
:::

## Set up an Azure Storage integration

To integrate `omniload` with Azure Blob or Data Lake Storage, you need a
storage account and one of the supported credentials. For guidance on
obtaining an account key or SAS token, refer to the Microsoft documentation
on [managing storage account access keys] and [shared access signatures].
Service-principal credentials come from an [Azure AD app registration].

## Authenticate with SAS token

```sh
omniload ingest \
    --source-uri 'az://?account_name=mystorageacct&sas_token=sv%3D2023-01-03%26ss%3Db%26sig%3DaBcD1234%253D' \
    --source-table 'my-container/data.csv' \
    --dest-uri 'duckdb:///local.duckdb' \
    --dest-table 'public.my_data'
```

## Authenticate with service principal

```sh
omniload ingest \
    --source-uri 'az://?account_name=mystorageacct&tenant_id=<tenant>&client_id=<client>&client_secret=<secret>' \
    --source-table 'my-container/data.csv' \
    --dest-uri 'duckdb:///local.duckdb' \
    --dest-table 'public.my_data'
```

## File glob patterns

The `--source-table` parameter specifies the Azure Storage container name
and the file glob pattern using the following format.

```text
<container-name>/<file-glob-pattern>
```

The `<file-glob-pattern>` allows for flexible file selection. Here are some
common patterns and their descriptions.

| Pattern                                        | Description                                                                                                          |
| :--------------------------------------------- | :----------------------------------------------------------------------------------------------------------------- |
| `container/**/*.csv`                           | Retrieves all CSV files recursively from `az://container`.                                                          |
| `container/*.csv`                              | Retrieves all CSV files located at the root level of `az://container`.                                              |
| `container/myFolder/**/*.jsonl`                | Retrieves all JSONL files recursively from the `myFolder` directory and its subdirectories in `az://container`.     |
| `container/myFolder/mySubFolder/users.parquet` | Retrieves the specific `users.parquet` file from the `myFolder/mySubFolder/` path in `az://container`.              |
| `container/employees.jsonl`                    | Retrieves the `employees.jsonl` file located at the root level of `az://container`.                                 |

## File type hinting

If your files are properly encoded but lack the correct file extension,
you can provide a file type hint to inform `omniload`
about the format of the files. This is done by appending a fragment identifier
(`#format`) to the end of the path in your `--source-table` parameter.

For example, if you have JSONL-formatted log files stored in Azure with a
non-standard extension, use an URI pattern like this.

```text
--source-table "my-container/logs/event-data#jsonl"
```

See also the full list of {ref}`file-formats` and their type hints.

:::{tip}
File type hinting works with `gzip` compressed files as well.
:::

## Compressed files

`omniload` automatically detects and handles gzipped files in your container. You can load data from compressed files with the `.gz` extension without any additional configuration.

For example, to load data from a gzipped CSV file:

```sh
omniload ingest \
    --source-uri 'az://?account_name=mystorageacct&account_key=dGVzdA%3D%3D' \
    --source-table 'my-container/logs/event-data.csv.gz' \
    --dest-uri duckdb:///compressed_data.duckdb \
    --dest-table 'logs.events'
```

## Examples

### Load data from Azure Blob Storage

Let's assume the following details:
*   `account_name`: `mystorageacct`
*   `account_key`: `dGVzdA==`
*   Container name: `my-container`
*   Path to files within the container: `path/to/data.csv`

The following command demonstrates how to copy data from the specified Azure location to a DuckDB database (the account key is URL-encoded, so `==` becomes `%3D%3D`):

```sh
omniload ingest \
    --source-uri   'az://?account_name=mystorageacct&account_key=dGVzdA%3D%3D' \
    --source-table 'my-container/path/to/data.csv' \
    --dest-uri     'duckdb:///demo.duckdb' \
    --dest-table   'testdrive.data'
```

Running the command creates a table named `data` within the `testdrive`
schema in the DuckDB database file located at `demo.duckdb`.

### Upload data to Azure Blob Storage

For this example, we'll assume that:
* `records.db` is a duckdb database.
* It has a table called `public.users`.
* The Azure credentials are the same as the example above.

The following command demonstrates how to copy data from a local duckdb database to Azure Blob Storage:

```sh
omniload ingest \
    --source-uri 'duckdb:///records.db' \
    --source-table 'public.users' \
    --dest-uri 'az://?account_name=mystorageacct&account_key=dGVzdA%3D%3D' \
    --dest-table 'my-container/records'
```

This will result in a file structure like the following:
```text
my-container/
└── records
    ├── _dlt_loads
    ├── _dlt_pipeline_state
    ├── _dlt_version
    └── users
        └── <load_id>.<file_id>.parquet
```

The value of `load_id` and `file_id` is determined at runtime. The default
layout creates a folder with the same table name as the source and places
the data inside a parquet file. This layout is configurable using the
`layout` parameter. See the [available layout placeholders] for the full list.

### Load data from ADLS Gen2

```sh
omniload ingest \
    --source-uri 'adls://?account_name=mystorageacct&account_key=dGVzdA%3D%3D' \
    --source-table 'my-filesystem/events/2024/*.parquet' \
    --dest-uri duckdb:///adls_data.duckdb \
    --dest-table 'analytics.events'
```

### Upload data to ADLS Gen2

```sh
omniload ingest \
    --source-uri 'duckdb:///records.db' \
    --source-table 'public.users' \
    --dest-uri 'abfss://?account_name=mystorageacct&account_key=dGVzdA%3D%3D' \
    --dest-table 'my-filesystem/users'
```


[adlfs]: https://github.com/fsspec/adlfs
[available layout placeholders]: https://dlthub.com/docs/dlt-ecosystem/destinations/filesystem#available-layout-placeholders
[Azure AD app registration]: https://learn.microsoft.com/en-us/azure/active-directory/develop/howto-create-service-principal-portal
[Azure Blob Storage]: https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blobs-introduction
[Azure Data Lake Storage Gen2]: https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction
[`pyarrow.fs.AzureFileSystem`]: https://arrow.apache.org/docs/python/generated/pyarrow.fs.AzureFileSystem.html
[managing storage account access keys]: https://learn.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage
[shared access signatures]: https://learn.microsoft.com/en-us/azure/storage/common/storage-sas-overview
[Azurite]: https://github.com/Azure/Azurite
