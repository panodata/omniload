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
  Your Azure storage account name (required).

:account_key:
  Your storage account access key (account-key auth).

:sas_token:
  A Shared Access Signature token (SAS auth), alternatively to `account_key`.

:tenant_id, client_id, client_secret:
  Azure AD service-principal credentials (service-principal auth). All three are required together.

:account_host:
  Custom storage endpoint host (optional, for sovereign clouds or Azurite).

:layout:
  Layout template (optional, destination only).

Supply **one** authentication mode: an `account_key`, a `sas_token`, or the
full service-principal triplet (`tenant_id` + `client_id` + `client_secret`).
Supplying both an account key/SAS and service-principal fields is rejected as
ambiguous, and a partial service-principal triplet reports the missing field.

:::{warning}
Account keys are base64 (containing `+`, `/`, `=`) and SAS tokens embed their
own `&` and `=` characters. You must URL-encode credential values in the URI
(`+` becomes `%2B`, `/` becomes `%2F`, `=` becomes `%3D`, `&` becomes `%26`).
Unencoded values are mangled when the query string is parsed.
:::

:::{note}
A Gen2 account differs from a plain Blob account only by having its
hierarchical namespace enabled, so no separate configuration is required.
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

If your files are properly encoded but lack the correct file extension (BSON,
CSV, JSONL, or Parquet), you can provide a file type hint to inform `omniload`
about the format of the files. This is done by appending a fragment identifier
(`#format`) to the end of the path in your `--source-table` parameter.

For example, if you have JSONL-formatted log files stored in Azure with a
non-standard extension, use an URI pattern like this.

```
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
*   Path to files within the container: `students/students_details.csv`

The following command demonstrates how to copy data from the specified Azure location to a DuckDB database (the account key is URL-encoded, so `==` becomes `%3D%3D`):

```sh
omniload ingest \
    --source-uri 'az://?account_name=mystorageacct&account_key=dGVzdA%3D%3D' \
    --source-table 'my-container/students/students_details.csv' \
    --dest-uri duckdb:///azure_data.duckdb \
    --dest-table 'processed_students.student_details'
```

This command will create a table named `student_details` within the `processed_students` schema (or equivalent grouping) in the DuckDB database file located at `azure_data.duckdb`.

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
```
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


[available layout placeholders]: https://dlthub.com/docs/dlt-ecosystem/destinations/filesystem#available-layout-placeholders
[Azure AD app registration]: https://learn.microsoft.com/en-us/azure/active-directory/develop/howto-create-service-principal-portal
[Azure Blob Storage]: https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blobs-introduction
[Azure Data Lake Storage Gen2]: https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction
[managing storage account access keys]: https://learn.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage
[shared access signatures]: https://learn.microsoft.com/en-us/azure/storage/common/storage-sas-overview
