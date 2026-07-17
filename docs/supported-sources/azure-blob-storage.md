(azure-blob-storage)=

# Azure Blob Storage

[Azure Blob Storage] is Microsoft's object storage service for the cloud, optimized for storing large amounts of unstructured data.

`omniload` supports Azure Blob Storage as both a data source and destination. The same backend also serves Azure Data Lake Storage Gen2; see [Azure Data Lake Storage](azure-data-lake-storage.md).

## URI Format

The URI for connecting to Azure Blob Storage is structured as follows:

```text
az://?account_name=<your_account_name>&account_key=<your_account_key>
```

**URI Parameters:**

*   `account_name`: Your Azure storage account name (required).
*   `account_key`: Your storage account access key (account-key auth).
*   `sas_token`: A Shared Access Signature token (SAS auth, alternative to `account_key`).
*   `tenant_id`, `client_id`, `client_secret`: Azure AD service-principal credentials (service-principal auth). All three are required together.
*   `account_host`: Custom storage endpoint host (optional, for sovereign clouds or Azurite).
*   `layout`: Layout template (optional, destination only).

Supply **one** authentication mode: an `account_key`, a `sas_token`, or the full service-principal triplet (`tenant_id` + `client_id` + `client_secret`). Supplying both an account key/SAS and service-principal fields is rejected as ambiguous, and a partial service-principal triplet reports the missing field.

::: warning
Account keys are base64 (containing `+`, `/`, `=`) and SAS tokens embed their own `&` and `=` characters. **URL-encode credential values** in the URI (`+` becomes `%2B`, `/` becomes `%2F`, `=` becomes `%3D`, `&` becomes `%26`). Unencoded values are mangled when the query string is parsed.
:::

The `--source-table` parameter specifies the container and file pattern using the following format:

```
<container-name>/<file-glob-pattern>
```

## Setting up an Azure Blob Storage Integration

To integrate `omniload` with Azure Blob Storage, you need a storage account and one of the supported credentials. For guidance on obtaining an account key or SAS token, refer to the Microsoft documentation on [managing storage account access keys](https://learn.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage) and [shared access signatures](https://learn.microsoft.com/en-us/azure/storage/common/storage-sas-overview). Service-principal credentials come from an [Azure AD app registration](https://learn.microsoft.com/en-us/azure/active-directory/develop/howto-create-service-principal-portal).

Once you have your credentials, you can configure the `az://` URI. The container name and file glob pattern are specified in the `--source-table` argument.

### Example: Loading data from Azure Blob Storage

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

### Example: Uploading data to Azure Blob Storage

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

The value of `load_id` and `file_id` is determined at runtime. The default layout creates a folder with the same table name as the source and places the data inside a parquet file. This layout is configurable using the `layout` parameter. See the [available layout placeholders](https://dlthub.com/docs/dlt-ecosystem/destinations/filesystem#available-layout-placeholders) for the full list.

### Authenticating with a SAS token

```sh
omniload ingest \
    --source-uri 'az://?account_name=mystorageacct&sas_token=sv%3D2023-01-03%26ss%3Db%26sig%3DaBcD1234%253D' \
    --source-table 'my-container/data.csv' \
    --dest-uri 'duckdb:///local.duckdb' \
    --dest-table 'public.my_data'
```

### Authenticating with a service principal

```sh
omniload ingest \
    --source-uri 'az://?account_name=mystorageacct&tenant_id=<tenant>&client_id=<client>&client_secret=<secret>' \
    --source-table 'my-container/data.csv' \
    --dest-uri 'duckdb:///local.duckdb' \
    --dest-table 'public.my_data'
```

### File Glob Pattern Examples:

::: warning
Glob patterns only apply when loading data from Azure Blob Storage as source.
:::

The `<file-glob-pattern>` in the `--source-table` argument allows for flexible file selection. Here are some common patterns and their descriptions:

| Pattern                                        | Description                                                                                                          |
| :--------------------------------------------- | :----------------------------------------------------------------------------------------------------------------- |
| `container/**/*.csv`                           | Retrieves all CSV files recursively from `az://container`.                                                          |
| `container/*.csv`                              | Retrieves all CSV files located at the root level of `az://container`.                                              |
| `container/myFolder/**/*.jsonl`                | Retrieves all JSONL files recursively from the `myFolder` directory and its subdirectories in `az://container`.     |
| `container/myFolder/mySubFolder/users.parquet` | Retrieves the specific `users.parquet` file from the `myFolder/mySubFolder/` path in `az://container`.              |
| `container/employees.jsonl`                    | Retrieves the `employees.jsonl` file located at the root level of `az://container`.                                 |

### Working with compressed files

`omniload` automatically detects and handles gzipped files in your container. You can load data from compressed files with the `.gz` extension without any additional configuration.

For example, to load data from a gzipped CSV file:

```sh
omniload ingest \
    --source-uri 'az://?account_name=mystorageacct&account_key=dGVzdA%3D%3D' \
    --source-table 'my-container/logs/event-data.csv.gz' \
    --dest-uri duckdb:///compressed_data.duckdb \
    --dest-table 'logs.events'
```

### File type hinting

If your files are properly encoded but lack the correct file extension (BSON, CSV, JSONL, or Parquet), you can provide a file type hint to inform `omniload` about the format of the files. This is done by appending a fragment identifier (`#format`) to the end of the path in your `--source-table` parameter.

For example, if you have JSONL-formatted log files stored in Azure with a non-standard extension:

```
--source-table "my-container/logs/event-data#jsonl"
```

Supported format hints include:
- `#bson` - For BSON (MongoDB dump) files. See [BSON](bson.md).
- `#csv` - For comma-separated values files with headers
- `#csv_headless` - For CSV files without headers
- `#jsonl` - For line-delimited JSON files
- `#parquet` - For Parquet format files

::: tip
File type hinting works with `gzip` compressed files as well.
:::

## Azure Data Lake Storage Gen2

The `adls://` and `abfss://` schemes are aliases for the same backend and accept identical parameters. Use them when you think in terms of ADLS Gen2. See [Azure Data Lake Storage](azure-data-lake-storage.md).


[Azure Blob Storage]: https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blobs-introduction
