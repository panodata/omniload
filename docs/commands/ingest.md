# `omniload ingest`

The `ingest` command is a core feature of the `omniload` tool, allowing users to transfer data from a source to a destination with optional support for incremental updates.

## Synopsis

The following example demonstrates how to use the `ingest` command to transfer data from a source to a destination.

```bash
omniload ingest \
   --source-uri '<your-source-uri-here>' \
   --source-table '<your-schema>.<your-table>' \
   --dest-uri '<your-destination-uri-here>'
```

## Required flags

- `--source-uri TEXT`: Required. Specifies the URI of the data source.
- `--dest-uri TEXT`: Required. Specifies the URI of the destination where data will be ingested.
- `--source-table TEXT`: Required. Defines the source table to fetch data from.

## Optional flags

- `--dest-table TEXT`: Designates the destination table to save the data. If not specified, defaults to the value of `--source-table`.
- `--incremental-key TEXT`: Identifies the key used for incremental data strategies. Defaults to `None`.
- `--incremental-strategy TEXT`: Defines the strategy for incremental updates. Options include `replace`, `append`, `delete+insert`, or `merge`. The default strategy is `replace`. Filesystem-family sources (`file://`, `s3://`, `gs://`, `az://`, `sftp://`, ...) manage incrementality themselves and ignore this flag. See [Filesystem sources](../getting-started/incremental-loading.md#filesystem-sources).
- `--interval-start`: Sets the start of the interval for the incremental key. Defaults to `None`.
- `--interval-end`: Sets the end of the interval for the incremental key. Defaults to `None`.
- `--primary-key TEXT`: Specifies the primary key for the merge operation. Defaults to `None`.
- `--columns <column_name>:<column_type>`: Specifies the columns to be ingested. Defaults to `None`.
- `--mask <column_name>:<algorithm>[:param]`: Applies data masking to specified columns. Can be used multiple times for different columns. See the [Data Masking](../getting-started/data-masking.md) documentation for available algorithms and usage examples. Defaults to `None`.
- `--schema-naming` Specifies what naming convention to use for table and column names on the destination. Can be `default` or `direct`.default is snake_case. `direct is case sensitive and doesn't contract underscores.
- `--full-refresh`: Ignores existing pipeline state and reloads the destination table from scratch (dlt's `refresh="drop_resources"`). Useful when a source appends on every run and you want a clean reload instead of another copy, as the filesystem family does by default. See [Filesystem sources](../getting-started/incremental-loading.md#filesystem-sources). Can also be set via the `FULL_REFRESH` / `OMNILOAD_FULL_REFRESH` environment variables. Defaults to `False`.

The `interval-start` and `interval-end` options support various datetime formats, here are some examples:
- `%Y-%m-%d`: `2023-01-31`
- `%Y-%m-%dT%H:%M:%S`: `2023-01-31T15:00:00`
- `%Y-%m-%dT%H:%M:%S%z`: `2023-01-31T15:00:00+00:00`
- `%Y-%m-%dT%H:%M:%S.%f`: `2023-01-31T15:00:00.000123`
- `%Y-%m-%dT%H:%M:%S.%f%z`: `2023-01-31T15:00:00.000123+00:00`

:::{note}
For the details around the incremental key and the various strategies, please refer to the [Incremental Loading](../getting-started/incremental-loading.md) section.
:::

## General flags

- `--help`: Displays the help message and exits the command.

## Example gallery

### Ingesting a CSV file to DuckDB

```bash
omniload ingest \
   --source-uri 'csv://input.csv' \
   --source-table 'sample' \
   --dest-uri 'duckdb://output.duckdb'
```

### Copy a table from Postgres to DuckDB

```bash
omniload ingest \
   --source-uri 'postgresql://myuser:mypassword@localhost:5432/mydatabase?sslmode=disable' \
   --source-table 'public.input_table' \
   --dest-uri 'duckdb://output.duckdb' \
   --dest-table 'public.output_table'
```

### Incrementally ingest a table from Postgres to BigQuery

```bash
omniload ingest 
   --source-uri 'postgresql://myuser:mypassword@localhost:5432/mydatabase?sslmode=disable' \
   --source-table 'public.users' \
   --dest-uri 'bigquery://my_project?credentials_path=/path/to/service/account.json&location=EU' \
   --dest-table 'raw.users' \
   --incremental-key 'updated_at' \
   --incremental-strategy 'delete+insert'
```

### Load an interval of data from Postgres to BigQuery using a date column

```bash
omniload ingest 
   --source-uri 'postgresql://myuser:mypassword@localhost:5432/mydatabase?sslmode=disable' \
   --source-table 'public.users' \
   --dest-uri 'bigquery://my_project?credentials_path=/path/to/service/account.json&location=EU' \
   --dest-table 'raw.users' \
   --incremental-key 'dt' \
   --incremental-strategy 'delete+insert' \
   --interval-start '2023-01-01' \
   --interval-end '2023-01-31' \
   --columns 'dt:date'
```

### Load a specific query from Postgres to Snowflake

```bash
omniload ingest 
   --source-uri 'postgresql://myuser:mypassword@localhost:5432/mydatabase?sslmode=disable' \
   --dest-uri 'snowflake://user:password@account/dbname?warehouse=COMPUTE_WH&role=my_role' \
   --source-table 'query:SELECT * FROM public.users as pu JOIN public.orders as o ON pu.id = o.user_id WHERE pu.dt BETWEEN :interval_start AND :interval_end' \
   --dest-table 'raw.users' \
   --incremental-key 'dt' \
   --incremental-strategy 'delete+insert' \
   --interval-start '2023-01-01' \
   --interval-end '2023-01-31' \
   --columns 'dt:date'
```

### Ingesting with Data Masking

```bash
omniload ingest \
   --source-uri 'postgresql://user:pass@localhost/customers' \
   --source-table 'customer_data' \
   --dest-uri 'duckdb:///masked_customers.db' \
   --dest-table 'masked_customers' \
   --mask 'email:hash' \
   --mask 'phone:partial:3' \
   --mask 'ssn:redact' \
   --mask 'salary:round:5000'
```

This example demonstrates masking sensitive customer data:
- Email addresses are hashed for consistent anonymization
- Phone numbers show only first and last 3 digits
- SSNs are completely redacted
- Salaries are rounded to nearest $5000

:::{note}
For more examples, please refer to the specific platforms' documentation on the sidebar.
:::
