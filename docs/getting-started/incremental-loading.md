---
outline: deep
---

(incremental-loading)=

# Incremental loading
omniload supports incremental loading, which means you can choose to append, merge or delete+insert data into the destination table. Incremental loading allows you to ingest only the new rows from the source table into the destination table, which means that you don't have to ingest the entire table every time you run omniload.

Before you use incremental loading, you should understand three important settings:
- `primary_key`: the column or columns that uniquely identify a row in the table, if you give a primary key for an ingestion the resulting rows will be deduplicated based on the primary key, which means there will only be one row for each primary key in the destination.
- `incremental_key`: the column that will be used to determine the new rows, if you give an incremental key for an ingestion the resulting rows will be filtered based on the incremental key, which means only the new rows will be ingested.
  - A good example of an incremental key is a timestamp column, where you only want to ingest the rows that are newer than the last ingestion, for example `created_at` or `updated_at`.
- `strategy`: the strategy to use for incremental loading, the available strategies are:
  - `replace`: replace the existing destination table with the source directly, this is the default strategy and the simplest one.
    - This strategy isn't recommended for large tables, as it will replace the entire table and can be slow.
  - `append`: simply append the new rows to the destination table.
  - `merge`: merge the new rows with the existing rows in the destination table, insert the new ones and update the existing ones with the new values.
  - `delete+insert`: delete the existing rows in the destination table that match the incremental key and then insert the new rows.
  - `scd2`: retire the existing row and open a new one when a row changes, keeping the history of both.



## Replace
Replace is the default strategy, and it simply replaces the entire destination table with the source table.

The following example below will replace the entire `my_schema.some_data` table in BigQuery with the `my_schema.some_data` table in Postgres.
```bash
omniload ingest \
    --source-uri 'postgresql://admin:admin@localhost:8837/web?sslmode=disable' \
    --source-table 'my_schema.some_data' \
    --dest-uri 'bigquery://<your-project-name>?credentials_path=/path/to/service/account.json'
```

Here's how the replace strategy works:
- The source table is downloaded.
- The source table is uploaded to the destination, replacing the destination table.

:::{caution}
This strategy will delete the entire destination table and replace it with the source table, use with caution.
:::

## Append
Append will simply append the new rows from the source table to the destination table. By default, it will append all the rows. You should provide an `incremental_key` to use it as an incremental strategy.

The following example below will append the new rows from the `my_schema.some_data` table in Postgres to the `my_schema.some_data` table in BigQuery, only where there's a new table.
```bash
omniload ingest \
    --source-uri 'postgresql://admin:admin@localhost:8837/web?sslmode=disable' \
    --source-table 'my_schema.some_data' \
    --dest-uri 'bigquery://<your-project-name>?credentials_path=/path/to/service/account.json' \
    --incremental-strategy append \
    --incremental-key updated_at
```

### Example

Let's assume you had the following source table:

| id | name | updated_at |
|----|------|------------|
| 1  | John | 2021-01-01 |
| 2  | Jane | 2021-01-01 |

#### First Ingestion
The first time you run the command, it will ingest all the rows into the destination table. Here's how your destination looks like now:

| id | name | updated_at |
|----|------|------------|
| 1  | John | 2021-01-01 |
| 2  | Jane | 2021-01-01 |

#### Second Ingestion, no new data
When there's no new data in the source table, the destination table will remain the same.

#### Third Ingestion, new data
Let's say John changed his name to Johnny, e.g. your source:

| id | name   | updated_at |
|----|--------|------------|
| 1  | Johnny | 2021-01-02 |
| 2  | Jane   | 2021-01-01 |


When you run the command again, it will only ingest the new rows into the destination table. Here's how your destination looks like now:

| id | name   | updated_at |
|----|--------|------------|
| 1  | John   | 2021-01-01 |
| 2  | Jane   | 2021-01-01 |
| 1  | Johnny | 2021-01-02 |

**Notice the last row in the table:** it's the new row that was ingested from the source table.

The behavior is the same if there were new rows in the source table, they would be appended to the destination table if they have `updated_at` that is **later than the latest record** in the destination table.

:::{tip}
The `append` strategy allows you to keep a version history of your data, as it will keep appending the new rows to the destination table. You can use it to build [Slowly Changing Dimensions (SCD) Type 2](https://en.wikipedia.org/wiki/Slowly_changing_dimension#Type_2:_add_new_row) tables, for example.
:::

## Merge
Merge will merge the new rows with the existing rows in the destination table, insert the new ones and update the existing ones with the new values. By default, it will merge all the rows. If you'd like to use it as an incremental strategy, you should provide an `incremental_key` as well as a `primary_key` to find the right rows to update.

The following example below will merge the new rows from the `my_schema.some_data` table in Postgres to the `my_schema.some_data` table in BigQuery, only where there's a new table.
```bash
omniload ingest \
    --source-uri 'postgresql://admin:admin@localhost:8837/web?sslmode=disable' \
    --source-table 'my_schema.some_data' \
    --dest-uri 'bigquery://<your-project-name>?credentials_path=/path/to/service/account.json' \
    --incremental-strategy merge \
    --incremental-key updated_at \
    --primary-key id
```

Here's how the merge strategy works:
- If the row with the `primary_key` exists in the destination table, it will be updated with the new values from the source table.
- If the row with the `primary_key` doesn't exist in the destination table, it will be inserted into the destination table.
- If the row with the `primary_key` exists in the destination table but not in the source table, it will remain in the destination table.
- If the row with the `primary_key` doesn't exist in the destination table but exists in the source table, it will be inserted into the destination table.

### Example

Let's assume you had the following source table:

| id | name | updated_at |
|----|------|------------|
| 1  | John | 2021-01-01 |
| 2  | Jane | 2021-01-01 |

#### First Ingestion
The first time you run the command, it will ingest all the rows into the destination table. Here's how your destination looks like now:

| id | name | updated_at |
|----|------|------------|
| 1  | John | 2021-01-01 |
| 2  | Jane | 2021-01-01 |

#### Second Ingestion, no new data
When there's no new data in the source table, the destination table will remain the same.

#### Third Ingestion, new data
Let's say John changed his name to Johnny, e.g. your source:

| id | name   | updated_at |
|----|--------|------------|
| 1  | Johnny | 2021-01-02 |
| 2  | Jane   | 2021-01-01 |
    
When you run the command again, it will merge the new rows into the destination table. Here's how your destination looks like now:

| id | name   | updated_at |
|----|--------|------------|
| 1  | Johnny | 2021-01-02 |
| 2  | Jane   | 2021-01-01 |

**Notice the first row in the table:** it's the updated row that was ingested from the source table.

The behavior is the same if there were new rows in the source table, they would be merged into the destination table if they have `updated_at` that is **later than the latest record** in the destination table.

:::{tip}
The `merge` strategy is different from the `append` strategy, as it will update the existing rows in the destination table with the new values from the source table. It's useful when you want to keep the latest version of your data in the destination table.
:::

:::{caution}
For the cases where there's a primary key match, the `merge` strategy will **update** the existing rows in the destination table with the new values from the source table. Use with caution, as it can lead to data loss if not used properly, as well as data processing charges if your data warehouse charges for updates.
:::

## Delete+Insert
Delete+Insert will delete the existing rows in the destination table that match the `incremental_key` and then insert the new rows from the source table. By default, it will delete and insert all the rows. If you'd like to use it as an incremental strategy, you should provide an `incremental_key`.

The following example below will delete the existing rows in the `my_schema.some_data` table in BigQuery that match the `updated_at` and then insert the new rows from the `my_schema.some_data` table in Postgres.
```bash
omniload ingest \
    --source-uri 'postgresql://admin:admin@localhost:8837/web?sslmode=disable' \
    --source-table 'my_schema.some_data' \
    --dest-uri 'bigquery://<your-project-name>?credentials_path=/path/to/service/account.json' \
    --incremental-strategy delete+insert \
    --incremental-key updated_at
```

Here's how the delete+insert strategy works:
- The new rows from the source table will be inserted into a staging table in the destination database.
- The existing rows in the destination table that match the `incremental_key` will be deleted.
- The new rows from the staging table will be inserted into the destination table.

A few important notes about the `delete+insert` strategy: 
- it does not guarantee the order of the rows in the destination table, as it will delete and insert the rows in the destination table.
- it does not deduplicate the rows in the destination table, as it will delete and insert the rows in the destination table, which means you may have multiple rows with the same `incremental_key` in the destination table.

### Example
Let's assume you had the following source table:

| id | name | updated_at |
|----|------|------------|
| 1  | John | 2021-01-01 |
| 2  | Jane | 2021-01-01 |

#### First Ingestion
The first time you run the command, it will ingest all the rows into the destination table. Here's how your destination looks like now:

| id | name | updated_at |
|----|------|------------|
| 1  | John | 2021-01-01 |
| 2  | Jane | 2021-01-01 |

#### Second Ingestion, no new data
Even when there's no new data in the source table, the rows from the source table will be inserted into a staging table in the destination database, and then the existing rows in the destination table that match the `incremental_key` will be deleted, and then the new rows from the staging table will be inserted into the destination table. The destination table will remain the same for the case of this example.
:::{caution}
If you had rows in the destination table that does not exist in the source table, they will be deleted from the destination table.
:::

#### Third Ingestion, new data
Let's say John changed his name to Johnny, e.g. your source:

| id | name   | updated_at |
|----|--------|------------|
| 1  | Johnny | 2021-01-02 |
| 2  | Jane   | 2021-01-01 |

When you run the command again, it will delete the existing rows in the destination table that match the `incremental_key` and then insert the new rows from the source table. Here's how your destination looks like now:

| id | name   | updated_at |
|----|--------|------------|
| 1  | Johnny | 2021-01-02 |
| 2  | Jane   | 2021-01-01 |

**Notice the first row in the table:** it's the updated row that was ingested from the source table.

The behavior is the same if there were new rows in the source table, they would be deleted and inserted into the destination table if they have `updated_at` that is **later than the latest record** in the destination table.

:::{tip}
The `delete+insert` strategy is useful when you want to keep the destination table clean, as it will delete the existing rows in the destination table that match the `incremental_key` and then insert the new rows from the source table. `delete+insert` strategy also allows you to backfill the data, e.g. going back to a past date and ingesting the data again.
:::

## SCD2

SCD2 (slowly changing dimension type 2) keeps the history of every row rather than overwriting it. When a row changes in the source, the existing record is retired and a new one is opened alongside it, so the destination records what the data looked like at any point in time. Each run stamps two extra columns, `_dlt_valid_from` and `_dlt_valid_to`. The record that is currently active is the one with an empty `_dlt_valid_to`.

SCD2 spots a change by comparing the whole source table against the records it is holding open, and retires whatever it no longer finds there. Every run must therefore read the table in full, so `scd2` rejects `--incremental-key`, `--sql-limit`, and `--yield-limit`: each reads back part of the table, which SCD2 cannot tell apart from the rest of the rows having been deleted. The filesystem family rejects `scd2` altogether ([Filesystem sources](#filesystem-sources)).

Where the source table is an expression rather than a plain table name, such as a `query:` for a SQL source, that expression's result is the table SCD2 tracks and carries the same requirement: it must return the tracked rows in full on every run. An expression whose result varies for any reason other than the data changing, such as one carrying its own limit, retires the rows it omits.

The following example applies the `scd2` strategy to the `my_schema.some_data` table in Postgres, loading it into BigQuery.

```bash
omniload ingest \
    --source-uri 'postgresql://admin:admin@localhost:8837/web?sslmode=disable' \
    --source-table 'my_schema.some_data' \
    --dest-uri 'bigquery://<your-project-name>?credentials_path=/path/to/service/account.json' \
    --incremental-strategy scd2
```

### Example

Let's assume you had the following source table:

| id | name |
|----|------|
| 1  | John |
| 2  | Jane |

#### First Ingestion
The first time you run the command, every row is ingested and opened as an active record:

| id | name | _dlt_valid_from | _dlt_valid_to |
|----|------|-----------------|---------------|
| 1  | John | 2021-01-01      |               |
| 2  | Jane | 2021-01-01      |               |

#### Second Ingestion, no new data
When no row has changed, the destination table remains the same. Unchanged records are left alone rather than being retired and reopened.

#### Third Ingestion, new data
Let's say John changed his name to Johnny, e.g. your source:

| id | name   |
|----|--------|
| 1  | Johnny |
| 2  | Jane   |

When you run the command again, the John record is retired and the Johnny record is opened:

| id | name   | _dlt_valid_from | _dlt_valid_to |
|----|--------|-----------------|---------------|
| 1  | John   | 2021-01-01      | 2021-01-03    |
| 2  | Jane   | 2021-01-01      |               |
| 1  | Johnny | 2021-01-03      |               |

**Notice the first row in the table:** it is kept, closed off at the moment the change was seen. Querying `where _dlt_valid_to is null` gives you the current state of the source.

:::{tip}
Pass `--primary-key` when you want the natural key of the entity marked on the destination table. It is optional: it does not affect which changes SCD2 detects.
:::

:::{caution}
SCD2 compares the row values it receives, so anything that rewrites a value differently on each run reads as a change. The `--mask` algorithms that draw a fresh value per run (`date_shift`, `noise`, `random`, `uuid`) are rejected with `scd2` for that reason. `sequential` is accepted, but numbers rows in the order they arrive, so it holds only for as long as that order does. Prefer a deterministic algorithm, such as `sha256`, `md5`, or `hmac`.
:::

:::{note}
SCD2 compares rows by a hash that dlt computes only for row-oriented data, so it does not work on data read as Arrow tables. For SCD2, SQL sources are read with the `sqlalchemy` backend, which omniload selects for you. Passing `--sql-backend pyarrow` or `--sql-backend connectorx` alongside `scd2` is rejected, as is `scd2` on an `mmap://` source, which is Arrow whichever backend you name.
:::

(incremental-loading-filesystem)=

## Filesystem sources

The filesystem-family sources manage their own incrementality, so omniload's per-key strategies above do not apply to them; they append by default and support an explicit `append` or `replace`. This covers the local source and every remote transport:

- `file://` (local files)
- `az://`, `abfss://`, `adls://` (Azure Blob Storage / ADLS Gen2)
- `gs://` (Google Cloud Storage)
- `s3://` (Amazon S3)
- `sftp://` (SFTP)

After the URI is parsed they all converge on the same reader, so their loading behaviour is identical regardless of transport.

### Default: append on re-run

Each of these sources declares (via `handles_incrementality()`) that omniload should not apply its own incremental logic. omniload responds by disabling that logic and leaving the write disposition unset, so dlt's default applies: **every run appends the source rows to the destination table.** Running the same command a second time adds another copy of the data rather than replacing it.

omniload's per-key incremental logic stays disabled, so `--incremental-key` does not drive loading: the local `file://` source rejects an explicit key with an error, while the remote transports ignore it. `--incremental-strategy` is honoured only for the two dispositions these sources can support, described next.

### Opt in to file-level incremental loading

Pass `--filesystem-incremental` to append rows only from files that have not
already been loaded. omniload lists matching files without reading their content,
filters that metadata using each file's modification time, and opens only the
files that survive the filter.

```bash
omniload ingest \
    --source-uri 's3://?access_key_id=...&secret_access_key=...' \
    --source-table 'my-bucket/events/*.jsonl' \
    --dest-uri 'duckdb://output.duckdb' \
    --dest-table 'events' \
    --filesystem-incremental
```

The mode is opt-in, so omitting the flag keeps the append-on-re-run behaviour
described above. It is append-only: omit `--incremental-strategy` or set it to
`append`. Combining it with `replace`, `delete+insert`, `merge`, or `scd2` is
rejected before extraction. Python callers set `filesystem_incremental=True` on
`run_ingest()`.

The cursor is stored in dlt pipeline state. With omniload's default temporary
pipeline directory, the destination must support dlt state sync so the next run
can restore that cursor from the destination. omniload checks this before
extraction and fails with a validation error when state cannot be restored. For
such a destination, pass a stable `--pipelines-dir` path and reuse it on every
run.

The single-file `csv://` and `file://` destinations are not supported. They
rebuild their output from the current run instead of retaining previously loaded
rows.

Cursor state is isolated by a hash of the storage namespace, bucket or local
directory, and file glob. Authentication values are excluded, so rotating a key
or password does not reset the cursor. Endpoints and accounts that distinguish
storage namespaces are included, such as an S3-compatible endpoint, Azure
account, or SFTP host, port, and username.

The modification-time boundary is closed. Files newer than the last maximum are
loaded. Files at exactly that maximum are compared using the lister's `file_url`
primary key, which lets a new file sharing the same timestamp load without
reopening files already recorded at the boundary. A newly added file with an
older modification time is not visible to the cursor. Likewise, modifying a file
without advancing its modification time leaves it unchanged from the cursor's
perspective. Use `--full-refresh` to reset the cursor and reload every matching
file when importing a backfill or working around coarse mtime precision.

### Choosing append or replace explicitly

Pass `--incremental-strategy` to make the write disposition explicit instead of relying on the append default:

- `append` keeps the default behaviour (each run adds the source rows), now as a stated, logged choice.
- `replace` resets the destination table on every run, so each run produces a clean replica rather than another copy. This is the flag-driven equivalent of `--full-refresh` for the common "re-import an updated file" case. It cannot be combined with `--filesystem-incremental`, because replacing a table with only the newly selected files would discard rows loaded earlier.

```bash
omniload ingest \
    --source-uri 'file:///data/people.csv' \
    --dest-uri 'duckdb://output.duckdb' \
    --dest-table 'people' \
    --incremental-strategy replace
```

The key-dependent strategies (`delete+insert`, `merge`, `scd2`) are rejected with an error, because filesystem sources expose no incremental or merge key for them to use. Use `append` or `replace` instead.

### Resetting the destination with `--full-refresh`

To reload from scratch instead of appending, pass `--full-refresh`:

```bash
omniload ingest \
    --source-uri 's3://my-bucket/data.csv?access_key_id=...&secret_access_key=...' \
    --source-table 'data.csv' \
    --dest-uri 'duckdb://output.duckdb' \
    --dest-table 'data' \
    --full-refresh
```

`--full-refresh` maps to dlt's `refresh="drop_resources"`: it drops the resource's tables and pipeline state, then reloads. With `--filesystem-incremental`, this is the supported way to reset the modification-time cursor and load an older backfill. Without that mode, `--incremental-strategy replace` also resets the destination through a `replace` write disposition. The `FULL_REFRESH` and `OMNILOAD_FULL_REFRESH` environment variables set the same option.

:::{tip}
For a one-shot import where you re-run the command to pick up an updated file, add `--full-refresh` so each run produces a clean replica instead of appending another copy.
:::

### Why append is uniform across transports

A local path is usually a single mutable file you re-import, whereas an `s3://` or `gs://` prefix is often an append-only collection of objects, so a case can be made for local defaulting to replace. omniload deliberately keeps the default uniform. Local and remote converge on the same reader after URI parsing, and a transport-dependent default would mean the same `--dest-table` mutates differently depending only on the scheme. If append is the wrong default for a one-shot local import, it is equally wrong for a single remote object, so the behaviour should change for the whole family or not at all.

## Conclusion
Incremental loading is a powerful feature that allows you to ingest only the new rows from the source table into the destination table. It's useful when you want to keep the destination table up-to-date with the source table, as well as when you want to keep a version history of your data in the destination table. However, there are a few things to keep in mind when using incremental loading:

- If you can and your data is not huge, use the `replace` strategy, as it's the simplest strategy and it will replace the entire destination table with the source table, which will always give you a clean exact replica of the source table.
- If you want to keep a version history of your data, use the `append` strategy, as it will keep appending the new rows to the destination table, which will give you a version history of your data.
- If you want to keep the latest version of your data in the destination table and your table has a natural primary key, such as a user ID, use the `merge` strategy, as it will update the existing rows in the destination table with the new values from the source table.
- If you want to keep the destination table clean and you want to backfill the data, use the `delete+insert` strategy, as it will delete the existing rows in the destination table that match the `incremental_key` and then insert the new rows from the source table.
- If you want to know what a row looked like at a past point in time, and not just its latest version, use the `scd2` strategy, as it retires the old record and opens a new one instead of overwriting it.

:::{tip}
Even though the document tries to explain, there's no better learning than trying it yourself. You can use the [Quickstart](/getting-started/quickstart.md) to try the incremental loading strategies yourself.
:::
