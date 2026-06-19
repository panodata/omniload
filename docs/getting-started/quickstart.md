---
outline: deep
---

(quickstart)=

# Quickstart

omniload is a polyglot data loader framework based on dlt.
It allows you to load data from any source into any destination,
either using a concise CLI from your shell,
or the Python API from your own applications.

(installation)=

## Installation

We recommend using [uv] to install or run `omniload`.

```
pip install uv
uvx omniload
```

Alternatively, if you'd like to install it globally:
```
uv pip install --system omniload
```

While installation with vanilla `pip` is possible, it's an order of magnitude slower.

## Usage

The next command instructs omniload to read the table `public.some_data` from
the PostgreSQL instance, and to write the data to your BigQuery warehouse
under the schema `omniload` and table `some_data`.

```shell
omniload ingest \
    --source-uri 'postgresql://admin:admin@localhost:8837/web?sslmode=disable' \
    --source-table 'public.some_data' \
    --dest-uri 'bigquery://<your-project-name>?credentials_path=/path/to/service/account.json' \
    --dest-table 'omniload.some_data'
```

The next command instructs omniload to fetch the `profiles` table for the
requested chess players, and to write the data into the DuckDB database at
`./chess.duckdb` under `raw.profiles`.

```shell
omniload ingest \
    --source-uri 'chess://?players=awryaw,albertojgomez' \
    --source-table 'profiles' \
    --dest-uri 'duckdb:///./chess.duckdb' \
    --dest-table 'raw.profiles'
```

:::{note}
The steps here assume you have [DuckDB](https://duckdb.org/install/) installed.
DuckDB runs locally with zero setup and keeps the quickstart easy and fast.
:::

If you'd like a quick check, you can query the table directly:
```shell
duckdb ./chess.duckdb "select * from raw.profiles"
```

Or alternatively explore the table in the DuckDB UI:
```shell
duckdb -ui ./chess.duckdb
```

## Supported sources & destinations

See the Supported Sources & Destinations page for a list of all supported sources and destinations.


[uv]: https://docs.astral.sh/uv/
