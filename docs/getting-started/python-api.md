---
outline: deep
---

(python-api)=

# Python API

Alongside the {ref}`CLI <quickstart>`, omniload exposes a Python entry point,
`run_ingest`, so you can run the same ingestion from your own application instead
of shelling out. The `omniload ingest` command is a thin wrapper over this
function, so the CLI and the API share their behaviour and defaults.

```python
from omniload import run_ingest

info = run_ingest(
    source_uri="sqlite:///./source.db",
    dest_uri="duckdb:///./warehouse.duckdb",
    source_table="main.some_table",
    dest_table="public.some_table",
)
print(info)  # a dlt LoadInfo describing the load
```

`run_ingest` returns the dlt [`LoadInfo`] for the run, or `None` when
`dry_run=True`. The keyword arguments map
one-to-one to the `omniload ingest` command-line options (`--source-uri` becomes
`source_uri`, and so on), and the defaults are identical.

## A complete example

The following script creates a small SQLite source, loads it into a local DuckDB
file, and reads the rows back.

```python
import sqlite3

from omniload import run_ingest

# Build a tiny SQLite source.
conn = sqlite3.connect("source.db")
conn.execute("CREATE TABLE some_table (id INTEGER, name TEXT)")
conn.executemany(
    "INSERT INTO some_table VALUES (?, ?)",
    [(1, "alice"), (2, "bob"), (3, "carol")],
)
conn.commit()
conn.close()

# Copy it into DuckDB.
run_ingest(
    source_uri="sqlite:///./source.db",
    dest_uri="duckdb:///./warehouse.duckdb",
    source_table="main.some_table",
    dest_table="public.some_table",
)
```

```shell
duckdb ./warehouse.duckdb "select * from public.some_table"
```

:::{note}
A DuckDB catalog is named after the database file (here, `warehouse`), so keep
the file name distinct from the destination schema (`public`) to avoid an
ambiguous-reference error.
:::

## Enums as strings

Parameters that the CLI exposes as a fixed set of choices, such as the
incremental strategy or the SQL backend, accept either the corresponding enum
member or its string value. Library callers can pass the plain string the CLI
uses and skip importing the enums:

```python
from omniload import run_ingest

run_ingest(
    source_uri="postgresql://admin:admin@localhost:5432/web",
    dest_uri="duckdb:///./warehouse.duckdb",
    source_table="public.events",
    dest_table="public.events",
    incremental_strategy="merge",  # or IncrementalStrategy.merge (from omniload import IncrementalStrategy)
    primary_key=["id"],
)
```

See {ref}`Incremental loading <incremental-loading>` for what the strategies do.

## Output, dry runs, and errors

- **`dry_run=True`** prints the planned transfer and returns `None` without
  loading anything.
- Instead of exiting the process the way the CLI does, the API raises exceptions
  you can catch:
  - `omniload.ValidationError` for invalid parameters (a malformed table
    specifier, an unsupported loader file format or column type).
  - `omniload.IngestJobError` when one or more load jobs fail; it carries the
    failed jobs on its `failed_jobs` attribute.

```python
from omniload import IngestJobError, ValidationError, run_ingest

try:
    run_ingest(
        source_uri="sqlite:///./source.db",
        dest_uri="duckdb:///./warehouse.duckdb",
        source_table="some_table",  # not schema.table, and no dest_table
    )
except ValidationError as exc:
    print(f"bad request: {exc}")
except IngestJobError as exc:
    print(f"{len(exc.failed_jobs)} job(s) failed")
```

## Pinning

omniload is pre-1.0, so pin the version when you depend on `run_ingest` as a
library:

```shell
pip install 'omniload[full]==0.0.42'
```


[`LoadInfo`]: https://dlthub.com/docs/walkthroughs/run-a-pipeline#4-inspect-a-load-process
