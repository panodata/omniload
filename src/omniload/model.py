from __future__ import annotations

import dataclasses
import datetime as dt
from enum import Enum

# https://dlthub.com/docs/dlt-ecosystem/file-formats/parquet#supported-destinations
PARQUET_SUPPORTED_DESTINATIONS = [
    "athena",
    "bigquery",
    "duckdb",
    "snowflake",
    "databricks",
    "synapse",
    "s3",
]

# Sources in this list will return JSON.
# This means they cannot be used universally like
# the Parquet loader is used for BigQuery.
JSON_RETURNING_SOURCES = [
    "notion",
]


class IncrementalStrategy(str, Enum):
    create_replace = "replace"
    append = "append"
    delete_insert = "delete+insert"
    merge = "merge"
    scd2 = "scd2"
    none = "none"


class LoaderFileFormat(str, Enum):
    jsonl = "jsonl"
    parquet = "parquet"
    insert_values = "insert_values"
    csv = "csv"


class SqlBackend(str, Enum):
    default = "default"
    sqlalchemy = "sqlalchemy"
    pyarrow = "pyarrow"
    connectorx = "connectorx"


class Progress(str, Enum):
    interactive = "interactive"
    log = "log"
    spinner = "spinner"


class SchemaNaming(str, Enum):
    default = "default"
    direct = "direct"


class SqlReflectionLevel(str, Enum):
    minimal = "minimal"
    full = "full"
    full_with_precision = "full_with_precision"


@dataclasses.dataclass
class LoadRequest:
    """Represent a data loading request"""

    _: dataclasses.KW_ONLY
    source_uri: str
    dest_uri: str
    source_table: str | None = None
    dest_table: str | None = None
    incremental_key: str | None = None
    # None means "not explicitly requested": run_ingest resolves the default
    # (replace for ordinary sources, append for the filesystem family) and uses
    # the explicit-ness to decide whether to honour a run-level write disposition
    # for filesystem sources.
    incremental_strategy: IncrementalStrategy | str | None = None
    filesystem_incremental: bool = False
    interval_start: dt.datetime | None = None
    interval_end: dt.datetime | None = None
    primary_key: list[str] | None = None
    partition_by: str | None = None
    cluster_by: str | None = None
    dry_run: bool = False
    full_refresh: bool = False
    progress: Progress | str = Progress.interactive
    sql_backend: SqlBackend | str = SqlBackend.default
    loader_file_format: LoaderFileFormat | str | None = None
    page_size: int = 50000
    loader_file_size: int = 100000
    schema_naming: SchemaNaming | str = SchemaNaming.default
    pipelines_dir: str | None = None
    extract_parallelism: int = 5
    sql_reflection_level: SqlReflectionLevel | str = SqlReflectionLevel.full
    sql_limit: int | None = None
    sql_exclude_columns: list[str] | None = None
    columns: list[str] | None = None
    yield_limit: int | None = None
    staging_bucket: str | None = None
    mask: list[str] | None = None
