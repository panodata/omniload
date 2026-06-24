"""Embeddable Python API for omniload.

`run_ingest` is the plain-callable core that the `omniload ingest` CLI command
delegates to. It performs the same source-to-destination data load, but instead
of printing CLI chrome and raising `typer` control-flow exceptions, it returns a
`LoadInfo` (or `None` for a dry run) and raises library exceptions
(`ValidationError`, `IngestJobError`).

Heavy imports (dlt and friends) stay inside `run_ingest` so importing this module
(which happens at package level, via `omniload.__init__`) stays cheap.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING

from omniload.error import IngestJobError, ValidationError

if TYPE_CHECKING:
    from dlt.common.pipeline import LoadInfo

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

# these sources would return a JSON for sure, which means they cannot be used with Parquet loader for BigQuery
JSON_RETURNING_SOURCES = ["notion"]


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


def _coerce(value, enum_cls):
    """Accept either an enum member or its string value (the CLI form)."""
    if value is None or isinstance(value, enum_cls):
        return value
    return enum_cls(value)


def run_ingest(
    *,
    source_uri: str,
    dest_uri: str,
    source_table: str,
    dest_table: str | None = None,
    incremental_key: str | None = None,
    incremental_strategy: IncrementalStrategy
    | str = IncrementalStrategy.create_replace,
    interval_start: datetime | None = None,
    interval_end: datetime | None = None,
    primary_key: list[str] | None = None,
    partition_by: str | None = None,
    cluster_by: str | None = None,
    dry_run: bool = False,
    full_refresh: bool = False,
    progress: Progress | str = Progress.interactive,
    sql_backend: SqlBackend | str = SqlBackend.default,
    loader_file_format: LoaderFileFormat | str | None = None,
    page_size: int = 50000,
    loader_file_size: int = 100000,
    schema_naming: SchemaNaming | str = SchemaNaming.default,
    pipelines_dir: str | None = None,
    extract_parallelism: int = 5,
    sql_reflection_level: SqlReflectionLevel | str = SqlReflectionLevel.full,
    sql_limit: int | None = None,
    sql_exclude_columns: list[str] | None = None,
    columns: list[str] | None = None,
    yield_limit: int | None = None,
    staging_bucket: str | None = None,
    mask: list[str] | None = None,
    quiet: bool = False,
) -> LoadInfo | None:
    """Load data from ``source_uri`` into ``dest_uri`` and return the dlt ``LoadInfo``.

    This is the embeddable core of the ``omniload ingest`` command. Defaults match
    the CLI option defaults so the CLI and the API behave the same. (The list
    options default to ``None`` rather than a shared mutable ``[]``, but are
    normalised identically.)

    Enum parameters accept either an enum member or its string value (the value the
    CLI accepts, e.g. ``"merge"``), coerced internally.

    Returns ``None`` when ``dry_run`` is true (nothing is loaded). Raises
    :class:`omniload.ValidationError` for invalid parameters and
    :class:`omniload.IngestJobError` when one or more load jobs fail.

    Set ``quiet=True`` to suppress the progress chrome printed to stdout when
    embedding omniload in another application.
    """
    import hashlib
    import tempfile
    import time
    from datetime import datetime
    from typing import Any, Dict, Optional

    import dlt
    import humanize
    from dlt.common.runtime.collector import Collector, LogCollector, TqdmCollector
    from dlt.common.schema.typing import TColumnSchema
    from dlt.pipeline.exceptions import PipelineStepFailed

    import omniload.core.resource as resource
    from omniload.codec import hint
    from omniload.codec.filter import (
        cast_set_to_list,
        cast_spanner_types,
        create_masking_filter,
        handle_mysql_empty_dates,
    )
    from omniload.core.factory import SourceDestinationFactory
    from omniload.source.mongodb.api import MongoDbSource
    from omniload.target.athena import AthenaDestination
    from omniload.target.clickhouse import ClickhouseDestination
    from omniload.util.spinner import SpinnerCollector

    incremental_strategy = _coerce(incremental_strategy, IncrementalStrategy)
    progress = _coerce(progress, Progress)
    sql_backend = _coerce(sql_backend, SqlBackend)
    loader_file_format = _coerce(loader_file_format, LoaderFileFormat)
    schema_naming = _coerce(schema_naming, SchemaNaming)
    sql_reflection_level = _coerce(sql_reflection_level, SqlReflectionLevel)

    def emit(message: str = "") -> None:
        if not quiet:
            print(message)

    def report_errors(run_info: LoadInfo):
        for load_package in run_info.load_packages:
            failed_jobs = load_package.jobs["failed_jobs"]
            if len(failed_jobs) == 0:
                continue

            emit()
            emit("[bold red]Failed jobs:[/bold red]")
            emit()
            for job in failed_jobs:
                emit(f"[bold red]  {job.job_file_info.job_id()}[/bold red]")
                emit(f"    [bold yellow]Error:[/bold yellow] {job.failed_message}")

            raise IngestJobError(failed_jobs)

    def validate_source_dest_tables(
        source_table: str, dest_table: str | None
    ) -> tuple[str, str]:
        if not dest_table:
            if len(source_table.split(".")) != 2:
                raise ValidationError(
                    "Table name must be in the format schema.table for source table when dest-table is not given."
                )

            emit()
            emit(
                "[yellow]Destination table is not given, defaulting to the source table.[/yellow]"
            )
            dest_table = source_table
        return (source_table, dest_table)

    def validate_loader_file_format(
        dlt_dest, loader_file_format: Optional[LoaderFileFormat]
    ):
        if (
            loader_file_format
            and loader_file_format.value
            not in dlt_dest.capabilities().supported_loader_file_formats
        ):
            raise ValidationError(
                f"Loader file format {loader_file_format.value} is not supported by the destination, available formats: {dlt_dest.capabilities().supported_loader_file_formats}."
            )

    def parse_columns(columns: list[str]) -> dict:
        from typing import cast, get_args

        from dlt.common.data_types import TDataType

        possible_types = get_args(TDataType)
        custom_types = ("bigdecimal",)

        types: dict[str, TDataType | str] = {}
        for column in columns:
            for candidate in column.split(","):
                column_name, column_type = candidate.split(":")
                if (
                    column_type not in possible_types
                    and column_type not in custom_types
                ):
                    raise ValidationError(
                        f"Column type '{column_type}' is not supported, supported types: {possible_types + custom_types}."
                    )
                types[column_name] = (
                    cast(TDataType, column_type)
                    if column_type in possible_types
                    else column_type
                )
        return types

    # The CLI used a mutable [] default; the SqlSource excluder expects a list,
    # not None, so normalise here before passing it downstream.
    sql_exclude_columns = sql_exclude_columns or []
    clean_sql_exclude_columns = []
    for col in sql_exclude_columns:
        for possible_col in col.split(","):
            clean_sql_exclude_columns.append(possible_col.strip())
    sql_exclude_columns = clean_sql_exclude_columns

    dlt.config["data_writer.buffer_max_items"] = page_size
    dlt.config["data_writer.file_max_items"] = loader_file_size
    dlt.config["extract.workers"] = extract_parallelism
    dlt.config["extract.max_parallel_items"] = extract_parallelism
    dlt.config["load.raise_on_max_retries"] = 15
    if schema_naming != SchemaNaming.default:
        dlt.config["schema.naming"] = schema_naming.value

    (source_table, dest_table) = validate_source_dest_tables(source_table, dest_table)

    factory = SourceDestinationFactory(source_uri, dest_uri)

    source = factory.get_source()
    destination = factory.get_destination()

    column_hints: dict[str, TColumnSchema] = {}
    original_incremental_strategy = incremental_strategy

    column_types = parse_columns(columns) if columns else None
    if column_types:
        for column_name, column_type in column_types.items():
            if column_type == "bigdecimal":
                column_hints[column_name] = {
                    "data_type": "decimal",
                    "precision": 76,
                    "scale": 38,
                }
            else:
                column_hints[column_name] = {"data_type": column_type}

    merge_key = None
    if incremental_strategy == IncrementalStrategy.delete_insert:
        merge_key = incremental_key
        incremental_strategy = IncrementalStrategy.merge
        if incremental_key:
            if incremental_key not in column_hints:
                column_hints[incremental_key] = {}

            column_hints[incremental_key]["merge_key"] = True

    m = hashlib.sha256()
    m.update(dest_table.encode("utf-8"))

    progressInstance: Collector = TqdmCollector()
    if progress == Progress.log:
        progressInstance = LogCollector()
    elif progress == Progress.spinner:
        progressInstance = SpinnerCollector()

    is_pipelines_dir_temp = False
    if pipelines_dir is None:
        pipelines_dir = tempfile.mkdtemp()
        is_pipelines_dir_temp = True

    dlt_dest = destination.dlt_dest(
        uri=dest_uri, dest_table=dest_table, staging_bucket=staging_bucket
    )
    validate_loader_file_format(dlt_dest, loader_file_format)

    if partition_by:
        if partition_by not in column_hints:
            column_hints[partition_by] = {}

        column_hints[partition_by]["partition"] = True

    if cluster_by:
        if cluster_by not in column_hints:
            column_hints[cluster_by] = {}

        column_hints[cluster_by]["cluster"] = True

    if primary_key:
        for key in primary_key:
            if key not in column_hints:
                column_hints[key] = {}

            column_hints[key]["primary_key"] = True

    pipeline = dlt.pipeline(
        pipeline_name=m.hexdigest(),
        destination=dlt_dest,
        progress=progressInstance,
        pipelines_dir=pipelines_dir,
        refresh="drop_resources" if full_refresh else None,  # ty: ignore[invalid-argument-type]
    )

    if source.handles_incrementality():
        incremental_strategy = IncrementalStrategy.none
        incremental_key = None

    incremental_strategy_text = (
        incremental_strategy.value
        if incremental_strategy.value != IncrementalStrategy.none
        else "Platform-specific"
    )

    source_table_print = source_table.split(":")[0]

    emit()
    emit("[bold green]Initiated the pipeline with the following:[/bold green]")
    emit(
        f"[bold yellow]  Source:[/bold yellow] {factory.source_scheme} / {source_table_print}"
    )
    emit(
        f"[bold yellow]  Destination:[/bold yellow] {factory.destination_scheme} / {dest_table}"
    )
    emit(
        f"[bold yellow]  Incremental Strategy:[/bold yellow] {incremental_strategy_text}"
    )
    emit(
        f"[bold yellow]  Incremental Key:[/bold yellow] {incremental_key if incremental_key else 'None'}"
    )
    emit(
        f"[bold yellow]  Primary Key:[/bold yellow] {primary_key if primary_key else 'None'}"
    )
    emit(f"[bold yellow]  Pipeline ID:[/bold yellow] {m.hexdigest()}")
    emit()

    if dry_run:
        emit("Skipping data transfer, because `--dry-run` was selected.")
        return None

    emit()
    emit("[bold green]Starting the ingestion...[/bold green]")

    if factory.source_scheme == "sqlite":
        source_table = "main." + source_table.split(".")[-1]

    if (
        incremental_key
        and incremental_key in column_hints
        and "data_type" in column_hints[incremental_key]
        and column_hints[incremental_key]["data_type"] == "date"
    ):
        # By default, omniload treats the start and end dates as datetime objects. While this worked fine for many cases, if the
        # incremental field is a date, the start and end dates cannot be compared to the incremental field, and the ingestion would fail.
        # In order to eliminate this, we have introduced a new option to omniload, --columns, which allows the user to specify the column types for the destination table.
        # This way, omniload will know the data type of the incremental field, and will be able to convert the start and end dates to the correct data type before running the ingestion.
        if interval_start:
            interval_start = interval_start.date()  # ty: ignore[invalid-assignment]
        if interval_end:
            interval_end = interval_end.date()  # ty: ignore[invalid-assignment]

    if factory.source_scheme.startswith("spanner"):
        # we tend to use the 'pyarrow' backend in general, however, it has issues with JSON objects, so we override it to 'sqlalchemy' for Spanner.
        if sql_backend.value == SqlBackend.default:
            sql_backend = SqlBackend.sqlalchemy

    # this allows us to identify the cases where the user does not have a preference, so that for some sources we can override it.
    if sql_backend == SqlBackend.default:
        sql_backend = SqlBackend.pyarrow

    dlt_source = source.dlt_source(
        uri=source_uri,
        table=source_table,
        incremental_key=incremental_key,
        merge_key=merge_key,
        interval_start=interval_start,
        interval_end=interval_end,
        sql_backend=sql_backend.value,
        page_size=page_size,
        sql_reflection_level=sql_reflection_level.value,
        sql_limit=sql_limit,
        sql_exclude_columns=sql_exclude_columns,
        extract_parallelism=extract_parallelism,
        column_types=column_types,
    )

    resource.for_each(dlt_source, lambda x: x.add_map(cast_set_to_list))
    if factory.source_scheme.startswith("mysql"):
        resource.for_each(dlt_source, lambda x: x.add_map(handle_mysql_empty_dates))

    if factory.source_scheme.startswith("spanner"):
        resource.for_each(dlt_source, lambda x: x.add_map(cast_spanner_types))

    if factory.source_scheme.startswith(
        "mmap"
    ) and factory.destination_scheme.startswith("clickhouse"):
        # https://github.com/dlt-hub/dlt/issues/2248
        # TODO(turtledev): only apply for write dispositions that actually cause an exception.
        # TODO(turtledev): make batch size configurable
        import omniload.source.arrow.adapter as arrow

        resource.for_each(dlt_source, lambda x: x.add_map(arrow.as_list))

    if mask:
        masking_filter = create_masking_filter(mask)
        resource.for_each(dlt_source, lambda x: x.add_map(masking_filter))

    if yield_limit:
        resource.for_each(dlt_source, lambda x: x.add_limit(yield_limit))

    if isinstance(source, MongoDbSource):
        from omniload.core.resource import TypeHintMap

        resource.for_each(dlt_source, lambda x: x.add_map(TypeHintMap().type_hint_map))

    def col_h(x):
        if column_hints:
            x.apply_hints(columns=column_hints)

    resource.for_each(dlt_source, col_h)

    if isinstance(destination, AthenaDestination) and partition_by:
        hint.apply_athena_hints(dlt_source, partition_by, column_hints)

    if isinstance(destination, ClickhouseDestination):
        from dlt.destinations.adapters import clickhouse_adapter

        settings = ClickhouseDestination.engine_settings(dest_uri)
        engine_type = ClickhouseDestination.engine_type(dest_uri)

        def apply_clickhouse_adapter(x):
            kwargs: Dict[str, Any] = {"settings": settings}
            if engine_type:
                kwargs["table_engine_type"] = engine_type
            clickhouse_adapter(x, **kwargs)

        resource.for_each(
            dlt_source,
            apply_clickhouse_adapter,
        )

    if original_incremental_strategy == IncrementalStrategy.delete_insert:

        def set_primary_key(x):
            x.incremental.primary_key = ()

        resource.for_each(dlt_source, set_primary_key)

    if (
        factory.destination_scheme in PARQUET_SUPPORTED_DESTINATIONS
        and loader_file_format is None
    ):
        loader_file_format = LoaderFileFormat.parquet

        # if the source is a JSON returning source, we cannot use Parquet loader for BigQuery
        if (
            factory.destination_scheme == "bigquery"
            and factory.source_scheme in JSON_RETURNING_SOURCES
        ):
            loader_file_format = None

    write_disposition = None
    if incremental_strategy != IncrementalStrategy.none:
        write_disposition = incremental_strategy.value

    if factory.source_scheme == "influxdb":
        if primary_key:
            write_disposition = "merge"

    start_time = datetime.now()

    def run_pipeline():
        return pipeline.run(
            dlt_source,
            **destination.dlt_run_params(
                uri=dest_uri,
                table=dest_table,
                staging_bucket=staging_bucket,
            ),
            write_disposition=write_disposition,
            primary_key=(primary_key if primary_key and len(primary_key) > 0 else None),
            loader_file_format=(
                loader_file_format.value if loader_file_format is not None else None
            ),
        )

    # Databricks concurrency error patterns that are safe to retry
    DATABRICKS_RETRYABLE_ERRORS = [
        "SCHEMA_ALREADY_EXISTS",
        "DELTA_METADATA_CHANGED",
        "MetadataChangedException",
    ]

    def is_databricks_retryable_error(exception: Exception) -> bool:
        if factory.destination_scheme != "databricks":
            return False
        error_str = str(exception)
        return any(pattern in error_str for pattern in DATABRICKS_RETRYABLE_ERRORS)

    max_retries = 3
    for attempt in range(max_retries):
        try:
            run_info: LoadInfo = run_pipeline()
            break
        except PipelineStepFailed as e:
            if is_databricks_retryable_error(e) and attempt < max_retries - 1:
                delay = (attempt + 1) * 2  # 2s, 4s backoff
                emit(
                    f"[yellow]Databricks concurrency error, retrying in {delay}s (attempt {attempt + 1}/{max_retries})...[/yellow]"
                )
                time.sleep(delay)
                continue
            raise

    report_errors(run_info)

    destination.post_load()

    end_time = datetime.now()
    elapsed = end_time - start_time
    elapsedHuman = f"in {humanize.precisedelta(elapsed)}"

    if is_pipelines_dir_temp:
        import shutil

        shutil.rmtree(pipelines_dir)

    emit(
        f"[bold green]Successfully finished loading data from '{factory.source_scheme}' to '{factory.destination_scheme}' {elapsedHuman} [/bold green]"
    )
    emit()

    return run_info
