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

import dataclasses
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from omniload.core.report import Stage
from omniload.core.tablename import split
from omniload.error import IngestJobError, ValidationError
from omniload.model import (
    JSON_RETURNING_SOURCES,
    PARQUET_SUPPORTED_DESTINATIONS,
    IncrementalStrategy,
    LoaderFileFormat,
    LoadRequest,
    NotifyOn,
    Progress,
    SchemaNaming,
    SqlBackend,
    SqlReflectionLevel,
)

if TYPE_CHECKING:
    from dlt.common.pipeline import LoadInfo

    from omniload.util.notify import Notifier

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class _RunContext:
    """How far one run got, for the job report: the wrapper alone cannot tell a setup
    failure from a `post_load()` one, and the `LoadInfo` stays local to `_run_ingest`."""

    stage: Stage = Stage.setup
    load_info: LoadInfo | None = None


def _mark(context: _RunContext | None, stage: Stage) -> None:
    if context is not None:
        context.stage = stage


#: Run-level keyword arguments `run_ingest` passes into every source's
#: ``dlt_source``. A source may optionally declare ``consumed_run_options()``
#: (see ``core.model.SourceProtocol``) to narrow this to the subset it actually
#: accepts; every name below is filtered against that declared subset before the
#: call, so an unfiltered source keeps receiving all fifteen unchanged. Kept in
#: step with the keywords the call site below actually passes by
#: ``tests/main/test_source_option_ownership.py::test_run_option_keys_match_the_api_call_site``.
RUN_OPTION_KEYS: frozenset[str] = frozenset(
    {
        "column_types",
        "data_item_format",
        "extract_parallelism",
        "filesystem_incremental",
        "incremental_key",
        "interval_end",
        "interval_start",
        "merge_key",
        "page_size",
        "requested_incremental_key",
        "requested_primary_key",
        "sql_backend",
        "sql_exclude_columns",
        "sql_limit",
        "sql_odbc_uri",
        "sql_reflection_level",
    }
)


#: Strategies that are a dlt merge strategy named on a `merge` disposition, rather than a
#: write disposition of their own.
NAMED_MERGE_STRATEGIES = (
    IncrementalStrategy.scd2,
    IncrementalStrategy.upsert,
    IncrementalStrategy.cdc,
)

#: Why each strategy that reads absence as deletion cannot take a partial read.
_ABSENCE_CONSEQUENCE = {
    IncrementalStrategy.scd2: (
        "scd2 spots a change by comparing the whole source table against the records "
        "it holds open, so a run that reads only part of the table retires every "
        "record it leaves out."
    ),
    IncrementalStrategy.cdc: (
        "cdc syncs the destination to the whole source table, so a run that reads "
        "only part of the table deletes every record it leaves out."
    ),
}


def _coerce(value, enum_cls):
    """Accept either an enum member or its string value (the CLI form)."""
    if value is None or isinstance(value, enum_cls):
        return value
    return enum_cls(value)


def _reject_unconsumed_incremental_key(
    source, scheme: str, requested_incremental_key
) -> None:
    """Raise if a source that owns its option vocabulary was asked for a key it
    does not consume.

    A source that declares ``consumed_run_options()`` has told us its full run
    vocabulary, so a requested row-level ``incremental_key`` reaching it is a
    user error *provided the source did not itself ask for one of the two
    incremental-key names* -- the hook's contract is "names I accept", not "I
    own incrementality", and a future source could legitimately declare one of
    them. Every current declarer happens not to, which is what lets this reuse
    one check for every filesystem-family source (and anything else that
    declares the hook): they used to carry it themselves, keyed on the nulled
    ``incremental_key`` for most of them (dead, since it is always ``None`` by
    the time ``dlt_source`` would see it) and on ``requested_incremental_key``
    for the rest (live). A source that does not declare the hook (Delta Lake,
    mq-bridge) keeps its own guard, unaffected.
    """
    consumed_run_options = getattr(source, "consumed_run_options", lambda: None)()
    if (
        consumed_run_options is not None
        and requested_incremental_key
        and {"incremental_key", "requested_incremental_key"}.isdisjoint(
            consumed_run_options
        )
    ):
        raise ValueError(
            f"{scheme} takes care of incrementality on its own, "
            "you should not provide incremental_key"
        )


def _is_merge(disposition) -> bool:
    """Return whether a static write disposition, string or dict, is a merge."""
    if isinstance(disposition, dict):
        return disposition.get("disposition") == "merge"
    return disposition == "merge"


def _first_merging_resource(dlt_source, run_disposition) -> str | None:
    """Return the name of the first selected resource that would load with merge.

    This is the disposition dlt applies, not the one omniload was asked for: a
    run-level disposition overrides every resource's hint (dlt applies it with
    ``apply_hints``), and without one each resource keeps its own, which sources
    that manage their incrementality set themselves. Merge is the string
    ``"merge"`` or a dict whose ``disposition`` is ``"merge"`` (the form a named
    merge strategy such as scd2 takes). A
    callable hint is resolved per data item, so nothing here can show it does
    not merge, and it counts as merging.
    """
    import omniload.core.resource as resource

    found: list[str] = []

    def check(res) -> None:
        disposition = run_disposition or getattr(res, "write_disposition", None)
        if callable(disposition) or _is_merge(disposition):
            found.append(getattr(res, "name", repr(res)))

    resource.for_each(dlt_source, check)
    return found[0] if found else None


def _require_primary_key(
    resource, strategy: IncrementalStrategy, source_table: str
) -> None:
    """Raise unless ``resource`` declares a primary key ``strategy`` can match rows on.

    A resource whose table hints depend on the data cannot be inspected before it is
    read, so it is left to dlt's own check at the load step.
    """
    from dlt.extract.exceptions import DataItemRequiredForDynamicTableHints

    try:
        columns = resource.compute_table_schema().get("columns", {})
    except DataItemRequiredForDynamicTableHints:
        return
    if not any(column.get("primary_key") for column in columns.values()):
        # A `query:` table's resource carries a placeholder name, not one the user wrote.
        table = (
            "the query"
            if source_table.startswith("query:")
            else f"the '{resource.name}' table"
        )
        raise ValidationError(
            f"Incremental strategy '{strategy.value}' needs a primary key to match "
            f"records on, and {table} declares none. Name its key columns with "
            "'--primary-key'."
        )


def run_ingest(**kwargs) -> LoadInfo | None:
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

    ``notify`` (apprise URLs) and ``notify_config`` (apprise config files or URLs)
    send one job report when the run ends, filtered by ``notify_on``. Both are read
    and checked before the load starts; a report that cannot be sent afterwards only
    logs a warning, and never changes the return value or the exception.
    """
    jr = LoadRequest(**kwargs)
    # Checked even without a target, so a typo fails now rather than once one is added.
    try:
        NotifyOn(jr.notify_on)
    except ValueError as exc:
        choices = ", ".join(member.value for member in NotifyOn)
        raise ValidationError(f"--notify-on must be one of: {choices}") from exc
    if not (jr.notify or jr.notify_config):
        return _run_ingest_in_pipelines_dir(jr)

    from omniload.util.notify import Notifier

    notifier = Notifier.prepare(jr)
    if notifier is None:
        return _run_ingest_in_pipelines_dir(jr)
    return _run_ingest_with_notification(jr, notifier)


def _run_ingest_with_notification(
    jr: LoadRequest, notifier: Notifier
) -> LoadInfo | None:
    """Run the load, then report its terminal outcome once, success or failure."""
    context = _RunContext()
    started_at = datetime.now(tz=timezone.utc)
    try:
        load_info = _run_ingest_in_pipelines_dir(jr, context)
    except Exception as exc:
        # The load's own cleanup (release, staging teardown) has already run.
        if not jr.dry_run:
            notifier.report(
                jr,
                started_at,
                exception=exc,
                stage=context.stage,
                load_info=context.load_info,
            )
        raise
    if not jr.dry_run:
        notifier.report(jr, started_at, load_info=load_info)
    return load_info


def _run_ingest_in_pipelines_dir(
    jr: LoadRequest, context: _RunContext | None = None
) -> LoadInfo | None:
    import tempfile

    if jr.pipelines_dir is not None:
        return _run_ingest_with_remote_database(
            jr, jr.pipelines_dir, is_pipelines_dir_temp=False, context=context
        )

    with tempfile.TemporaryDirectory() as pipelines_dir:
        return _run_ingest_with_remote_database(
            jr, pipelines_dir, is_pipelines_dir_temp=True, context=context
        )


def _refuse_run_options(jr: LoadRequest, *, is_pipelines_dir_temp: bool) -> None:
    """Raise the destination's refusal of a run option before a source is staged.

    A remote source is downloaded before the run is assembled, so a refusal left
    until then would cost that download for a run that cannot go ahead.
    """
    if is_pipelines_dir_temp and not jr.full_refresh:
        return
    from omniload.core.factory import SourceDestinationFactory

    destination = SourceDestinationFactory(jr.source_uri, jr.dest_uri).get_destination()
    if not is_pipelines_dir_temp:
        refusal = getattr(
            destination, "persistent_pipelines_dir_refusal", lambda uri: None
        )(jr.dest_uri)
        if refusal:
            raise ValidationError(refusal)
    if jr.full_refresh:
        refusal = getattr(destination, "full_refresh_refusal", lambda: None)()
        if refusal:
            raise ValidationError(refusal)


def _run_ingest_with_remote_database(
    jr: LoadRequest,
    pipelines_dir: str,
    *,
    is_pipelines_dir_temp: bool,
    context: _RunContext | None = None,
) -> LoadInfo | None:
    """Stage a remote SQLite/DuckDB source for the complete pipeline lifetime."""
    from pathlib import Path

    from omniload.source.sql_database.remote import (
        dry_run_database_uri,
        parse_remote_database_uri,
        stage_remote_database,
    )
    from omniload.util.turso import (
        RemoteTurso,
        is_turso_uri,
        parse_turso_uri,
        stage_turso_replica,
    )

    _refuse_run_options(jr, is_pipelines_dir_temp=is_pipelines_dir_temp)
    staging_root = jr.remote_database_staging_root or pipelines_dir
    if is_turso_uri(jr.source_uri):
        turso = parse_turso_uri(jr.source_uri)
        if not isinstance(turso, RemoteTurso):
            # A local Turso file is read in place through pyturso's SQLAlchemy
            # dialect, so the SQL source sees it as the `sqlite+turso` URI it is.
            jr = dataclasses.replace(jr, source_uri=turso.url)
        elif jr.dry_run:
            # Validate the SQL source the real run uses, without connecting.
            return _run_ingest(
                dataclasses.replace(
                    jr, source_uri=f"sqlite+turso:///{Path(staging_root, 'dry_run.db')}"
                ),
                pipelines_dir,
                is_pipelines_dir_temp=is_pipelines_dir_temp,
                context=context,
            )
        else:
            # A remote database is bootstrapped into a run-scoped replica, which is
            # then read as a local file. The token stays with the bootstrap.
            _mark(context, Stage.staging)
            with stage_turso_replica(turso, staging_root=staging_root) as replica_uri:
                return _run_ingest(
                    dataclasses.replace(jr, source_uri=replica_uri),
                    pipelines_dir,
                    is_pipelines_dir_temp=is_pipelines_dir_temp,
                    context=context,
                )

    remote_database = parse_remote_database_uri(jr.source_uri, jr.source_table or "")
    if remote_database is None:
        return _run_ingest(
            jr,
            pipelines_dir,
            is_pipelines_dir_temp=is_pipelines_dir_temp,
            context=context,
        )

    if jr.dry_run:
        # Validate the SQL source the real run uses, without downloading anything.
        return _run_ingest(
            dataclasses.replace(jr, source_uri=dry_run_database_uri(remote_database)),
            pipelines_dir,
            is_pipelines_dir_temp=is_pipelines_dir_temp,
            context=context,
        )

    _mark(context, Stage.staging)
    with stage_remote_database(
        remote_database, staging_root=staging_root
    ) as staged_source_uri:
        staged_request = dataclasses.replace(jr, source_uri=staged_source_uri)
        return _run_ingest(
            staged_request,
            pipelines_dir,
            is_pipelines_dir_temp=is_pipelines_dir_temp,
            context=context,
        )


def _run_ingest(
    jr: LoadRequest,
    pipelines_dir: str,
    *,
    is_pipelines_dir_temp: bool,
    context: _RunContext | None = None,
) -> LoadInfo | None:
    """Run the load, then close the destination on every exit once it exists.

    A destination may hold run-scoped resources from ``dlt_dest()`` on (a local
    replica of a remote database, say). It releases them in an optional
    ``close()``, which runs here whether the run loads, stops at ``--dry-run`` or
    fails, including inside ``dlt_dest()`` itself. Destinations without one have
    nothing to release.
    """
    destinations: list = []
    try:
        result = _assemble_and_run(
            jr,
            pipelines_dir,
            is_pipelines_dir_temp=is_pipelines_dir_temp,
            on_destination=destinations.append,
            context=context,
        )
    except BaseException:
        # The run's own error is the one to report; a close() failing on top of it
        # is logged rather than allowed to replace it.
        for destination in destinations:
            try:
                getattr(destination, "close", lambda: None)()
            except Exception:
                logger.exception("Closing the destination failed after a failed run")
        raise
    _mark(context, Stage.destination_close)
    for destination in destinations:
        getattr(destination, "close", lambda: None)()
    return result


def _assemble_and_run(
    jr: LoadRequest,
    pipelines_dir: str,
    *,
    is_pipelines_dir_temp: bool,
    on_destination,
    context: _RunContext | None = None,
) -> LoadInfo | None:
    _mark(context, Stage.setup)

    import hashlib
    import time
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
        cast_timedelta,
        create_masking_filter,
        handle_mysql_empty_dates,
    )
    from omniload.codec.masking import non_deterministic_masks
    from omniload.core.factory import SourceDestinationFactory
    from omniload.core.registry import SQL_SOURCE_SCHEMES
    from omniload.source.mongodb.api import MongoDbSource
    from omniload.target.athena import AthenaDestination
    from omniload.target.clickhouse import ClickhouseDestination
    from omniload.util.spinner import SpinnerCollector

    # `incremental_strategy` defaults to None ("not explicitly requested"). Capture the
    # explicit-ness before resolving the default: for the filesystem family it decides
    # whether a run-level write disposition is honoured (see the forcing block below).
    strategy_was_explicit = jr.incremental_strategy is not None
    incremental_strategy = _coerce(jr.incremental_strategy, IncrementalStrategy)
    progress = _coerce(jr.progress, Progress)
    sql_backend = _coerce(jr.sql_backend, SqlBackend)
    loader_file_format = _coerce(jr.loader_file_format, LoaderFileFormat)
    schema_naming = _coerce(jr.schema_naming, SchemaNaming)
    sql_reflection_level = _coerce(jr.sql_reflection_level, SqlReflectionLevel)
    dest_table_was_defaulted = jr.dest_table is None and jr.source_table is not None

    def report_errors(run_info: LoadInfo):
        for load_package in run_info.load_packages:
            failed_jobs = load_package.jobs["failed_jobs"]
            if len(failed_jobs) == 0:
                continue

            logger.error("Failed jobs:")
            for job in failed_jobs:
                logger.error(f"{job.job_file_info.job_id()}: {job.failed_message}")

            raise IngestJobError(failed_jobs)

    def validate_source_dest_tables(
        source_table: str | None, dest_table: str | None
    ) -> tuple[str, str]:
        if dest_table is None and source_table is not None:
            logger.info(
                "Destination table is not given, defaulting to the source table."
            )
            dest_table = source_table
        # TODO: Permit empty source and target table options to better support streaming
        #       elements and others which do not have a notion of a table at all.
        #       This might be dangerous for downstream decoders that expect
        #       source_table or dest_table to be non-empty. omniload needs to
        #       start keeping track of which dlt sources exactly need a table
        #       option and those which don't, so it can error out more appropriately.
        #       Currently, operating on strings, this defies type checking pretty much.
        #       omniload should introduce a data class that handles
        #       `(catalog, schema, table) | None` more properly.
        source_table = source_table or ""
        dest_table = dest_table or ""

        return (source_table, dest_table)

    def validate_loader_file_format(
        dlt_dest, loader_file_format: Optional[LoaderFileFormat]
    ):
        if not loader_file_format:
            return
        formats = dlt_dest.capabilities().supported_loader_file_formats
        if loader_file_format.value not in formats:
            cli_formats = sorted(
                fmt.value for fmt in LoaderFileFormat if fmt.value in formats
            )
            raise ValidationError(
                f"Loader file format {loader_file_format.value} is not supported by the destination. "
                + (
                    f"Supported loader file formats: {', '.join(cli_formats)}."
                    if cli_formats
                    else "It has no supported loader file format exposed by '--loader-file-format'."
                )
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
    sql_exclude_columns = jr.sql_exclude_columns or []
    clean_sql_exclude_columns = []
    for col in sql_exclude_columns:
        for possible_col in col.split(","):
            clean_sql_exclude_columns.append(possible_col.strip())
    sql_exclude_columns = clean_sql_exclude_columns

    dlt.config["data_writer.buffer_max_items"] = jr.page_size
    dlt.config["data_writer.file_max_items"] = jr.loader_file_size
    dlt.config["extract.workers"] = jr.extract_parallelism
    dlt.config["extract.max_parallel_items"] = jr.extract_parallelism
    dlt.config["load.raise_on_max_retries"] = 15
    if schema_naming != SchemaNaming.default:
        dlt.config["schema.naming"] = schema_naming.value

    (source_table, dest_table) = validate_source_dest_tables(
        jr.source_table, jr.dest_table
    )

    factory = SourceDestinationFactory(jr.source_uri, jr.dest_uri)

    source = factory.get_source()
    destination = factory.get_destination()
    on_destination(destination)

    produces_multiple_tables = getattr(
        source, "produces_multiple_tables", lambda uri, table: False
    )(jr.source_uri, source_table)
    if (
        produces_multiple_tables
        and not getattr(destination, "supports_multiple_tables", lambda: True)()
    ):
        raise ValidationError(
            f"The '{factory.destination_scheme}' destination cannot represent multiple "
            "worksheet tables. Select one worksheet with '#sheet_name=<name>' or "
            "'#sheet_id=<number>', or use a destination that supports datasets."
        )

    column_hints: dict[str, TColumnSchema] = {}
    original_incremental_strategy = incremental_strategy

    # cdc deletes every destination record a load leaves out, so it is only safe on a
    # read of the whole table. A SQL table reads whole unless an option windows or
    # truncates it, which the partial-read check below refuses. Other sources can keep a
    # cursor or a default window of their own that nothing here can see before the read,
    # so cdc is refused for them outright, ahead of the suppression that would otherwise
    # drop it to `none` for sources that own their disposition.
    if (
        original_incremental_strategy == IncrementalStrategy.cdc
        and factory.source_scheme not in SQL_SOURCE_SCHEMES
    ):
        raise ValidationError(
            "Incremental strategy 'cdc' is only supported for SQL database sources, "
            f"and '{factory.source_scheme}' is not one. cdc deletes every destination "
            "record a run does not read, so it needs a source omniload can read in "
            "full."
        )

    supports_filesystem_incremental = getattr(
        source, "supports_filesystem_incremental", lambda: False
    )()
    if jr.filesystem_incremental and not supports_filesystem_incremental:
        # Names the scheme rather than the family, because a filesystem-family
        # source can decline it too: file selection by modification time needs a
        # transport that reports one, and HTTP need not.
        raise ValidationError(
            f"The '{factory.source_scheme}' source does not support the "
            "'--filesystem-incremental' option, which selects files by their "
            "modification time."
        )
    if jr.filesystem_incremental and factory.destination_scheme in {"csv", "file"}:
        raise ValidationError(
            "The '--filesystem-incremental' option cannot write to the 'csv' or "
            "'file' destinations because those destinations replace a single output "
            "file using only the rows selected for the current run."
        )
    if jr.filesystem_incremental and original_incremental_strategy not in (
        None,
        IncrementalStrategy.append,
        IncrementalStrategy.none,
    ):
        raise ValidationError(
            "The '--filesystem-incremental' option requires append loading. "
            "Omit '--incremental-strategy' or set it to 'append'; use "
            "'--full-refresh' to reset the cursor and reload every matching file."
        )

    column_types = parse_columns(jr.columns) if jr.columns else None
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

    # `IncrementalStrategy` flattens dlt's two axes (a write disposition of
    # skip/append/replace/merge, and a merge strategy of delete-insert/scd2/upsert/cdc/
    # insert-only) onto one, so its merge-strategy values unflatten back onto dlt's shape
    # here: each is a `merge` disposition carrying a strategy. `delete+insert` marks the
    # incremental key as the merge key and relies on dlt's default strategy; `scd2`,
    # `upsert` and `cdc` name their strategy on the disposition itself, applied below once
    # the sources that own their own disposition have had their say.
    merge_key = None
    if incremental_strategy == IncrementalStrategy.delete_insert:
        merge_key = jr.incremental_key
        incremental_strategy = IncrementalStrategy.merge
        if jr.incremental_key:
            if jr.incremental_key not in column_hints:
                column_hints[jr.incremental_key] = {}

            column_hints[jr.incremental_key]["merge_key"] = True
    elif incremental_strategy in NAMED_MERGE_STRATEGIES:
        incremental_strategy = IncrementalStrategy.merge

    # TODO: What is this hash used for?
    #       Arrow test cases are failing if you modify it.
    m = hashlib.sha256()
    m.update(dest_table.encode("utf-8"))

    progressInstance: Collector = TqdmCollector()
    if progress == Progress.log:
        progressInstance = LogCollector()
    elif progress == Progress.spinner:
        progressInstance = SpinnerCollector()

    stage_under = getattr(destination, "set_remote_database_staging_root", None)
    if stage_under is not None:
        stage_under(jr.remote_database_staging_root)
    dlt_dest = destination.dlt_dest(
        uri=jr.dest_uri,
        dest_table=dest_table,
        staging_bucket=jr.staging_bucket,
    )
    if dest_table_was_defaulted:
        # A defaulted name carries the source's spelling into a destination that may
        # read its dots differently. A destination that splits on them would claim
        # components the source kept whole, so only a name it reads the same way
        # defaults through; anything longer needs an explicit destination table. A
        # destination whose name is opaque splits nothing and is unaffected.
        capability = getattr(destination, "table_capability", None)
        if capability is not None and capability.split_kind == "qualified":
            components = len(split(dest_table))
            if components > 2:
                raise ValidationError(
                    f"Cannot default the destination table to the source table "
                    f"{dest_table!r}: the '{factory.destination_scheme}' destination "
                    f"reads it as {components} components, which need not be the ones "
                    f"the source reads. Give --dest-table explicitly."
                )

        # Validate a defaulted destination before an eager SQL source reflects its
        # table. The destination now owns its name shape, including opaque names.
        destination.dlt_run_params(
            uri=jr.dest_uri,
            table=dest_table,
            staging_bucket=jr.staging_bucket,
        )

    if jr.partition_by:
        if jr.partition_by not in column_hints:
            column_hints[jr.partition_by] = {}

        column_hints[jr.partition_by]["partition"] = True

    if jr.cluster_by:
        if jr.cluster_by not in column_hints:
            column_hints[jr.cluster_by] = {}

        column_hints[jr.cluster_by]["cluster"] = True

    if jr.primary_key:
        for key in jr.primary_key:
            if key not in column_hints:
                column_hints[key] = {}

            column_hints[key]["primary_key"] = True

    pipeline = dlt.pipeline(
        pipeline_name=m.hexdigest(),
        destination=dlt_dest,
        progress=progressInstance,
        pipelines_dir=pipelines_dir,
        refresh="drop_resources" if jr.full_refresh else None,  # ty: ignore[invalid-argument-type]
    )
    validate_loader_file_format(pipeline.destination, loader_file_format)

    if jr.filesystem_incremental and is_pipelines_dir_temp:
        from dlt.common.destination.client import WithStateSync

        client_class = getattr(pipeline.destination, "client_class", None)
        supports_state_sync = isinstance(client_class, type) and issubclass(
            client_class, WithStateSync
        )
        if not supports_state_sync:
            raise ValidationError(
                "The destination cannot restore filesystem incremental state when "
                "omniload uses a temporary pipeline directory. Set '--pipelines-dir' "
                "to a persistent directory, or use a destination that supports dlt "
                "state sync."
            )

    # Capture the user's original request before it is nulled below: sources that manage
    # incrementality themselves (e.g. mq-bridge) need to see what was asked for so they can
    # reject conflicting flags, rather than silently ignoring them.
    requested_incremental_key = jr.incremental_key
    _reject_unconsumed_incremental_key(
        source, factory.source_scheme, requested_incremental_key
    )
    if source.handles_incrementality():
        jr.incremental_key = None
        # Filesystem-family and Delta Lake sources cannot derive a row-level incremental
        # or merge key. Unlike SaaS/streaming sources, a run-level disposition is safe to
        # apply to them: the filesystem family holds no resource-level one at all, and
        # Delta Lake's is a plain `replace` that an explicit request may override. Honour
        # an explicit --incremental-strategy append/replace for them; otherwise fall back
        # to `none` and let the source/platform own the write. `honours_run_disposition`
        # is absent on sources that set their own disposition, so getattr defaults to False.
        honours_disposition = getattr(
            source, "honours_run_disposition", lambda: False
        )()
        if strategy_was_explicit and honours_disposition:
            if original_incremental_strategy in (
                IncrementalStrategy.append,
                IncrementalStrategy.create_replace,
            ):
                # Keep the explicit strategy; write_disposition flows through below.
                pass
            elif original_incremental_strategy in (
                IncrementalStrategy.merge,
                IncrementalStrategy.delete_insert,
                *NAMED_MERGE_STRATEGIES,
            ):
                raise ValidationError(
                    f"Incremental strategy '{original_incremental_strategy.value}' needs an "
                    f"incremental or merge key, which the '{factory.source_scheme}' source "
                    "does not expose. Use 'append' or 'replace', or '--full-refresh' to "
                    "reset the destination."
                )
            else:  # explicit 'none'
                incremental_strategy = IncrementalStrategy.none
        else:
            incremental_strategy = IncrementalStrategy.none

    # Resolve the "not explicitly requested" default for every path that reaches the run.
    if incremental_strategy is None:
        incremental_strategy = IncrementalStrategy.create_replace

    # The strategy is final from here, so this is what actually reaches the load: a named
    # merge strategy as asked for, and not suppressed above by a source that owns its own
    # disposition.
    merge_strategy = (
        original_incremental_strategy
        if original_incremental_strategy in NAMED_MERGE_STRATEGIES
        and incremental_strategy != IncrementalStrategy.none
        else None
    )
    runs_scd2 = merge_strategy == IncrementalStrategy.scd2

    # scd2 and cdc both read a record missing from the load as gone from the source: scd2
    # retires it, cdc deletes it. Anything that hands dlt less than the whole source state
    # corrupts the destination rather than failing, so refuse it up front.
    if merge_strategy in _ABSENCE_CONSEQUENCE:
        # An incremental key windows the read, and either limit truncates it. An interval
        # windows it too where a `query:` table binds it into its `:interval_start` /
        # `:interval_end` placeholders, which needs no incremental key; on any other
        # table it takes effect only through the incremental key. Whatever the run leaves
        # out is absent from the batch.
        def binds(placeholder: str) -> bool:
            return source_table.startswith("query:") and placeholder in source_table

        partial_read = next(
            (
                option
                for option, value in (
                    ("--incremental-key", jr.incremental_key),
                    (
                        "--interval-start",
                        binds(":interval_start") and jr.interval_start,
                    ),
                    ("--interval-end", binds(":interval_end") and jr.interval_end),
                    ("--sql-limit", jr.sql_limit),
                    ("--yield-limit", jr.yield_limit),
                )
                if value
            ),
            None,
        )
        if partial_read:
            raise ValidationError(
                f"Incremental strategy '{merge_strategy.value}' cannot be combined with "
                f"'{partial_read}': {_ABSENCE_CONSEQUENCE[merge_strategy]}"
            )

    # scd2 also hashes each row to spot a change, which some inputs cannot carry.
    if runs_scd2:
        # A mask that draws a fresh value per run rewrites the rows scd2 compares, so an
        # untouched source row reads as a new version of itself on every load.
        unstable_masks = non_deterministic_masks(jr.mask) if jr.mask else []
        if unstable_masks:
            raise ValidationError(
                f"Incremental strategy 'scd2' cannot be combined with the mask "
                f"'{unstable_masks[0]}': its algorithm draws a fresh value on every run, so "
                "every record would read as changed and be retired and re-inserted on every "
                "load. Use a deterministic algorithm, such as 'sha256'."
            )
        # dlt hashes each row into `_dlt_id` to spot a change, and only does so for rows it
        # receives as dicts. Arrow data never gets that hash: the column stays empty and the
        # load dies in `normalize`, where scd2 marks it non-nullable. Filling it in
        # (`normalize.parquet_normalizer.add_dlt_id`) reads like the fix and is worse than
        # the crash: those ids are random per run rather than a hash of the row, so every
        # record would be retired and re-inserted on every load, silently.
        if factory.source_scheme == "mmap":
            raise ValidationError(
                "Incremental strategy 'scd2' cannot read from 'mmap://': it yields Arrow "
                "tables, for which dlt does not compute the row hash scd2 needs to spot a "
                "change."
            )
        # A `query:` table is read with sqlalchemy whatever the backend says (see
        # `SqlSourceRouter`), so it is row-oriented already and the backend is moot.
        is_custom_query = source_table.startswith("query:")
        if factory.source_scheme in SQL_SOURCE_SCHEMES and not is_custom_query:
            if sql_backend == SqlBackend.default:
                sql_backend = SqlBackend.sqlalchemy
            elif sql_backend in (
                SqlBackend.pyarrow,
                SqlBackend.connectorx,
                SqlBackend.adbcbridge,
            ):
                raise ValidationError(
                    f"Incremental strategy 'scd2' cannot use the '{sql_backend.value}' SQL "
                    "backend: it yields Arrow tables, for which dlt does not compute the "
                    "row hash scd2 needs to spot a change. Use '--sql-backend sqlalchemy'."
                )

    incremental_strategy_text = (
        incremental_strategy.value
        if incremental_strategy.value != IncrementalStrategy.none
        else "Platform-specific"
    )

    logger.info("Initiated the pipeline with the following:")
    logger.info("Source: %s / %s", factory.source_scheme, source_table)
    logger.info("Destination: %s / %s", factory.destination_scheme, dest_table)
    logger.info("Incremental Strategy: %s", incremental_strategy_text)
    logger.info(
        "Incremental Key: %s", jr.incremental_key if jr.incremental_key else "None"
    )
    logger.info(
        "Filesystem Incremental: %s",
        "Enabled" if jr.filesystem_incremental else "Disabled",
    )
    logger.info("Primary Key: %s", jr.primary_key if jr.primary_key else "None")
    logger.info("Pipeline ID: %s", m.hexdigest())

    # Resolve the reshape before the dry-run return, so a bad spec, a missing extra
    # or an engine the source cannot feed is reported by `--dry-run` rather than
    # waiting for a real run. A batch (polars) reshape consumes Arrow tables, so the
    # source has to be told to deliver Arrow; per-row reshapes do not care.
    reshape_mapper = None
    data_item_format = "object"
    if jr.reshape:
        from omniload.codec.reshape import create_reshape_mapper

        # A bad spec (unknown engine, empty recipes) or a missing extra is user/config
        # error: surface it as ValidationError so the CLI reports it instead of
        # tracebacking.
        try:
            reshape_mapper = create_reshape_mapper(jr.reshape)
        except (ValueError, ImportError) as exc:
            raise ValidationError(str(exc)) from exc
        if reshape_mapper.batch:
            if not isinstance(source, MongoDbSource):
                raise ValidationError(
                    "the batch (polars) reshape engine needs an Arrow-yielding "
                    "source; only the MongoDB source supports it currently"
                )
            data_item_format = "arrow"

    write_disposition: str | dict[str, str] | None = None
    if merge_strategy is not None:
        write_disposition = {"disposition": "merge", "strategy": merge_strategy.value}
    elif incremental_strategy != IncrementalStrategy.none:
        write_disposition = incremental_strategy.value

    # The influxdb source merges on its primary key, whatever the strategy says.
    strategy_merges = _is_merge(write_disposition)
    primary_key_merges = factory.source_scheme == "influxdb" and bool(jr.primary_key)
    if primary_key_merges:
        write_disposition = "merge"

    # dlt stores a table's merge strategy in its schema and leaves it in place when a
    # later run names none, so a table once loaded with `cdc` would go on deleting
    # unread rows under a plain `merge` or `delete+insert`. Name the strategy dlt would
    # default to, the destination's first, so the run loads with the one it asked for.
    # The pipeline's destination, not `dlt_dest`: a destination may hand dlt a factory
    # callable (MongoDB does), which only the pipeline resolves into one with capabilities.
    if write_disposition == "merge":
        strategies = pipeline.destination.capabilities().supported_merge_strategies
        if strategies:
            write_disposition = {"disposition": "merge", "strategy": strategies[0]}

    supports_merge = getattr(destination, "supports_merge", lambda: True)()
    if not supports_merge and _is_merge(write_disposition):
        # Known from the request alone, so `--dry-run` reports it too. Dispositions a
        # source sets on its own resources are checked once they exist, below.
        causes, fixes = [], []
        if strategy_merges:
            causes.append("the requested incremental strategy uses")
            fixes.append("use '--incremental-strategy append' or 'replace'")
        if primary_key_merges:
            causes.append(
                f"'--primary-key' selects for the '{factory.source_scheme}' source"
            )
            fixes.append("omit '--primary-key'")
        raise ValidationError(
            f"The '{factory.destination_scheme}' destination cannot load with a merge "
            f"write disposition, which {' and '.join(causes)}. To load, "
            f"{' and '.join(fixes)}."
        )

    # dlt checks a named merge strategy against the destination only at the load step,
    # and a destination with no merge strategy for the table is not refused at all:
    # dlt appends instead, or a custom sink merges its own way, so `cdc` would delete
    # nothing. Check upsert and cdc against what this table supports, up front.
    if merge_strategy in (IncrementalStrategy.upsert, IncrementalStrategy.cdc):
        capabilities = pipeline.destination.capabilities()
        strategies = list(capabilities.supported_merge_strategies or [])
        if strategies and capabilities.merge_strategies_selector:
            table_format = destination.dlt_run_params(
                uri=jr.dest_uri, table=dest_table, staging_bucket=jr.staging_bucket
            ).get("table_format")
            strategies = list(
                capabilities.merge_strategies_selector(
                    strategies, table_schema={"table_format": table_format}
                )
            )
        if merge_strategy.value not in strategies:
            cli_strategies = {
                strategy.value
                for strategy in NAMED_MERGE_STRATEGIES
                if strategy.value in strategies
            }
            # Plain merge and delete+insert both use the unfiltered default above.
            # Do not suggest one the table selector has removed, or name upsert as
            # delete+insert merely because the destination defaults to it.
            default_strategy = (capabilities.supported_merge_strategies or [None])[0]
            if default_strategy in strategies:
                cli_strategies.add(IncrementalStrategy.merge.value)
                if default_strategy == "delete-insert":
                    cli_strategies.add(IncrementalStrategy.delete_insert.value)
            raise ValidationError(
                f"The '{factory.destination_scheme}' destination does not support "
                f"incremental strategy '{merge_strategy.value}' for this table. "
                + (
                    f"Supported incremental strategies for this table: {', '.join(sorted(cli_strategies))}."
                    if cli_strategies
                    else (
                        "It has no merge strategy for it."
                        if not strategies
                        else "It has no supported incremental strategy exposed by '--incremental-strategy' for this table."
                    )
                )
            )

    if jr.dry_run:
        logger.info("Skipping data transfer, because `--dry-run` was selected.")
        return None

    logger.info("Starting the ingestion")

    if (
        jr.incremental_key
        and jr.incremental_key in column_hints
        and "data_type" in column_hints[jr.incremental_key]
        and column_hints[jr.incremental_key]["data_type"] == "date"
    ):
        # By default, omniload treats the start and end dates as datetime objects. While this worked fine for many cases, if the
        # incremental field is a date, the start and end dates cannot be compared to the incremental field, and the ingestion would fail.
        # In order to eliminate this, we have introduced a new option to omniload, --columns, which allows the user to specify the column types for the destination table.
        # This way, omniload will know the data type of the incremental field, and will be able to convert the start and end dates to the correct data type before running the ingestion.
        if jr.interval_start:
            jr.interval_start = jr.interval_start.date()  # ty: ignore[invalid-assignment]
        if jr.interval_end:
            jr.interval_end = jr.interval_end.date()  # ty: ignore[invalid-assignment]

    if factory.source_scheme.startswith("spanner"):
        # we tend to use the 'pyarrow' backend in general, however, it has issues with JSON objects, so we override it to 'sqlalchemy' for Spanner.
        if sql_backend.value == SqlBackend.default:
            sql_backend = SqlBackend.sqlalchemy

    # this allows us to identify the cases where the user does not have a preference, so that for some sources we can override it.
    if sql_backend == SqlBackend.default:
        sql_backend = SqlBackend.pyarrow

    run_options = {
        "incremental_key": jr.incremental_key,
        "requested_incremental_key": requested_incremental_key,
        "requested_primary_key": jr.primary_key,
        "merge_key": merge_key,
        "interval_start": jr.interval_start,
        "interval_end": jr.interval_end,
        "sql_backend": sql_backend.value,
        "page_size": jr.page_size,
        "sql_reflection_level": sql_reflection_level.value,
        "sql_limit": jr.sql_limit,
        "sql_exclude_columns": sql_exclude_columns,
        "sql_odbc_uri": jr.sql_odbc_uri,
        "extract_parallelism": jr.extract_parallelism,
        "column_types": column_types,
        "filesystem_incremental": jr.filesystem_incremental,
        "data_item_format": data_item_format,
    }
    # A source that declared `consumed_run_options()` gets only the subset it
    # named; the ~90 that never define the hook keep receiving everything, which
    # is the current behaviour preserved for them.
    consumed_run_options = getattr(source, "consumed_run_options", lambda: None)()
    if consumed_run_options is not None:
        run_options = {
            key: value
            for key, value in run_options.items()
            if key in consumed_run_options
        }

    dlt_source = source.dlt_source(
        uri=jr.source_uri,
        table=source_table,
        **run_options,
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

    if reshape_mapper is not None:
        # Both models go through add_yield_map: a batch reshape is N dicts out of one
        # Arrow table, and a per-row one has to be able to rowify an Arrow item too,
        # because add_map would hand it the whole table. See ReshapeMapper.
        reshape_fn = reshape_mapper.as_yield_map()
        resource.for_each(dlt_source, lambda x: x.add_yield_map(reshape_fn))

    if jr.mask:
        from omniload.codec.masking import MaskingEngine

        # One mapper per resource: a mapper remembers the type it gave each masked
        # column, which belongs to that resource's table. They share one engine,
        # so `sequential` and `uuid` give a value the same token in every table.
        masking_engine = MaskingEngine()
        resource.for_each(
            dlt_source,
            lambda x: x.add_map(create_masking_filter(jr.mask, masking_engine)),
        )

    # dlt can load neither a `timedelta` nor an Arrow `duration`, so intervals
    # reach it as text. This runs after masking, which hashes the original value.
    resource.for_each(dlt_source, lambda x: x.add_map(cast_timedelta))

    if jr.yield_limit:
        resource.for_each(dlt_source, lambda x: x.add_limit(jr.yield_limit))

    # TypeHintMap json-hints top-level Mongo arrays into JSON columns. A reshape owns the
    # schema instead (and needs the arrays un-hinted so they normalize into child tables),
    # so skip it when a reshape is active. See omniload.codec.reshape.
    if isinstance(source, MongoDbSource) and not jr.reshape:
        from omniload.core.resource import TypeHintMap

        resource.for_each(dlt_source, lambda x: x.add_map(TypeHintMap().type_hint_map))

    def col_h(x):
        if column_hints:
            x.apply_hints(columns=column_hints)

    resource.for_each(dlt_source, col_h)

    if (
        merge_strategy in (IncrementalStrategy.upsert, IncrementalStrategy.cdc)
        and not jr.primary_key
    ):
        # dlt refuses both without a primary key, but only at the load step and as a
        # traceback. A key given with `--primary-key` reaches dlt at `pipeline.run()`
        # rather than on the resource, so it satisfies this outright; otherwise the
        # resource must carry one, as a SQL table with a declared key does by reflection.
        resource.for_each(
            dlt_source,
            lambda x: _require_primary_key(x, merge_strategy, source_table),
        )

    if isinstance(destination, AthenaDestination) and jr.partition_by:
        hint.apply_athena_hints(dlt_source, jr.partition_by, column_hints)

    if isinstance(destination, ClickhouseDestination):
        from dlt.destinations.adapters import clickhouse_adapter

        settings = ClickhouseDestination.engine_settings(jr.dest_uri)
        engine_type = ClickhouseDestination.engine_type(jr.dest_uri)

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

    if not supports_merge:
        merging = _first_merging_resource(dlt_source, write_disposition)
        if merging is not None:
            # A source that owns its incrementality without honouring a run-level
            # disposition has had `--incremental-strategy` discarded above, so
            # suggesting one would not help.
            owns_disposition = (
                source.handles_incrementality()
                and not getattr(source, "honours_run_disposition", lambda: False)()
            )
            raise ValidationError(
                f"The '{factory.destination_scheme}' destination cannot load with a "
                f"merge write disposition, which resource '{merging}' would use. "
                + (
                    f"The '{factory.source_scheme}' source sets that disposition "
                    "itself and does not take '--incremental-strategy', so this "
                    "table cannot load into this destination."
                    if owns_disposition
                    else "Use '--incremental-strategy append' or 'replace'."
                )
            )

    start_time = datetime.now()

    def run_pipeline():
        run_params = destination.dlt_run_params(
            uri=jr.dest_uri,
            table=dest_table,
            staging_bucket=jr.staging_bucket,
        )
        if produces_multiple_tables:
            # Per-item worksheet metadata supplies each table name. Passing the
            # placeholder table from --dest-table at run level overrides that
            # metadata in dlt, so retain only the parsed dataset and other
            # destination-specific parameters.
            run_params.pop("table_name", None)
        return pipeline.run(
            dlt_source,
            **run_params,
            write_disposition=write_disposition,
            primary_key=(
                jr.primary_key if jr.primary_key and len(jr.primary_key) > 0 else None
            ),
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

    # Guard the load itself: mq-bridge opens a Consumer when the resource is first pulled
    # (inside run_pipeline) and acks its offsets only after the load durably commits. If the
    # load fails we must release the source (without acking) so the batches redeliver; ordinary
    # sources have no such hooks and the getattr calls are no-ops for them. Nothing touches the
    # broker before this point, so the transform registration above need not be guarded.
    _mark(context, Stage.load)
    try:
        max_retries = 3
        for attempt in range(max_retries):
            try:
                run_info: LoadInfo = run_pipeline()
                break
            except PipelineStepFailed as e:
                if is_databricks_retryable_error(e) and attempt < max_retries - 1:
                    delay = (attempt + 1) * 2  # 2s, 4s backoff
                    logger.warning(
                        f"Databricks concurrency error, retrying in {delay}s (attempt {attempt + 1}/{max_retries})"
                    )
                    time.sleep(delay)
                    continue
                raise

        if context is not None:
            context.load_info = run_info
        report_errors(run_info)

        _mark(context, Stage.destination_post_load)
        destination.post_load()

        # Let the source commit, now that the load is durably committed (e.g. mq-bridge ack).
        _mark(context, Stage.source_post_load)
        getattr(source, "post_load", lambda: None)()
    except BaseException:
        # Load failed: release the source without committing, so it redelivers.
        release = getattr(source, "release", None)
        if callable(release):
            release()
        raise

    end_time = datetime.now()
    elapsed = end_time - start_time
    elapsedHuman = f"in {humanize.precisedelta(elapsed)}"

    logger.info(
        f"Successfully finished loading data from '{factory.source_scheme}' to '{factory.destination_scheme}' {elapsedHuman}"
    )

    return run_info
