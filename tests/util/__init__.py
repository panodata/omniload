import os
import subprocess
import sys
import traceback

from typer.testing import CliRunner

from omniload.main import app

# Sentinel for a required-but-positionally-late argument. `dest_uri` must stay
# effectively required (omitting it is a caller bug), but `source_table` sits
# before it in the public positional order and needs a default so it can be
# omitted. A plain `=None` default on `dest_uri` would silently accept a missing
# destination; the sentinel keeps it required while dodging the "non-default
# argument follows default argument" SyntaxError.
_MISSING = object()


def build_ingest_args(
    source_uri,
    dest_uri,
    source_table=None,
    dest_table=None,
    inc_strategy=None,
    inc_key=None,
    primary_key=None,
    merge_key=None,
    interval_start=None,
    interval_end=None,
    sql_backend=None,
    loader_file_format=None,
    sql_exclude_columns=None,
    columns=None,
    sql_limit=None,
    yield_limit=None,
    mask=None,
):
    """Build the ``ingest`` CLI argument list. Pure: no I/O, no CliRunner.

    ``--source-table`` / ``--dest-table`` are emitted only when the respective
    table is not ``None``. Omission is keyed on ``is None`` (not truthiness), so
    an explicit empty string is still forwarded as ``--source-table ""``. The
    table flags keep their original inline positions (source table between
    ``--source-uri`` and ``--dest-uri``, dest table right after ``--dest-uri``),
    so a call that supplies both produces a list byte-identical to the previous
    hard-coded literal.
    """
    args = [
        "ingest",
        "--source-uri",
        source_uri,
    ]

    if source_table is not None:
        args.append("--source-table")
        args.append(source_table)

    args.append("--dest-uri")
    args.append(dest_uri)

    if dest_table is not None:
        args.append("--dest-table")
        args.append(dest_table)

    if inc_strategy:
        args.append("--incremental-strategy")
        args.append(inc_strategy)

    if inc_key:
        args.append("--incremental-key")
        args.append(inc_key)

    if primary_key:
        if isinstance(primary_key, list):
            for key in primary_key:
                args.append("--primary-key")
                args.append(key)
        else:
            args.append("--primary-key")
            args.append(primary_key)

    if merge_key:
        args.append("--merge-key")
        args.append(merge_key)

    if interval_start:
        args.append("--interval-start")
        args.append(interval_start)

    if interval_end:
        args.append("--interval-end")
        args.append(interval_end)

    if sql_backend:
        args.append("--sql-backend")
        args.append(sql_backend)

    if loader_file_format:
        args.append("--loader-file-format")
        args.append(loader_file_format)

    if sql_exclude_columns:
        args.append("--sql-exclude-columns")
        args.append(sql_exclude_columns)

    if columns:
        args.append("--columns")
        args.append(columns)

    if sql_limit:
        args.append("--sql-limit")
        args.append(str(sql_limit))

    if yield_limit:
        args.append("--yield-limit")
        args.append(str(yield_limit))

    if mask:
        if isinstance(mask, str):
            mask = [mask]
        for m in mask:
            args.append("--mask")
            args.append(m)

    return args


def invoke_ingest_command(
    source_uri,
    source_table=None,
    dest_uri=_MISSING,
    dest_table=None,
    inc_strategy=None,
    inc_key=None,
    primary_key=None,
    merge_key=None,
    interval_start=None,
    interval_end=None,
    sql_backend=None,
    loader_file_format=None,
    sql_exclude_columns=None,
    columns=None,
    sql_limit=None,
    yield_limit=None,
    mask=None,
    print_output=True,
    run_in_subprocess=False,
    subprocess_timeout=120,
):
    """Invoke the ``ingest`` CLI command for a test.

    Both table arguments are optional, matching the CLI contract (``--source-table``
    and ``--dest-table`` are optional options). Omit the destination table by
    passing it positionally short::

        invoke_ingest_command(src_uri, src_table, dest_uri)  # no dest_table

    Omit the source table via the ``dest_uri`` keyword::

        invoke_ingest_command(src_uri, dest_uri=dest_uri)    # no source_table

    ``dest_uri`` remains required; omitting it raises ``TypeError``.
    """
    if dest_uri is _MISSING:
        raise TypeError("dest_uri is required")

    args = build_ingest_args(
        source_uri,
        dest_uri,
        source_table=source_table,
        dest_table=dest_table,
        inc_strategy=inc_strategy,
        inc_key=inc_key,
        primary_key=primary_key,
        merge_key=merge_key,
        interval_start=interval_start,
        interval_end=interval_end,
        sql_backend=sql_backend,
        loader_file_format=loader_file_format,
        sql_exclude_columns=sql_exclude_columns,
        columns=columns,
        sql_limit=sql_limit,
        yield_limit=yield_limit,
        mask=mask,
    )

    if not run_in_subprocess:
        result = CliRunner().invoke(
            app,
            args,
        )
        if result.exit_code != 0 and print_output:
            if result.exc_info is not None:
                traceback.print_exception(*result.exc_info)
            else:
                raise RuntimeError(f"Command failed with output: {result.stdout}")

        return result

    cmd = [sys.executable, "-m", "omniload.main"] + args
    env = os.environ.copy()

    process = subprocess.run(  # noqa: S603
        cmd,
        text=True,
        capture_output=True,
        env=env,
        timeout=subprocess_timeout,
    )

    # Create a result object similar to what CliRunner returns
    class Result:
        def __init__(self, exit_code, stdout, stderr, exc_info=None):
            self.exit_code = exit_code
            self.stdout = stdout
            self.stderr = stderr
            self.exc_info = exc_info

    result = Result(process.returncode, process.stdout, process.stderr)

    if result.exit_code != 0 and print_output:
        print(result.stdout)
        print(result.stderr)
        # traceback.print_exception(result.exc_info)

    return result
