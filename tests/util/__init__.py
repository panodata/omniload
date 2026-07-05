import os
import subprocess
import sys
import traceback

from typer.testing import CliRunner

from omniload.main import app


def invoke_ingest_command(
    source_uri,
    source_table,
    dest_uri,
    dest_table,
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
    args = [
        "ingest",
        "--source-uri",
        source_uri,
        "--source-table",
        source_table,
        "--dest-uri",
        dest_uri,
        "--dest-table",
        dest_table,
    ]

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
