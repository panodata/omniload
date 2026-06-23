import tempfile
from datetime import datetime, timezone
from typing import Optional

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
import sqlalchemy
from pyarrow import ipc as ipc

from tests.util import (
    invoke_ingest_command,
)
from tests.util.common import as_datetime, as_datetime2, get_random_string
from tests.warehouse.settings import DESTINATIONS


@pytest.mark.parametrize(
    "dest", list(DESTINATIONS.values()), ids=list(DESTINATIONS.keys())
)
def test_arrow_mmap_to_db_create_replace(dest):
    schema = f"testschema_arrow_mmap_create_replace_{get_random_string(5)}"

    def run_command(
        table: pa.Table,
        incremental_key: Optional[str] = None,
        incremental_strategy: Optional[str] = None,
    ):
        with tempfile.NamedTemporaryFile(suffix=".arrow", delete=True) as tmp:
            with pa.OSFile(tmp.name, "wb") as f:
                writer = ipc.new_file(f, table.schema)
                writer.write_table(table)
                writer.close()

            res = invoke_ingest_command(
                f"mmap://{tmp.name}",
                "whatever",
                dest_uri,
                f"{schema}.output",
                # we use this because postgres destination fails with nested fields, gonna have to investigate this more
                loader_file_format=(
                    "insert_values" if dest_uri.startswith("postgresql") else None
                ),
            )

            assert res.exit_code == 0
            return res

    dest_uri = dest.start()

    # let's start with a basic dataframe
    row_count = 1000
    df = pd.DataFrame(
        {
            "id": range(row_count),
            "value": np.random.rand(row_count),
            "category": np.random.choice(["A", "B", "C"], size=row_count),
            "nested": [{"a": 1, "b": 2, "c": {"d": 3}}] * row_count,
            "date": [as_datetime("2024-11-05")] * row_count,
        }
    )

    table = pa.Table.from_pandas(df)
    run_command(table)

    dest_engine = sqlalchemy.create_engine(dest_uri)
    with dest_engine.begin() as conn:
        res = conn.exec_driver_sql(f"select count(*) from {schema}.output").fetchall()
        assert res[0][0] == row_count

        res = conn.exec_driver_sql(
            f"select date, count(*) from {schema}.output group by 1 order by 1 asc"
        ).fetchall()
        assert res[0][0] == as_datetime("2024-11-05")
        assert res[0][1] == row_count
    dest_engine.dispose()

    # let's add a new column to the dataframe
    df["new_col"] = "some value"
    table = pa.Table.from_pandas(df)
    run_command(table)

    # there should be no change, just a new column
    with dest_engine.begin() as conn:
        res = conn.exec_driver_sql(f"select count(*) from {schema}.output").fetchall()
        assert res[0][0] == row_count

        res = conn.exec_driver_sql(
            f"select date, count(*) from {schema}.output group by 1 order by 1 asc"
        ).fetchall()
        assert res[0][0] == as_datetime("2024-11-05")
        assert res[0][1] == row_count

        res = conn.exec_driver_sql(
            f"select new_col, count(*) from {schema}.output group by 1 order by 1 asc"
        ).fetchall()
        assert res[0][0] == "some value"
        assert res[0][1] == row_count
    dest_engine.dispose()


@pytest.mark.parametrize(
    "dest", list(DESTINATIONS.values()), ids=list(DESTINATIONS.keys())
)
def test_arrow_mmap_to_db_delete_insert(dest):
    schema = f"testschema_arrow_mmap_del_ins_{get_random_string(5)}"

    def run_command(df: pd.DataFrame, incremental_key: Optional[str] = None):
        table = pa.Table.from_pandas(df)
        with tempfile.NamedTemporaryFile(suffix=".arrow", delete=True) as tmp:
            with pa.OSFile(tmp.name, "wb") as f:
                writer = ipc.new_file(f, table.schema)
                writer.write_table(table)
                writer.close()

            res = invoke_ingest_command(
                f"mmap://{tmp.name}",
                "whatever",
                dest_uri,
                f"{schema}.output",
                inc_key=incremental_key,
                inc_strategy="delete+insert",
            )

            assert res.exit_code == 0
            return res

    dest_uri = dest.start()
    dest_engine = sqlalchemy.create_engine(dest_uri)

    # let's start with a basic dataframe
    row_count = 1000
    df = pd.DataFrame(
        {
            "id": range(row_count),
            "value": np.random.rand(row_count),
            "category": np.random.choice(["A", "B", "C"], size=row_count),
            "date": pd.to_datetime(["2024-11-05"] * row_count),
        }
    )

    run_command(df, "date")

    def build_datetime(ds: str):
        dt: datetime = as_datetime2(ds)
        if dest_uri.startswith("clickhouse"):
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    def compare_dates(actual, expected_str):
        """Compare dates ignoring timezone for dlt 1.16.0 compatibility"""
        expected_date = build_datetime(expected_str)

        # If actual has timezone info and it causes time offset, compare just dates
        if hasattr(actual, "tzinfo") and actual.tzinfo is not None:
            # Compare only the date part for timezone-aware datetimes
            if hasattr(actual, "date") and hasattr(expected_date, "date"):
                return actual.date() == expected_date.date()

        # For timezone-naive comparison
        actual_date = actual
        if hasattr(actual_date, "replace"):
            actual_date = actual_date.replace(tzinfo=None)
        if hasattr(expected_date, "replace"):
            expected_date = expected_date.replace(tzinfo=None)
        return actual_date == expected_date

    # the first load, it should be loaded correctly
    with dest_engine.begin() as conn:
        res = conn.exec_driver_sql(f"select count(*) from {schema}.output").fetchall()
        assert res[0][0] == row_count

        res = conn.exec_driver_sql(
            f"select date, count(*) from {schema}.output group by 1 order by 1 asc"
        ).fetchall()
        assert compare_dates(res[0][0], "2024-11-05")
        assert res[0][1] == row_count
    dest_engine.dispose()

    # run again, it should be deleted and reloaded
    run_command(df, "date")
    with dest_engine.begin() as conn:
        res = conn.exec_driver_sql(f"select count(*) from {schema}.output").fetchall()
        assert res[0][0] == row_count

        res = conn.exec_driver_sql(
            f"select date, count(*) from {schema}.output group by 1 order by 1 asc"
        ).fetchall()
        assert compare_dates(res[0][0], "2024-11-05")
        assert res[0][1] == row_count
    dest_engine.dispose()

    # append 1000 new rows with a different date
    new_rows = pd.DataFrame(
        {
            "id": range(row_count, row_count + 1000),
            "value": np.random.rand(1000),
            "category": np.random.choice(["A", "B", "C"], size=1000),
            "date": pd.to_datetime(["2024-11-06"] * 1000),
        }
    )
    df = pd.concat([df, new_rows], ignore_index=True)

    run_command(df, "date")

    with dest_engine.begin() as conn:
        res = conn.exec_driver_sql(f"select count(*) from {schema}.output").fetchall()
        assert res[0][0] == row_count + 1000

        res = conn.exec_driver_sql(
            f"select date, count(*) from {schema}.output group by 1 order by 1 asc"
        ).fetchall()
        assert compare_dates(res[0][0], "2024-11-05")
        assert res[0][1] == row_count
        assert compare_dates(res[1][0], "2024-11-06")
        assert res[1][1] == 1000
    dest_engine.dispose()

    # append 1000 old rows for a previous date, these should not be loaded
    old_rows = pd.DataFrame(
        {
            "id": range(row_count, row_count + 1000),
            "value": np.random.rand(1000),
            "category": np.random.choice(["A", "B", "C"], size=1000),
            "date": pd.to_datetime(["2024-11-04"] * 1000),
        }
    )
    df = pd.concat([df, old_rows], ignore_index=True)

    run_command(df, "date")
    with dest_engine.begin() as conn:
        res = conn.exec_driver_sql(f"select count(*) from {schema}.output").fetchall()
        assert res[0][0] == row_count + 1000

        res = conn.exec_driver_sql(
            f"select date, count(*) from {schema}.output group by 1 order by 1 asc"
        ).fetchall()
        assert compare_dates(res[0][0], "2024-11-05")
        assert res[0][1] == row_count
        assert compare_dates(res[1][0], "2024-11-06")
        assert res[1][1] == 1000
    dest_engine.dispose()


@pytest.mark.parametrize(
    "dest", list(DESTINATIONS.values()), ids=list(DESTINATIONS.keys())
)
def test_arrow_mmap_to_db_merge_without_incremental(dest):
    schema = f"testschema_arrow_mmap_{get_random_string(5)}"

    def run_command(df: pd.DataFrame):
        table = pa.Table.from_pandas(df)
        with tempfile.NamedTemporaryFile(suffix=".arrow", delete=True) as tmp:
            with pa.OSFile(tmp.name, "wb") as f:
                writer = ipc.new_file(f, table.schema)
                writer.write_table(table)
                writer.close()

            res = invoke_ingest_command(
                f"mmap://{tmp.name}",
                "whatever",
                dest_uri,
                f"{schema}.output",
                inc_strategy="merge",
                primary_key="id",
            )
            assert res.exit_code == 0
            return res

    dest_uri = dest.start()
    dest_engine = sqlalchemy.create_engine(dest_uri)

    # let's start with a basic dataframe
    row_count = 1000
    df = pd.DataFrame({"id": range(row_count), "value": ["a"] * row_count})

    run_command(df)

    # the first load, it should be loaded correctly
    with dest_engine.begin() as conn:
        res = conn.exec_driver_sql(f"select count(*) from {schema}.output").fetchall()
        assert res[0][0] == row_count

        res = conn.exec_driver_sql(
            f"select value, count(*) from {schema}.output group by 1 order by 1 asc"
        ).fetchall()
        assert res[0][0] == "a"
        assert res[0][1] == row_count
    dest_engine.dispose()

    # run again, no change
    run_command(df)
    with dest_engine.begin() as conn:
        res = conn.exec_driver_sql(f"select count(*) from {schema}.output").fetchall()
        assert res[0][0] == row_count

        res = conn.exec_driver_sql(
            f"select value, count(*) from {schema}.output group by 1 order by 1 asc"
        ).fetchall()
        assert res[0][0] == "a"
        assert res[0][1] == row_count
    dest_engine.dispose()

    # append 1000 new rows with a different value
    new_rows = pd.DataFrame(
        {
            "id": range(row_count, row_count + 1000),
            "value": ["b"] * 1000,
        }
    )
    df = pd.concat([df, new_rows], ignore_index=True)

    run_command(df)

    with dest_engine.begin() as conn:
        res = conn.exec_driver_sql(f"select count(*) from {schema}.output").fetchall()

        assert res[0][0] == row_count + 1000

        res = conn.exec_driver_sql(
            f"select value, count(*) from {schema}.output group by 1 order by 1 asc"
        ).fetchall()
        assert res[0][0] == "a"
        assert res[0][1] == row_count
        assert res[1][0] == "b"
        assert res[1][1] == 1000

    dest_engine.dispose()

    # append 1000 old rows for previous ids, they should be merged
    old_rows = pd.DataFrame(
        {
            "id": range(row_count, row_count + 1000),
            "value": ["a"] * 1000,
        }
    )
    run_command(old_rows)
    with dest_engine.begin() as conn:
        res = conn.exec_driver_sql(f"select count(*) from {schema}.output").fetchall()
        assert res[0][0] == row_count + 1000
        res = conn.exec_driver_sql(
            f"select value, count(*) from {schema}.output group by 1 order by 1 asc"
        ).fetchall()
        assert res[0][0] == "a"
        assert res[0][1] == row_count + 1000
    dest_engine.dispose()
