import traceback
from concurrent.futures import ThreadPoolExecutor

import pytest
import sqlalchemy
from sqlalchemy.pool import NullPool

from tests.util import invoke_ingest_command
from tests.util.common import as_datetime, get_random_string
from tests.util.container.impl.duckdb import EphemeralDuckDb
from tests.warehouse.operations import (
    custom_query_tests,
    db_to_db_append,
    db_to_db_create_replace,
    db_to_db_delete_insert_with_timerange,
    db_to_db_delete_insert_without_primary_key,
    db_to_db_merge_with_primary_key,
)
from tests.warehouse.settings import DESTINATIONS, SOURCES


@pytest.mark.parametrize(
    "dest", list(DESTINATIONS.values()), ids=list(DESTINATIONS.keys())
)
@pytest.mark.parametrize("source", list(SOURCES.values()), ids=list(SOURCES.keys()))
@pytest.mark.parametrize("testcase", custom_query_tests())
def test_custom_query(testcase, source, dest):
    with ThreadPoolExecutor() as executor:
        source_future = executor.submit(source.start)
        dest_future = executor.submit(dest.start)
        source_uri = source_future.result()
        dest_uri = dest_future.result()
    testcase(source_uri, dest_uri)
    source.stop()
    dest.stop()


@pytest.mark.parametrize(
    "dest", list(DESTINATIONS.values()), ids=list(DESTINATIONS.keys())
)
@pytest.mark.parametrize("source", list(SOURCES.values()), ids=list(SOURCES.keys()))
def test_create_replace(source, dest):
    with ThreadPoolExecutor() as executor:
        source_future = executor.submit(source.start)
        dest_future = executor.submit(dest.start)
        source_uri = source_future.result()
        dest_uri = dest_future.result()
    db_to_db_create_replace(source_uri, dest_uri)
    source.stop()
    dest.stop()


@pytest.mark.parametrize(
    "dest", list(DESTINATIONS.values()), ids=list(DESTINATIONS.keys())
)
@pytest.mark.parametrize("source", list(SOURCES.values()), ids=list(SOURCES.keys()))
def test_append(source, dest):
    with ThreadPoolExecutor() as executor:
        source_future = executor.submit(source.start)
        dest_future = executor.submit(dest.start)
        source_uri = source_future.result()
        dest_uri = dest_future.result()
    db_to_db_append(source_uri, dest_uri)
    source.stop()
    dest.stop()


@pytest.mark.parametrize(
    "dest", list(DESTINATIONS.values()), ids=list(DESTINATIONS.keys())
)
@pytest.mark.parametrize("source", list(SOURCES.values()), ids=list(SOURCES.keys()))
def test_merge_with_primary_key(source, dest):
    with ThreadPoolExecutor() as executor:
        source_future = executor.submit(source.start)
        dest_future = executor.submit(dest.start)
        source_uri = source_future.result()
        dest_uri = dest_future.result()
    db_to_db_merge_with_primary_key(source_uri, dest_uri)
    source.stop()
    dest.stop()


@pytest.mark.parametrize(
    "dest", list(DESTINATIONS.values()), ids=list(DESTINATIONS.keys())
)
@pytest.mark.parametrize("source", list(SOURCES.values()), ids=list(SOURCES.keys()))
def test_delete_insert_without_primary_key(source, dest):
    with ThreadPoolExecutor() as executor:
        source_future = executor.submit(source.start)
        dest_future = executor.submit(dest.start)
        source_uri = source_future.result()
        dest_uri = dest_future.result()
    db_to_db_delete_insert_without_primary_key(source_uri, dest_uri)
    source.stop()
    dest.stop()


@pytest.mark.parametrize(
    "dest", list(DESTINATIONS.values()), ids=list(DESTINATIONS.keys())
)
@pytest.mark.parametrize("source", list(SOURCES.values()), ids=list(SOURCES.keys()))
def test_delete_insert_with_time_range(source, dest):
    with ThreadPoolExecutor() as executor:
        source_future = executor.submit(source.start)
        dest_future = executor.submit(dest.start)
        source_uri = source_future.result()
        dest_uri = dest_future.result()
    db_to_db_delete_insert_with_timerange(source_uri, dest_uri)
    source.stop()
    dest.stop()


@pytest.mark.parametrize(
    "dest", list(DESTINATIONS.values()), ids=list(DESTINATIONS.keys())
)
@pytest.mark.parametrize("source", list(SOURCES.values()), ids=list(SOURCES.keys()))
def test_db_to_db_exclude_columns(source, dest):
    with ThreadPoolExecutor() as executor:
        source_future = executor.submit(source.start)
        dest_future = executor.submit(dest.start)
        source_uri = source_future.result()
        dest_uri = dest_future.result()

    schema_rand_prefix = f"testschema_db_to_db_exclude_columns_{get_random_string(5)}"

    source_engine = sqlalchemy.create_engine(source_uri)
    with source_engine.begin() as conn:
        conn.exec_driver_sql(f"DROP SCHEMA IF EXISTS {schema_rand_prefix}")
        conn.exec_driver_sql(f"CREATE SCHEMA {schema_rand_prefix}")
        conn.exec_driver_sql(
            f"CREATE TABLE {schema_rand_prefix}.input (id INTEGER, val VARCHAR(20), updated_at DATE, col_to_exclude1 VARCHAR(20), col_to_exclude2 VARCHAR(20))"
        )
        conn.exec_driver_sql(
            f"INSERT INTO {schema_rand_prefix}.input VALUES (1, 'val1', '2022-01-01', 'col1', 'col2')"
        )
        conn.exec_driver_sql(
            f"INSERT INTO {schema_rand_prefix}.input VALUES (2, 'val2', '2022-02-01', 'col1', 'col2')"
        )
        res = conn.exec_driver_sql(
            f"select count(*) from {schema_rand_prefix}.input"
        ).fetchall()
        assert res[0][0] == 2
    source_engine.dispose()
    result = invoke_ingest_command(
        source_uri,
        f"{schema_rand_prefix}.input",
        dest_uri,
        f"{schema_rand_prefix}.output",
        sql_exclude_columns="col_to_exclude1,col_to_exclude2",
    )

    assert result.exit_code == 0

    dest_engine = sqlalchemy.create_engine(dest_uri)
    with dest_engine.begin() as dest_conn:
        res = dest_conn.exec_driver_sql(
            f"select id, val, updated_at from {schema_rand_prefix}.output"
        ).fetchall()

        assert len(res) == 2
        assert res[0] == (1, "val1", as_datetime("2022-01-01"))
        assert res[1] == (2, "val2", as_datetime("2022-02-01"))

        # Verify excluded columns don't exist in destination schema
        columns = dest_conn.exec_driver_sql(
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_schema = '{schema_rand_prefix}' AND table_name = 'output' "
            f"ORDER BY ordinal_position"
        ).fetchall()
        assert columns == [("id",), ("val",), ("updated_at",)]

    # Clean up
    dest_engine.dispose()
    source.stop()
    dest.stop()


def test_sql_limit():
    source_instance = EphemeralDuckDb()
    dest_instance = EphemeralDuckDb()

    source_uri = source_instance.start()
    dest_uri = dest_instance.start()

    schema_rand_prefix = f"test_sql_limit_{get_random_string(5)}"
    source_engine = sqlalchemy.create_engine(source_uri, poolclass=NullPool)
    with source_engine.begin() as conn:
        conn.exec_driver_sql(f"DROP SCHEMA IF EXISTS {schema_rand_prefix}")
        conn.exec_driver_sql(f"CREATE SCHEMA {schema_rand_prefix}")
        conn.exec_driver_sql(
            f"CREATE TABLE {schema_rand_prefix}.input (id INTEGER, val VARCHAR(20), updated_at DATE)"
        )
        conn.exec_driver_sql(
            f"""INSERT INTO {schema_rand_prefix}.input VALUES 
                (1, 'val1', '2024-01-01'),
                (2, 'val2', '2024-01-01'),
                (3, 'val3', '2024-01-01'),
                (4, 'val4', '2024-01-02'),
                (5, 'val5', '2024-01-02')"""
        )
        res = conn.exec_driver_sql(
            f"select count(*) from {schema_rand_prefix}.input"
        ).fetchall()
        assert res[0][0] == 5

    result = invoke_ingest_command(
        source_uri,
        f"{schema_rand_prefix}.input",
        dest_uri,
        f"{schema_rand_prefix}.output",
        sql_backend="sqlalchemy",
        sql_limit=4,
    )
    if result.exception:
        traceback.print_exception(*result.exc_info)
    assert result.exit_code == 0

    dest_engine = sqlalchemy.create_engine(dest_uri, poolclass=NullPool)
    with dest_engine.connect() as dest_conn:
        res = dest_conn.exec_driver_sql(
            f"select id, val, updated_at from {schema_rand_prefix}.output order by id asc"
        ).fetchall()

    assert res == [
        (1, "val1", as_datetime("2024-01-01")),
        (2, "val2", as_datetime("2024-01-01")),
        (3, "val3", as_datetime("2024-01-01")),
        (4, "val4", as_datetime("2024-01-02")),
    ]

    source_instance.stop()
    dest_instance.stop()


def test_date_coercion_issue():
    """
    By default, omniload treats the start and end dates as datetime objects. While this worked fine for many cases, if the
    incremental field is a date, the start and end dates cannot be compared to the incremental field, and the ingestion would fail.
    In order to eliminate this, we have introduced a new option to omniload, --columns, which allows the user to specify the column types for the destination table.
    This way, omniload will know the data type of the incremental field, and will be able to convert the start and end dates to the correct data type before running the ingestion.
    """
    source_instance = EphemeralDuckDb()
    dest_instance = EphemeralDuckDb()

    source_uri = source_instance.start()
    dest_uri = dest_instance.start()

    schema_rand_prefix = f"test_date_coercion_{get_random_string(5)}"
    source_engine = sqlalchemy.create_engine(source_uri, poolclass=NullPool)
    with source_engine.begin() as conn:
        conn.exec_driver_sql(f"DROP SCHEMA IF EXISTS {schema_rand_prefix}")
        conn.exec_driver_sql(f"CREATE SCHEMA {schema_rand_prefix}")
        conn.exec_driver_sql(
            f"CREATE TABLE {schema_rand_prefix}.input (id INTEGER, val VARCHAR(20), updated_at DATE)"
        )
        conn.exec_driver_sql(
            f"""INSERT INTO {schema_rand_prefix}.input VALUES 
                (1, 'val1', '2024-01-01'),
                (2, 'val2', '2024-01-01'),
                (3, 'val3', '2024-01-01'),
                (4, 'val4', '2024-01-02'),
                (5, 'val5', '2024-01-02'),
                (6, 'val6', '2024-01-02'),
                (7, 'val7', '2024-01-03'),
                (8, 'val8', '2024-01-03'),
                (9, 'val9', '2024-01-03')"""
        )
        res = conn.exec_driver_sql(
            f"select count(*) from {schema_rand_prefix}.input"
        ).fetchall()
        assert res[0][0] == 9

    result = invoke_ingest_command(
        source_uri,
        f"{schema_rand_prefix}.input",
        dest_uri,
        f"{schema_rand_prefix}.output",
        inc_strategy="delete+insert",
        inc_key="updated_at",
        sql_backend="sqlalchemy",
        interval_start="2024-01-01",
        interval_end="2024-01-02",
        columns="updated_at:date",
    )
    if result.exception:
        traceback.print_exception(*result.exc_info)
    assert result.exit_code == 0

    dest_engine = sqlalchemy.create_engine(dest_uri, poolclass=NullPool)
    with dest_engine.connect() as dest_conn:
        res = dest_conn.exec_driver_sql(
            f"select id, val, updated_at from {schema_rand_prefix}.output order by id asc"
        ).fetchall()

    assert res == [
        (1, "val1", as_datetime("2024-01-01")),
        (2, "val2", as_datetime("2024-01-01")),
        (3, "val3", as_datetime("2024-01-01")),
        (4, "val4", as_datetime("2024-01-02")),
        (5, "val5", as_datetime("2024-01-02")),
        (6, "val6", as_datetime("2024-01-02")),
    ]

    source_instance.stop()
    dest_instance.stop()
