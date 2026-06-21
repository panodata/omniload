import shutil
from datetime import datetime, timezone

import pytest
import sqlalchemy

from tests.util import get_abs_path, get_random_string, invoke_ingest_command
from tests.warehouse.container import DESTINATIONS, mysqlDocker


@pytest.mark.parametrize("source", [mysqlDocker], ids=["mysql8"])
@pytest.mark.parametrize(
    "dest", list(DESTINATIONS.values()), ids=list(DESTINATIONS.keys())
)
def test_mysql_zero_dates(source, dest):
    source_uri = source.start()
    dest_uri = dest.start()

    schema_rand_prefix = f"testschema_mysql_zero_dates_{get_random_string(5)}"
    try:
        shutil.rmtree(get_abs_path("../pipeline_data"))
    except Exception:
        pass

    source_engine = sqlalchemy.create_engine(source_uri)
    with source_engine.begin() as conn:
        conn.exec_driver_sql(f"DROP SCHEMA IF EXISTS {schema_rand_prefix}")
        conn.exec_driver_sql(f"CREATE SCHEMA {schema_rand_prefix}")
        conn.exec_driver_sql(
            f"""
            CREATE TABLE {schema_rand_prefix}.input (
                name VARCHAR(255),
                created_at DATETIME
            );"""
        )
        conn.exec_driver_sql(
            f"INSERT INTO {schema_rand_prefix}.input VALUES ('Row 1', null), ('Row 2', '2024-01-01 12:00:00'), ('Row 3', null), ('Row 4', '2025-04-05 08:30:00'), ('Row 5', null)"
        )

        conn.exec_driver_sql("SET sql_mode = '';")

        # this is the crucial step of this test: once the field becomes non-nullable, MySQL starts returning `0000-00-00 00:00:00` for empty dates.
        conn.exec_driver_sql(
            f"ALTER TABLE {schema_rand_prefix}.input MODIFY created_at DATETIME NOT NULL"
        )

        res = conn.exec_driver_sql(
            f"select count(*) from {schema_rand_prefix}.input"
        ).fetchall()
        assert res[0][0] == 5
    source_engine.dispose()

    result = invoke_ingest_command(
        source_uri,
        f"{schema_rand_prefix}.input",
        dest_uri,
        f"{schema_rand_prefix}.output",
        sql_backend="sqlalchemy",
    )

    assert result.exit_code == 0

    dest_engine = sqlalchemy.create_engine(dest_uri)
    dest_conn = dest_engine.connect()
    res = dest_conn.exec_driver_sql(
        f"select * from {schema_rand_prefix}.output"
    ).fetchall()
    dest_engine.dispose()

    # assert there are no new rows, since DBs like DuckDB accept NULL and dlt adds a separate string column for the value `0000-00-00 00:00:00`
    # we want 4 columns: name, created_at, _dlt_load_id, _dlt_id
    assert len(res[0]) == 4

    # import pdb; pdb.set_trace()

    res = [
        (
            row[0],
            (
                row[1].astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                if isinstance(row[1], datetime)
                else row[1]
            ),
        )
        for row in res
    ]

    assert len(res) == 5
    assert res[0] == ("Row 1", "1969-12-31 23:00:00")
    assert res[1] == ("Row 2", "2024-01-01 11:00:00")
    assert res[2] == ("Row 3", "1969-12-31 23:00:00")
    assert res[3] == ("Row 4", "2025-04-05 06:30:00")
    assert res[4] == ("Row 5", "1969-12-31 23:00:00")

    # Clean up
    source.stop()
    dest.stop()
