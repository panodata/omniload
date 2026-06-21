import traceback

import sqlalchemy
from sqlalchemy.pool import NullPool

from tests.util import (
    as_datetime,
    get_random_string,
    invoke_ingest_command,
)


def db_to_db_create_replace(source_connection_url: str, dest_connection_url: str):
    schema_rand_prefix = f"testschema_create_replace_{get_random_string(5)}"

    source_engine = sqlalchemy.create_engine(source_connection_url)
    with source_engine.begin() as conn:
        conn.exec_driver_sql(f"DROP SCHEMA IF EXISTS {schema_rand_prefix}")
        conn.exec_driver_sql(f"CREATE SCHEMA {schema_rand_prefix}")
        conn.exec_driver_sql(
            f"CREATE TABLE {schema_rand_prefix}.input (id INTEGER, val VARCHAR(20), updated_at DATE)"
        )
        conn.exec_driver_sql(
            f"INSERT INTO {schema_rand_prefix}.input VALUES (1, 'val1', '2022-01-01')"
        )
        conn.exec_driver_sql(
            f"INSERT INTO {schema_rand_prefix}.input VALUES (2, 'val2', '2022-02-01')"
        )
        res = conn.exec_driver_sql(
            f"select count(*) from {schema_rand_prefix}.input"
        ).fetchall()
        assert res[0][0] == 2
    source_engine.dispose()

    result = invoke_ingest_command(
        source_connection_url,
        f"{schema_rand_prefix}.input",
        dest_connection_url,
        f"{schema_rand_prefix}.output",
    )

    assert result.exit_code == 0

    dest_engine = sqlalchemy.create_engine(dest_connection_url)
    with dest_engine.connect() as dest_conn:
        res = dest_conn.exec_driver_sql(
            f"select id, val, updated_at from {schema_rand_prefix}.output"
        ).fetchall()
    dest_engine.dispose()

    assert len(res) == 2
    assert res[0] == (1, "val1", as_datetime("2022-01-01"))
    assert res[1] == (2, "val2", as_datetime("2022-02-01"))


def db_to_db_append(source_connection_url: str, dest_connection_url: str):
    schema_rand_prefix = f"testschema_append_{get_random_string(5)}"

    source_engine = sqlalchemy.create_engine(source_connection_url)
    dest_engine = sqlalchemy.create_engine(dest_connection_url)

    with source_engine.begin() as conn:
        conn.exec_driver_sql(f"DROP SCHEMA IF EXISTS {schema_rand_prefix}")
        conn.exec_driver_sql(f"CREATE SCHEMA {schema_rand_prefix}")
        conn.exec_driver_sql(
            f"CREATE TABLE {schema_rand_prefix}.input (id INTEGER, val VARCHAR(20), updated_at DATE)"
        )
        conn.exec_driver_sql(
            f"INSERT INTO {schema_rand_prefix}.input VALUES (1, 'val1', '2022-01-01'), (2, 'val2', '2022-01-02')"
        )
        res = conn.exec_driver_sql(
            f"select count(*) from {schema_rand_prefix}.input"
        ).fetchall()
        assert res[0][0] == 2
    source_engine.dispose()

    def run():
        res = invoke_ingest_command(
            source_connection_url,
            f"{schema_rand_prefix}.input",
            dest_connection_url,
            f"{schema_rand_prefix}.output",
            "append",
            "updated_at",
            sql_backend="sqlalchemy",
        )
        assert res.exit_code == 0

    def get_output_table():
        with dest_engine.connect() as dest_conn:
            results = dest_conn.exec_driver_sql(
                f"select id, val, updated_at from {schema_rand_prefix}.output order by id asc"
            ).fetchall()
        dest_engine.dispose()
        return results

    run()

    res = get_output_table()
    assert len(res) == 2
    assert res[0] == (1, "val1", as_datetime("2022-01-01"))
    assert res[1] == (2, "val2", as_datetime("2022-01-02"))

    # # run again, nothing should be inserted into the output table
    run()

    res = get_output_table()
    assert len(res) == 2
    assert res[0] == (1, "val1", as_datetime("2022-01-01"))
    assert res[1] == (2, "val2", as_datetime("2022-01-02"))


def db_to_db_merge_with_primary_key(
    source_connection_url: str, dest_connection_url: str
):
    schema_rand_prefix = f"testschema_merge_{get_random_string(5)}"

    source_engine = sqlalchemy.create_engine(source_connection_url)
    with source_engine.begin() as conn:
        conn.exec_driver_sql(f"DROP SCHEMA IF EXISTS {schema_rand_prefix}")
        conn.exec_driver_sql(f"CREATE SCHEMA {schema_rand_prefix}")
        conn.exec_driver_sql(
            f"CREATE TABLE {schema_rand_prefix}.input (id INTEGER NOT NULL, val VARCHAR(20), updated_at DATE NOT NULL)"
        )
        conn.exec_driver_sql(
            f"INSERT INTO {schema_rand_prefix}.input VALUES (1, 'val1', '2022-01-01')"
        )
        conn.exec_driver_sql(
            f"INSERT INTO {schema_rand_prefix}.input VALUES (2, 'val2', '2022-02-01')"
        )

        res = conn.exec_driver_sql(
            f"select count(*) from {schema_rand_prefix}.input"
        ).fetchall()
        assert res[0][0] == 2

    source_engine.dispose()

    def run():
        res = invoke_ingest_command(
            source_connection_url,
            f"{schema_rand_prefix}.input",
            dest_connection_url,
            f"{schema_rand_prefix}.output",
            "merge",
            "updated_at",
            "id",
            sql_backend="sqlalchemy",
        )
        assert res.exit_code == 0
        return res

    dest_engine = sqlalchemy.create_engine(dest_connection_url)

    def get_output_rows():
        with dest_engine.connect() as dest_conn:
            return dest_conn.exec_driver_sql(
                f"select id, val, updated_at from {schema_rand_prefix}.output order by id asc"
            ).fetchall()

    def assert_output_equals(expected):
        res = get_output_rows()
        assert len(res) == len(expected)
        for i, row in enumerate(expected):
            assert res[i] == row

    dest_engine.dispose()
    res = run()
    assert_output_equals(
        [(1, "val1", as_datetime("2022-01-01")), (2, "val2", as_datetime("2022-02-01"))]
    )

    with dest_engine.connect() as dest_conn:
        first_run_id = dest_conn.exec_driver_sql(
            f"select _dlt_load_id from {schema_rand_prefix}.output limit 1"
        ).fetchall()[0][0]

    dest_engine.dispose()

    ##############################
    # we'll run again, we don't expect any changes since the data hasn't changed
    res = run()
    assert_output_equals(
        [(1, "val1", as_datetime("2022-01-01")), (2, "val2", as_datetime("2022-02-01"))]
    )

    # we also ensure that the other rows were not touched
    with dest_engine.connect() as dest_conn:
        count_by_run_id = dest_conn.exec_driver_sql(
            f"select _dlt_load_id, count(*) from {schema_rand_prefix}.output group by 1 order by 2 desc"
        ).fetchall()
    assert len(count_by_run_id) == 1
    assert count_by_run_id[0][1] == 2
    assert count_by_run_id[0][0] == first_run_id
    dest_engine.dispose()
    ##############################

    ##############################
    # now we'll modify the source data but not the updated at, the output table should not be updated
    with source_engine.begin() as conn:
        conn.exec_driver_sql(
            f"UPDATE {schema_rand_prefix}.input SET val = 'val1_modified' WHERE id = 2"
        )
    source_engine.dispose()

    run()
    assert_output_equals(
        [(1, "val1", as_datetime("2022-01-01")), (2, "val2", as_datetime("2022-02-01"))]
    )

    # we also ensure that the other rows were not touched
    with dest_engine.connect() as dest_conn:
        count_by_run_id = dest_conn.exec_driver_sql(
            f"select _dlt_load_id, count(*) from {schema_rand_prefix}.output group by 1"
        ).fetchall()
    assert len(count_by_run_id) == 1
    assert count_by_run_id[0][1] == 2
    assert count_by_run_id[0][0] == first_run_id
    dest_engine.dispose()
    ##############################

    ##############################
    # now we'll insert a new row but with an old date, the new row will not show up
    with source_engine.begin() as conn:
        conn.exec_driver_sql(
            f"INSERT INTO {schema_rand_prefix}.input VALUES (3, 'val3', '2022-01-01')"
        )
    source_engine.dispose()

    run()
    assert_output_equals(
        [(1, "val1", as_datetime("2022-01-01")), (2, "val2", as_datetime("2022-02-01"))]
    )

    # we also ensure that the other rows were not touched
    with dest_engine.connect() as dest_conn:
        count_by_run_id = dest_conn.exec_driver_sql(
            f"select _dlt_load_id, count(*) from {schema_rand_prefix}.output group by 1"
        ).fetchall()
    assert len(count_by_run_id) == 1
    assert count_by_run_id[0][1] == 2
    assert count_by_run_id[0][0] == first_run_id
    dest_engine.dispose()
    ##############################

    ##############################
    # now we'll insert a new row but with a new date, the new row will show up
    with source_engine.begin() as conn:
        conn.exec_driver_sql(
            f"INSERT INTO {schema_rand_prefix}.input VALUES (3, 'val3', '2022-02-02')"
        )
    source_engine.dispose()

    run()
    assert_output_equals(
        [
            (1, "val1", as_datetime("2022-01-01")),
            (2, "val2", as_datetime("2022-02-01")),
            (3, "val3", as_datetime("2022-02-02")),
        ]
    )

    # we have a new run that inserted rows to this table, so the run count should be 2
    with dest_engine.connect() as dest_conn:
        count_by_run_id = dest_conn.exec_driver_sql(
            f"select _dlt_load_id, count(*) from {schema_rand_prefix}.output group by 1 order by 2 desc"
        ).fetchall()
    assert len(count_by_run_id) == 2
    assert count_by_run_id[0][1] == 2
    assert count_by_run_id[0][0] == first_run_id
    # we don't care about the run ID
    assert count_by_run_id[1][1] == 1
    dest_engine.dispose()
    ##############################

    ##############################
    # lastly, let's try modifying the updated_at of an old column, it should be updated in the output table
    with source_engine.begin() as conn:
        conn.exec_driver_sql(
            f"UPDATE {schema_rand_prefix}.input SET val='val2_modified', updated_at = '2022-02-03' WHERE id = 2"
        )
    source_engine.dispose()

    run()
    assert_output_equals(
        [
            (1, "val1", as_datetime("2022-01-01")),
            (2, "val2_modified", as_datetime("2022-02-03")),
            (3, "val3", as_datetime("2022-02-02")),
        ]
    )

    # we have a new run that inserted rows to this table, so the run count should be 2
    with dest_engine.connect() as dest_conn:
        count_by_run_id = dest_conn.exec_driver_sql(
            f"select _dlt_load_id, count(*) from {schema_rand_prefix}.output group by 1 order by 2 desc, 1 asc"
        ).fetchall()
    assert len(count_by_run_id) == 3
    assert count_by_run_id[0][1] == 1
    assert count_by_run_id[0][0] == first_run_id
    # we don't care about the rest of the run IDs
    assert count_by_run_id[1][1] == 1
    assert count_by_run_id[2][1] == 1
    dest_engine.dispose()
    ##############################


def db_to_db_delete_insert_without_primary_key(
    source_connection_url: str, dest_connection_url: str
):
    schema_rand_prefix = f"testschema_delete_insert_{get_random_string(5)}"

    source_engine = sqlalchemy.create_engine(source_connection_url)
    with source_engine.begin() as source_conn:
        source_conn.exec_driver_sql(f"DROP SCHEMA IF EXISTS {schema_rand_prefix}")
        source_conn.exec_driver_sql(f"CREATE SCHEMA {schema_rand_prefix}")
        source_conn.exec_driver_sql(
            f"CREATE TABLE {schema_rand_prefix}.input (id INTEGER, val VARCHAR(20), updated_at DATE)"
        )
        source_conn.exec_driver_sql(
            f"INSERT INTO {schema_rand_prefix}.input VALUES (1, 'val1', '2022-01-01')"
        )
        source_conn.exec_driver_sql(
            f"INSERT INTO {schema_rand_prefix}.input VALUES (2, 'val2', '2022-02-01')"
        )

        res = source_conn.exec_driver_sql(
            f"select count(*) from {schema_rand_prefix}.input"
        ).fetchall()
        assert res[0][0] == 2
    source_engine.dispose()

    def run():
        res = invoke_ingest_command(
            source_connection_url,
            f"{schema_rand_prefix}.input",
            dest_connection_url,
            f"{schema_rand_prefix}.output",
            inc_strategy="delete+insert",
            inc_key="updated_at",
            sql_backend="sqlalchemy",
        )
        if res.exit_code != 0:
            traceback.print_exception(*res.exc_info)
        assert res.exit_code == 0, res.output
        return res

    dest_engine = sqlalchemy.create_engine(dest_connection_url)

    def get_output_rows():
        with dest_engine.connect() as dest_conn:
            results = dest_conn.exec_driver_sql(
                f"select id, val, updated_at from {schema_rand_prefix}.output order by id asc"
            ).fetchall()
        dest_engine.dispose()
        return results

    def assert_output_equals(expected):
        res = get_output_rows()
        assert len(res) == len(expected)
        for i, row in enumerate(expected):
            assert res[i] == row

    run()
    assert_output_equals(
        [(1, "val1", as_datetime("2022-01-01")), (2, "val2", as_datetime("2022-02-01"))]
    )

    with dest_engine.connect() as dest_conn:
        first_run_id = dest_conn.exec_driver_sql(
            f"select _dlt_load_id from {schema_rand_prefix}.output limit 1"
        ).fetchall()[0][0]
    dest_engine.dispose()

    ##############################
    # we'll run again, since this is a delete+insert, we expect the run ID to change for the last one
    res = run()
    assert_output_equals(
        [(1, "val1", as_datetime("2022-01-01")), (2, "val2", as_datetime("2022-02-01"))]
    )

    # we ensure that one of the rows is updated with a new run
    with dest_engine.connect() as dest_conn:
        count_by_run_id = dest_conn.exec_driver_sql(
            f"select _dlt_load_id, count(*) from {schema_rand_prefix}.output group by 1 order by 1 asc"
        ).fetchall()
    assert len(count_by_run_id) == 2
    assert count_by_run_id[0][0] == first_run_id
    assert count_by_run_id[0][1] == 1
    assert count_by_run_id[1][0] != first_run_id
    assert count_by_run_id[1][1] == 1
    dest_engine.dispose()
    ##############################

    ##############################
    # now we'll insert a few more lines for the same day, the new rows should show up
    with source_engine.begin() as source_conn:
        source_conn.exec_driver_sql(
            f"INSERT INTO {schema_rand_prefix}.input VALUES (3, 'val3', '2022-02-01'), (4, 'val4', '2022-02-01')"
        )
    source_engine.dispose()

    run()
    assert_output_equals(
        [
            (1, "val1", as_datetime("2022-01-01")),
            (2, "val2", as_datetime("2022-02-01")),
            (3, "val3", as_datetime("2022-02-01")),
            (4, "val4", as_datetime("2022-02-01")),
        ]
    )

    # the new rows should have a new run ID, there should be 2 distinct runs now
    with dest_engine.connect() as dest_conn:
        count_by_run_id = dest_conn.exec_driver_sql(
            f"select _dlt_load_id, count(*) from {schema_rand_prefix}.output group by 1 order by 2 desc, 1 asc"
        ).fetchall()
    assert len(count_by_run_id) == 2
    assert count_by_run_id[0][0] != first_run_id
    assert count_by_run_id[0][1] == 3  # 2 new rows + 1 old row
    assert count_by_run_id[1][0] == first_run_id
    assert count_by_run_id[1][1] == 1
    dest_engine.dispose()
    ##############################


def db_to_db_delete_insert_with_timerange(
    source_connection_url: str, dest_connection_url: str
):
    schema_rand_prefix = f"testschema_delete_insert_timerange_{get_random_string(5)}"
    source_engine = sqlalchemy.create_engine(source_connection_url)
    with source_engine.begin() as source_conn:
        source_conn.exec_driver_sql(f"DROP SCHEMA IF EXISTS {schema_rand_prefix}")
        source_conn.exec_driver_sql(f"CREATE SCHEMA {schema_rand_prefix}")
        try:
            source_conn.exec_driver_sql(
                f"CREATE TABLE {schema_rand_prefix}.input (id INTEGER, val VARCHAR(20), updated_at DATETIME)"
            )
        except Exception:
            # hello postgres
            source_conn.exec_driver_sql("ROLLBACK;")
            source_conn.exec_driver_sql(f"DROP SCHEMA IF EXISTS {schema_rand_prefix}")
            source_conn.exec_driver_sql(f"CREATE SCHEMA {schema_rand_prefix}")
            source_conn.exec_driver_sql(
                f"CREATE TABLE {schema_rand_prefix}.input (id INTEGER, val VARCHAR(20), updated_at TIMESTAMP)"
            )

        source_conn.exec_driver_sql(
            f"""INSERT INTO {schema_rand_prefix}.input VALUES 
            (1, 'val1', '2022-01-01T00:00:00'),
            (2, 'val2', '2022-01-01T00:00:00'),
            (3, 'val3', '2022-01-02T00:00:00'),
            (4, 'val4', '2022-01-02T00:00:00'),
            (5, 'val5', '2022-01-03T00:00:00'),
            (6, 'val6', '2022-01-03T00:00:00')
        """
        )

        res = source_conn.exec_driver_sql(
            f"select count(*) from {schema_rand_prefix}.input"
        ).fetchall()
    assert res[0][0] == 6
    source_engine.dispose()

    def run(start_date: str, end_date: str):
        res = invoke_ingest_command(
            source_connection_url,
            f"{schema_rand_prefix}.input",
            dest_connection_url,
            f"{schema_rand_prefix}.output",
            inc_strategy="delete+insert",
            inc_key="updated_at",
            interval_start=start_date,
            interval_end=end_date,
            sql_backend="sqlalchemy",
        )
        assert res.exit_code == 0
        return res

    run("2022-01-01", "2022-01-02")  # dlt runs them with the end date exclusive

    dest_engine = sqlalchemy.create_engine(dest_connection_url, poolclass=NullPool)

    def get_output_rows():
        with dest_engine.connect() as dest_conn:
            if "clickhouse" not in dest_connection_url:
                dest_conn.exec_driver_sql("CHECKPOINT")
            rows = dest_conn.exec_driver_sql(
                f"select id, val, updated_at from {schema_rand_prefix}.output order by id asc"
            ).fetchall()
            return [(row[0], row[1], row[2].date()) for row in rows]

    def assert_output_equals(expected):
        res = get_output_rows()
        assert len(res) == len(expected)
        for i, row in enumerate(expected):
            assert res[i] == row

    assert_output_equals(
        [
            (1, "val1", as_datetime("2022-01-01")),
            (2, "val2", as_datetime("2022-01-01")),
            (3, "val3", as_datetime("2022-01-02")),
            (4, "val4", as_datetime("2022-01-02")),
        ]
    )

    with dest_engine.connect() as dest_conn:
        first_run_id = dest_conn.exec_driver_sql(
            f"select _dlt_load_id from {schema_rand_prefix}.output limit 1"
        ).fetchall()[0][0]

    ##############################
    # we'll run again, since this is a delete+insert, we expect the run ID to change for the last one
    res = run("2022-01-01", "2022-01-02")

    assert_output_equals(
        [
            (1, "val1", as_datetime("2022-01-01")),
            (2, "val2", as_datetime("2022-01-01")),
            (3, "val3", as_datetime("2022-01-02")),
            (4, "val4", as_datetime("2022-01-02")),
        ]
    )

    # both rows should have a new run ID
    with dest_engine.connect() as dest_conn:
        count_by_run_id = dest_conn.exec_driver_sql(
            f"select _dlt_load_id, count(*) from {schema_rand_prefix}.output group by 1 order by 1 asc"
        ).fetchall()
    assert len(count_by_run_id) == 1
    assert count_by_run_id[0][0] != first_run_id
    assert count_by_run_id[0][1] == 4
    ##############################

    ##############################
    # now run for the day after, new rows should land
    run("2022-01-02", "2022-01-03")
    assert_output_equals(
        [
            (1, "val1", as_datetime("2022-01-01")),
            (2, "val2", as_datetime("2022-01-01")),
            (3, "val3", as_datetime("2022-01-02")),
            (4, "val4", as_datetime("2022-01-02")),
            (5, "val5", as_datetime("2022-01-03")),
            (6, "val6", as_datetime("2022-01-03")),
        ]
    )

    # there should be 4 rows with 2 distinct run IDs
    with dest_engine.connect() as dest_conn:
        count_by_run_id = dest_conn.exec_driver_sql(
            f"select _dlt_load_id, count(*) from {schema_rand_prefix}.output group by 1 order by 1 asc"
        ).fetchall()
    assert len(count_by_run_id) == 2
    assert count_by_run_id[0][1] == 2
    assert count_by_run_id[1][1] == 4
    ##############################

    ##############################
    # let's bring in the rows for the third day
    run("2022-01-03", "2022-01-04")
    assert_output_equals(
        [
            (1, "val1", as_datetime("2022-01-01")),
            (2, "val2", as_datetime("2022-01-01")),
            (3, "val3", as_datetime("2022-01-02")),
            (4, "val4", as_datetime("2022-01-02")),
            (5, "val5", as_datetime("2022-01-03")),
            (6, "val6", as_datetime("2022-01-03")),
        ]
    )

    # there should be 6 rows with 3 distinct run IDs
    with dest_engine.connect() as dest_conn:
        count_by_run_id = dest_conn.exec_driver_sql(
            f"select _dlt_load_id, count(*) from {schema_rand_prefix}.output group by 1 order by 1 asc"
        ).fetchall()
    assert len(count_by_run_id) == 3
    assert count_by_run_id[0][1] == 2
    assert count_by_run_id[1][1] == 2
    assert count_by_run_id[2][1] == 2
    ##############################

    ##############################
    # now let's do a backfill for the first day again, the rows should be updated
    with source_engine.begin() as source_conn:
        source_conn.exec_driver_sql(
            f"UPDATE {schema_rand_prefix}.input SET val = 'val1_modified' WHERE id = 1"
        )
    source_engine.dispose()

    run("2022-01-01", "2022-01-02")
    assert_output_equals(
        [
            (1, "val1_modified", as_datetime("2022-01-01")),
            (2, "val2", as_datetime("2022-01-01")),
            (3, "val3", as_datetime("2022-01-02")),
            (4, "val4", as_datetime("2022-01-02")),
            (5, "val5", as_datetime("2022-01-03")),
            (6, "val6", as_datetime("2022-01-03")),
        ]
    )

    # there should still be 6 rows with 3 distinct run IDs
    with dest_engine.connect() as dest_conn:
        count_by_run_id = dest_conn.exec_driver_sql(
            f"select _dlt_load_id, count(*) from {schema_rand_prefix}.output group by 1 order by 1 asc"
        ).fetchall()
    assert len(count_by_run_id) == 2
    assert count_by_run_id[0][1] == 2
    assert count_by_run_id[1][1] == 4
    ##############################


def get_query_result(uri: str, query: str):
    engine = sqlalchemy.create_engine(uri, poolclass=NullPool)
    with engine.connect() as conn:
        res = conn.exec_driver_sql(query).fetchall()
    engine.dispose()
    return res


def custom_query_tests():
    def replace(source_connection_url, dest_connection_url):
        schema = f"testschema_cr_cust_{get_random_string(5)}"
        engine = sqlalchemy.create_engine(source_connection_url, poolclass=NullPool)
        with engine.begin() as conn:
            conn.exec_driver_sql(f"DROP SCHEMA IF EXISTS {schema}")
            conn.exec_driver_sql(f"CREATE SCHEMA {schema}")
            conn.exec_driver_sql(
                f"CREATE TABLE {schema}.orders (id INTEGER, name VARCHAR(255) NOT NULL, updated_at DATE)"
            )
            conn.exec_driver_sql(
                f"CREATE TABLE {schema}.order_items (id INTEGER, order_id INTEGER NOT NULL, subname VARCHAR(255) NOT NULL)"
            )
            conn.exec_driver_sql(
                f"INSERT INTO {schema}.orders (id, name, updated_at) VALUES (1, 'First Order', '2024-01-01'), (2, 'Second Order', '2024-01-01'), (3, 'Third Order', '2024-01-01'), (4, 'Fourth Order', '2024-01-01')"
            )
            conn.exec_driver_sql(
                f"INSERT INTO {schema}.order_items (id, order_id, subname) VALUES (1, 1, 'Item 1 for First Order'), (2, 1, 'Item 2 for First Order'), (3, 2, 'Item 1 for Second Order'), (4, 3, 'Item 1 for Third Order')"
            )
            res = conn.exec_driver_sql(
                f"select count(*) from {schema}.orders"
            ).fetchall()
            assert res[0][0] == 4
            res = conn.exec_driver_sql(
                f"select count(*) from {schema}.order_items"
            ).fetchall()
            assert res[0][0] == 4
        engine.dispose()

        if dest_connection_url.startswith("clickhouse"):
            get_query_result(
                dest_connection_url, f"CREATE DATABASE IF NOT EXISTS {schema}"
            )

        result = invoke_ingest_command(
            source_connection_url,
            f"query:select oi.*, o.updated_at from {schema}.order_items oi join {schema}.orders o on oi.order_id = o.id",
            dest_connection_url,
            f"{schema}.output",
            run_in_subprocess=True,
        )

        assert result.exit_code == 0

        res = get_query_result(
            dest_connection_url,
            f"select id, order_id, subname, updated_at from {schema}.output order by id asc",
        )

        assert len(res) == 4
        assert res[0] == (1, 1, "Item 1 for First Order", as_datetime("2024-01-01"))
        assert res[1] == (2, 1, "Item 2 for First Order", as_datetime("2024-01-01"))
        assert res[2] == (3, 2, "Item 1 for Second Order", as_datetime("2024-01-01"))
        assert res[3] == (4, 3, "Item 1 for Third Order", as_datetime("2024-01-01"))

    def merge(source_connection_url, dest_connection_url):
        schema = f"testschema_merge_cust_{get_random_string(5)}"
        source_engine = sqlalchemy.create_engine(
            source_connection_url, poolclass=NullPool
        )
        with source_engine.begin() as conn:
            conn.exec_driver_sql(f"DROP SCHEMA IF EXISTS {schema}")
            conn.exec_driver_sql(f"CREATE SCHEMA {schema}")
            conn.exec_driver_sql(
                f"CREATE TABLE {schema}.orders (id INTEGER, name VARCHAR(255) NOT NULL, updated_at DATE)"
            )
            conn.exec_driver_sql(
                f"CREATE TABLE {schema}.order_items (id INTEGER, order_id INTEGER NOT NULL, subname VARCHAR(255) NOT NULL)"
            )
            conn.exec_driver_sql(
                f"INSERT INTO {schema}.orders (id, name, updated_at) VALUES (1, 'First Order', '2024-01-01'), (2, 'Second Order', '2024-01-01'), (3, 'Third Order', '2024-01-01'), (4, 'Fourth Order', '2024-01-01')"
            )
            conn.exec_driver_sql(
                f"INSERT INTO {schema}.order_items (id, order_id, subname) VALUES (1, 1, 'Item 1 for First Order'), (2, 1, 'Item 2 for First Order'), (3, 2, 'Item 1 for Second Order'), (4, 3, 'Item 1 for Third Order')"
            )
        source_engine.dispose()

        if dest_connection_url.startswith("clickhouse"):
            get_query_result(
                dest_connection_url, f"CREATE DATABASE IF NOT EXISTS {schema}"
            )

        def run():
            result = invoke_ingest_command(
                source_connection_url,
                f"query:select oi.*, o.updated_at from {schema}.order_items oi join {schema}.orders o on oi.order_id = o.id where o.updated_at > :interval_start",
                dest_connection_url,
                f"{schema}.output",
                inc_strategy="merge",
                inc_key="updated_at",
                primary_key="id",
                run_in_subprocess=True,
            )
            assert result.exit_code == 0

        # Initial run to get all data
        run()

        res = get_query_result(
            dest_connection_url,
            f"select id, order_id, subname, updated_at, _dlt_load_id from {schema}.output order by id asc",
        )

        assert len(res) == 4
        initial_load_id = res[0][4]
        assert all(r[4] == initial_load_id for r in res)
        assert res[0] == (
            1,
            1,
            "Item 1 for First Order",
            as_datetime("2024-01-01"),
            initial_load_id,
        )
        assert res[1] == (
            2,
            1,
            "Item 2 for First Order",
            as_datetime("2024-01-01"),
            initial_load_id,
        )
        assert res[2] == (
            3,
            2,
            "Item 1 for Second Order",
            as_datetime("2024-01-01"),
            initial_load_id,
        )
        assert res[3] == (
            4,
            3,
            "Item 1 for Third Order",
            as_datetime("2024-01-01"),
            initial_load_id,
        )

        # Run again - should get same load_id since no changes
        run()
        res = get_query_result(
            dest_connection_url,
            f"select id, order_id, subname, updated_at, _dlt_load_id from {schema}.output order by id asc",
        )
        assert len(res) == 4
        assert all(r[4] == initial_load_id for r in res)

        # Update an order item and its order's updated_at
        with source_engine.begin() as conn:
            conn.exec_driver_sql(
                f"UPDATE {schema}.order_items SET subname = 'Item 1 for Second Order - new' WHERE id = 3"
            )
            conn.exec_driver_sql(
                f"UPDATE {schema}.orders SET updated_at = '2024-01-02' WHERE id = 2"
            )
        source_engine.dispose()

        # Run again - should see updated data with new load_id
        run()
        res = get_query_result(
            dest_connection_url,
            f"select id, order_id, subname, updated_at, _dlt_load_id from {schema}.output order by id asc",
        )

        assert len(res) == 4
        assert res[0] == (
            1,
            1,
            "Item 1 for First Order",
            as_datetime("2024-01-01"),
            res[0][4],
        )
        assert res[1] == (
            2,
            1,
            "Item 2 for First Order",
            as_datetime("2024-01-01"),
            res[1][4],
        )
        assert res[2] == (
            3,
            2,
            "Item 1 for Second Order - new",
            as_datetime("2024-01-02"),
            res[2][4],
        )
        assert res[3] == (
            4,
            3,
            "Item 1 for Third Order",
            as_datetime("2024-01-01"),
            res[3][4],
        )

    return [
        replace,
        merge,
    ]
