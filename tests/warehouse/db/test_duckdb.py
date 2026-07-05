import csv
import os
import tempfile

import duckdb
import sqlalchemy
from sqlalchemy.pool import NullPool

from tests.util import invoke_ingest_command
from tests.util.common import get_random_string
from tests.util.container.impl.duckdb import EphemeralDuckDb
from tests.warehouse.db.util import assert_output_equals_to_csv


def test_create_replace_csv_to_duckdb(testdata_path, tmp_path):

    abs_db_path = tmp_path / "test_create_replace_csv.db"

    result = invoke_ingest_command(
        "csv://omniload/testdata/create_replace.csv",
        "testschema.input",
        f"duckdb:///{abs_db_path}",
        "testschema.output",
    )
    assert result.exit_code == 0

    conn = duckdb.connect(abs_db_path)
    res = conn.sql(
        "select symbol, date, is_enabled, name from testschema.output "
        "order by symbol, date, name"
    ).fetchall()
    conn.close()

    # read CSV file
    actual_rows = []
    with open(testdata_path / "create_replace.csv", "r") as f:
        reader = csv.reader(f, delimiter=",", quotechar='"')
        next(reader, None)
        for row in reader:
            actual_rows.append([None if v.strip() == "" else v for v in row])

    # compare the CSV file with the DuckDB table
    assert len(res) == len(actual_rows)
    for i, row in enumerate(actual_rows):
        assert res[i] == tuple(row)


def test_merge_with_primary_key_csv_to_duckdb(testdata_path, tmp_path):

    abs_db_path = tmp_path / "test_merge_with_primary_key_csv.db"
    uri = f"duckdb:///{abs_db_path}"

    # DuckDB is sensitive about multiple connections to the same database file.
    # Connection Error: Can't open a connection to same database file with a
    # different configuration than existing connections
    # conn = duckdb.connect(abs_db_path)

    def run(source: str):
        res = invoke_ingest_command(
            source,
            "whatever",  # table name doesnt matter for CSV
            uri,
            "testschema_merge.output",
            "merge",
            "date",
            "symbol",
        )
        assert res.exit_code == 0
        return res

    def get_output_rows():
        conn = duckdb.connect(abs_db_path)
        conn.execute("CHECKPOINT")
        results = conn.sql(
            "select symbol, date, is_enabled, name from testschema_merge.output order by symbol asc"
        ).fetchall()
        conn.close()
        return results

    run("csv://omniload/testdata/merge_part1.csv")
    assert_output_equals_to_csv(get_output_rows(), testdata_path / "merge_part1.csv")

    conn = duckdb.connect(abs_db_path)
    first_run_id = conn.sql(
        "select _dlt_load_id from testschema_merge.output limit 1"
    ).fetchall()[0][0]
    conn.close()

    ##############################
    # we'll run again, we don't expect any changes since the data hasn't changed
    run("csv://omniload/testdata/merge_part1.csv")
    assert_output_equals_to_csv(get_output_rows(), testdata_path / "merge_part1.csv")

    # we also ensure that the other rows were not touched
    conn = duckdb.connect(abs_db_path)
    count_by_run_id = conn.sql(
        "select _dlt_load_id, count(*) from testschema_merge.output group by 1"
    ).fetchall()
    conn.close()
    assert len(count_by_run_id) == 1
    assert count_by_run_id[0][1] == 3
    assert count_by_run_id[0][0] == first_run_id
    ##############################

    ##############################
    # now we'll run the same ingestion but with a different file this time

    run("csv://omniload/testdata/merge_part2.csv")
    assert_output_equals_to_csv(get_output_rows(), testdata_path / "merge_expected.csv")

    # let's check the runs
    conn = duckdb.connect(abs_db_path)
    count_by_run_id = conn.sql(
        "select _dlt_load_id, count(*) from testschema_merge.output group by 1 order by 1 asc"
    ).fetchall()
    conn.close()

    # we expect that there's a new load ID now
    assert len(count_by_run_id) == 2

    # there should be only one row with the first load ID
    assert count_by_run_id[0][1] == 1
    assert count_by_run_id[0][0] == first_run_id

    # there should be a new run with the rest, 2 rows updated + 1 new row
    assert count_by_run_id[1][1] == 3


def test_delete_insert_without_primary_key_csv_to_duckdb(testdata_path, tmp_path):

    abs_db_path = tmp_path / "test_delete_insert_without_primary_key_csv.db"
    uri = f"duckdb:///{abs_db_path}"

    conn = duckdb.connect(abs_db_path)

    def run(source: str):
        res = invoke_ingest_command(
            source,
            "whatever",  # table name doesnt matter for CSV
            uri,
            "testschema.output",
            "delete+insert",
            "date",
        )
        assert res.exit_code == 0
        return res

    def get_output_rows():
        conn.execute("CHECKPOINT")
        return conn.sql(
            "select symbol, date, is_enabled, name from testschema.output order by symbol asc"
        ).fetchall()

    run("csv://omniload/testdata/delete_insert_part1.csv")
    assert_output_equals_to_csv(
        get_output_rows(), testdata_path / "delete_insert_part1.csv"
    )

    first_run_id = conn.sql(
        "select _dlt_load_id from testschema.output limit 1"
    ).fetchall()[0][0]

    ##############################
    # we'll run again, we expect the data to be the same, but a new load_id to exist
    # this is due to the fact that the old data won't be touched, but the ones with the
    # latest value will be rewritten
    run("csv://omniload/testdata/delete_insert_part1.csv")
    assert_output_equals_to_csv(
        get_output_rows(), testdata_path / "delete_insert_part1.csv"
    )

    # we also ensure that the other rows were not touched
    count_by_run_id = conn.sql(
        "select _dlt_load_id, count(*) from testschema.output group by 1 order by 1 asc"
    ).fetchall()

    assert len(count_by_run_id) == 2
    assert count_by_run_id[0][1] == 1
    assert count_by_run_id[0][0] == first_run_id
    assert count_by_run_id[1][1] == 3
    ##############################

    ##############################
    # now we'll run the same ingestion but with a different file this time

    run("csv://omniload/testdata/delete_insert_part2.csv")
    assert_output_equals_to_csv(
        get_output_rows(), testdata_path / "delete_insert_expected.csv"
    )

    # let's check the runs
    count_by_run_id = conn.sql(
        "select _dlt_load_id, count(*) from testschema.output group by 1 order by 1 asc"
    ).fetchall()

    # we expect that there's a new load ID now
    assert len(count_by_run_id) == 2

    # there should be only one row with the first load ID, oldest date
    assert count_by_run_id[0][1] == 1
    assert count_by_run_id[0][0] == first_run_id

    # there should be a new run with the rest, 3 rows updated + 1 new row
    assert count_by_run_id[1][1] == 4


def test_duckdb_masking_basic():
    """
    Test basic masking functionality with DuckDB source and destination.
    Tests hash, partial, redact, and round masking algorithms.
    """
    source_instance = EphemeralDuckDb()
    dest_instance = EphemeralDuckDb()

    source_uri = source_instance.start()
    dest_uri = dest_instance.start()

    schema_rand_prefix = f"test_masking_{get_random_string(5)}"
    source_engine = sqlalchemy.create_engine(source_uri, poolclass=NullPool)

    # Create test data with sensitive information
    with source_engine.begin() as conn:
        conn.exec_driver_sql(f"DROP SCHEMA IF EXISTS {schema_rand_prefix}")
        conn.exec_driver_sql(f"CREATE SCHEMA {schema_rand_prefix}")
        conn.exec_driver_sql(
            f"""CREATE TABLE {schema_rand_prefix}.customers (
                id INTEGER,
                name VARCHAR(100),
                email VARCHAR(100),
                phone VARCHAR(20),
                ssn VARCHAR(15),
                salary INTEGER,
                created_date DATE
            )"""
        )
        conn.exec_driver_sql(
            f"""INSERT INTO {schema_rand_prefix}.customers VALUES
                (1, 'John Doe', 'john.doe@example.com', '555-123-4567', '123-45-6789', 52300, '2024-01-15'),
                (2, 'Jane Smith', 'jane.smith@gmail.com', '555-987-6543', '987-65-4321', 67800, '2024-02-20'),
                (3, 'Bob Johnson', 'bob.j@company.org', '555-555-1234', '456-78-9012', 45000, '2024-03-10')
            """
        )
    source_engine.dispose()

    # Run ingestion with masking
    result = invoke_ingest_command(
        source_uri,
        f"{schema_rand_prefix}.customers",
        dest_uri,
        f"{schema_rand_prefix}.masked_customers",
        mask=["email:hash", "phone:partial:3", "ssn:redact", "salary:round:5000"],
    )

    assert result.exit_code == 0

    # Verify masked data
    dest_engine = sqlalchemy.create_engine(dest_uri, poolclass=NullPool)
    with dest_engine.connect() as dest_conn:
        res = dest_conn.exec_driver_sql(
            f"SELECT id, name, email, phone, ssn, salary FROM {schema_rand_prefix}.masked_customers ORDER BY id"
        ).fetchall()
    dest_engine.dispose()

    # Check that data was masked correctly
    assert len(res) == 3

    # First row checks
    assert res[0][0] == 1  # id unchanged
    assert res[0][1] == "John Doe"  # name unchanged
    assert len(res[0][2]) == 64  # email should be SHA-256 hash (64 chars)
    assert res[0][3] == "555******567"  # phone partially masked
    assert res[0][4] == "REDACTED"  # SSN redacted
    assert res[0][5] == 50000  # salary rounded to nearest 5000

    # Second row checks
    assert res[1][0] == 2
    assert res[1][1] == "Jane Smith"
    assert len(res[1][2]) == 64  # email hash
    assert res[1][3] == "555******543"
    assert res[1][4] == "REDACTED"
    assert res[1][5] == 70000  # 67800 -> 70000

    # Third row checks
    assert res[2][0] == 3
    assert res[2][1] == "Bob Johnson"
    assert len(res[2][2]) == 64
    assert res[2][3] == "555******234"
    assert res[2][4] == "REDACTED"
    assert res[2][5] == 45000  # 45000 -> 45000 (already rounded)

    source_instance.stop()
    dest_instance.stop()


def test_duckdb_masking_consistency():
    """
    Test that hash masking produces consistent results across multiple runs.
    """
    source_instance = EphemeralDuckDb()
    dest_instance1 = EphemeralDuckDb()
    dest_instance2 = EphemeralDuckDb()

    source_uri = source_instance.start()
    dest_uri1 = dest_instance1.start()
    dest_uri2 = dest_instance2.start()

    schema_rand_prefix = f"test_mask_consistency_{get_random_string(5)}"
    source_engine = sqlalchemy.create_engine(source_uri, poolclass=NullPool)

    # Create test data
    with source_engine.begin() as conn:
        conn.exec_driver_sql(f"DROP SCHEMA IF EXISTS {schema_rand_prefix}")
        conn.exec_driver_sql(f"CREATE SCHEMA {schema_rand_prefix}")
        conn.exec_driver_sql(
            f"""CREATE TABLE {schema_rand_prefix}.users (
                id INTEGER,
                username VARCHAR(100),
                email VARCHAR(100)
            )"""
        )
        conn.exec_driver_sql(
            f"""INSERT INTO {schema_rand_prefix}.users VALUES
                (1, 'user1', 'user1@example.com'),
                (2, 'user2', 'user2@example.com')
            """
        )
    source_engine.dispose()

    # Run first ingestion with masking
    result1 = invoke_ingest_command(
        source_uri,
        f"{schema_rand_prefix}.users",
        dest_uri1,
        f"{schema_rand_prefix}.masked_users",
        mask=["email:hash", "username:hash"],
    )
    assert result1.exit_code == 0

    # Run second ingestion with same masking
    result2 = invoke_ingest_command(
        source_uri,
        f"{schema_rand_prefix}.users",
        dest_uri2,
        f"{schema_rand_prefix}.masked_users",
        mask=["email:hash", "username:hash"],
    )
    assert result2.exit_code == 0

    # Get results from both destinations
    dest_engine1 = sqlalchemy.create_engine(dest_uri1, poolclass=NullPool)
    with dest_engine1.connect() as conn:
        res1 = conn.exec_driver_sql(
            f"SELECT id, username, email FROM {schema_rand_prefix}.masked_users ORDER BY id"
        ).fetchall()
    dest_engine1.dispose()

    dest_engine2 = sqlalchemy.create_engine(dest_uri2, poolclass=NullPool)
    with dest_engine2.connect() as conn:
        res2 = conn.exec_driver_sql(
            f"SELECT id, username, email FROM {schema_rand_prefix}.masked_users ORDER BY id"
        ).fetchall()
    dest_engine2.dispose()

    # Check that hashes are consistent between runs
    assert res1 == res2

    # Verify hashes are different from original values
    assert res1[0][1] != "user1"
    assert res1[0][2] != "user1@example.com"
    assert len(res1[0][1]) == 64  # SHA-256 hash
    assert len(res1[0][2]) == 64

    source_instance.stop()
    dest_instance1.stop()
    dest_instance2.stop()


def test_duckdb_masking_format_preserving():
    """
    Test format-preserving masking algorithms.
    """
    source_instance = EphemeralDuckDb()
    dest_instance = EphemeralDuckDb()

    source_uri = source_instance.start()
    dest_uri = dest_instance.start()

    schema_rand_prefix = f"test_format_masking_{get_random_string(5)}"
    source_engine = sqlalchemy.create_engine(source_uri, poolclass=NullPool)

    # Create test data
    with source_engine.begin() as conn:
        conn.exec_driver_sql(f"DROP SCHEMA IF EXISTS {schema_rand_prefix}")
        conn.exec_driver_sql(f"CREATE SCHEMA {schema_rand_prefix}")
        conn.exec_driver_sql(
            f"""CREATE TABLE {schema_rand_prefix}.contacts (
                id INTEGER,
                email VARCHAR(100),
                phone VARCHAR(20),
                credit_card VARCHAR(20),
                ssn VARCHAR(15),
                name VARCHAR(100)
            )"""
        )
        conn.exec_driver_sql(
            f"""INSERT INTO {schema_rand_prefix}.contacts VALUES
                (1, 'alice@example.com', '555-123-4567', '4111-1111-1111-1111', '123-45-6789', 'Alice Brown'),
                (2, 'bob@company.org', '555-987-6543', '5500-0000-0000-0004', '987-65-4321', 'Bob Smith')
            """
        )
    source_engine.dispose()

    # Run ingestion with format-preserving masks
    result = invoke_ingest_command(
        source_uri,
        f"{schema_rand_prefix}.contacts",
        dest_uri,
        f"{schema_rand_prefix}.masked_contacts",
        mask=[
            "email:email",
            "phone:phone",
            "credit_card:credit_card",
            "ssn:ssn",
            "name:first_letter",
        ],
    )

    assert result.exit_code == 0

    # Verify masked data
    dest_engine = sqlalchemy.create_engine(dest_uri, poolclass=NullPool)
    with dest_engine.connect() as dest_conn:
        res = dest_conn.exec_driver_sql(
            f"SELECT id, email, phone, credit_card, ssn, name FROM {schema_rand_prefix}.masked_contacts ORDER BY id"
        ).fetchall()
    dest_engine.dispose()

    # Check format-preserving masks
    assert len(res) == 2

    # Email masking - preserves domain (column 1)
    assert "@example.com" in res[0][1]
    assert "@company.org" in res[1][1]
    assert res[0][1] != "alice@example.com"
    assert res[1][1] != "bob@company.org"

    # Phone masking - shows area code and last digits (column 2)
    assert res[0][2].startswith("555")
    assert "***" in res[0][2]

    # Credit card - shows last 4 digits only (column 3)
    assert res[0][3] == "************1111"
    assert res[1][3] == "************0004"

    # SSN - shows last 4 digits (column 4)
    assert res[0][4] == "***-**-6789"
    assert res[1][4] == "***-**-4321"

    # Name - first letter only (column 5)
    assert res[0][5] == "A**********"  # Alice Brown (11 chars -> 10 stars)
    assert res[1][5] == "B********"  # Bob Smith (9 chars -> 8 stars)

    source_instance.stop()
    dest_instance.stop()


def test_duckdb_masking_numeric_and_date():
    """
    Test numeric masking algorithms.
    """
    source_instance = EphemeralDuckDb()
    dest_instance = EphemeralDuckDb()

    source_uri = source_instance.start()
    dest_uri = dest_instance.start()

    schema_rand_prefix = f"test_numeric_masking_{get_random_string(5)}"
    source_engine = sqlalchemy.create_engine(source_uri, poolclass=NullPool)

    # Create test data
    with source_engine.begin() as conn:
        conn.exec_driver_sql(f"DROP SCHEMA IF EXISTS {schema_rand_prefix}")
        conn.exec_driver_sql(f"CREATE SCHEMA {schema_rand_prefix}")
        conn.exec_driver_sql(
            f"""CREATE TABLE {schema_rand_prefix}.transactions (
                id INTEGER,
                amount DOUBLE,
                age INTEGER,
                score INTEGER,
                notes VARCHAR(100)
            )"""
        )
        conn.exec_driver_sql(
            f"""INSERT INTO {schema_rand_prefix}.transactions VALUES
                (1, 12345.67, 34, 456, 'Transaction notes 1'),
                (2, 98765.43, 57, 789, 'Transaction notes 2'),
                (3, 5432.10, 28, 234, 'Transaction notes 3')
            """
        )
    source_engine.dispose()

    # Run ingestion with numeric masks
    result = invoke_ingest_command(
        source_uri,
        f"{schema_rand_prefix}.transactions",
        dest_uri,
        f"{schema_rand_prefix}.masked_transactions",
        mask=["amount:round:1000", "age:round:10", "score:round:100", "notes:redact"],
    )

    assert result.exit_code == 0

    # Verify masked data
    dest_engine = sqlalchemy.create_engine(dest_uri, poolclass=NullPool)
    with dest_engine.connect() as dest_conn:
        res = dest_conn.exec_driver_sql(
            f"SELECT id, amount, age, score, notes FROM {schema_rand_prefix}.masked_transactions ORDER BY id"
        ).fetchall()
    dest_engine.dispose()

    # Check numeric masks
    assert len(res) == 3

    # Round masking on amount
    assert res[0][1] == 12000  # 12345.67 -> 12000 (round to 1000)
    assert res[1][1] == 99000  # 98765.43 -> 99000
    assert res[2][1] == 5000  # 5432.10 -> 5000

    # Round masking on age
    assert res[0][2] == 30  # 34 -> 30 (round to 10)
    assert res[1][2] == 60  # 57 -> 60
    assert res[2][2] == 30  # 28 -> 30

    # Round masking on score column
    assert res[0][3] == 500  # 456 -> 500 (round to 100)
    assert res[1][3] == 800  # 789 -> 800
    assert res[2][3] == 200  # 234 -> 200

    # Notes redacted
    assert res[0][4] == "REDACTED"
    assert res[1][4] == "REDACTED"
    assert res[2][4] == "REDACTED"

    source_instance.stop()
    dest_instance.stop()


def test_csv_to_duckdb():
    """
    Smoke test to ensure that CSV destination works.
    """
    with (
        tempfile.NamedTemporaryFile("w") as duck_src,
        tempfile.NamedTemporaryFile("w") as csv_dest,
    ):
        duck_src.close()
        csv_dest.close()
        try:
            conn = duckdb.connect(duck_src.name)
            conn.sql(
                """
                CREATE SCHEMA public;
                CREATE TABLE public.testdata(name varchar, age integer);
                INSERT INTO public.testdata(name, age)
                VALUES ('Jhon', 42), ('Lisa', 21), ('Mike', 24), ('Mary', 27);
            """
            )
            conn.close()
            result = invoke_ingest_command(
                f"duckdb:///{duck_src.name}",
                "public.testdata",
                f"csv://{csv_dest.name}",
                "dataset.table",  # unused by csv dest
            )
            assert result.exit_code == 0
            with open(csv_dest.name, "r") as output:
                reader = csv.DictReader(output)
                rows = [row for row in reader]
                assert len(rows) == 4
        finally:
            os.remove(duck_src.name)
            os.remove(csv_dest.name)
