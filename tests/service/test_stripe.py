import os

import duckdb
import pytest

from tests.util import get_abs_path, get_random_string, invoke_ingest_command


@pytest.mark.parametrize(
    "stripe_table",
    [
        "subscription",
        "customer",
        "product",
        "price",
        "event",
        "invoice",
        "charge",
        "balancetransaction",
    ],
)
def test_stripe_source_full_refresh(stripe_table):
    # Get Stripe token from environment
    stripe_token = os.environ.get("OMNILOAD_TEST_STRIPE_TOKEN")
    if not stripe_token:
        pytest.skip("OMNILOAD_TEST_STRIPE_TOKEN not set")

    # Create test database
    dbname = f"test_stripe_{stripe_table}{get_random_string(5)}.db"
    abs_db_path = get_abs_path(f"./testdata/{dbname}")
    rel_db_path_to_command = f"omniload/testdata/{dbname}"
    uri = f"duckdb:///{rel_db_path_to_command}"

    # Run ingest command
    result = invoke_ingest_command(
        f"stripe://{stripe_table}s?api_key={stripe_token}",
        stripe_table,
        uri,
        f"raw.{stripe_table}s",
    )

    assert result.exit_code == 0

    # Verify data was loaded
    conn = duckdb.connect(abs_db_path)
    res = conn.sql(f"select count(*) from raw.{stripe_table}s").fetchone()
    assert res, "Database result is empty"
    assert res[0] > 0, f"No {stripe_table} records found"

    # Clean up
    conn.close()
    try:
        os.remove(abs_db_path)
    except Exception:
        pass


@pytest.mark.parametrize(
    "stripe_table", ["event", "invoice", "charge", "balancetransaction"]
)
def test_stripe_source_incremental(stripe_table):
    # Get Stripe token from environment
    stripe_token = os.environ.get("OMNILOAD_TEST_STRIPE_TOKEN")
    if not stripe_token:
        pytest.skip("OMNILOAD_TEST_STRIPE_TOKEN not set")

    # Create test database
    dbname = f"test_stripe_{stripe_table}{get_random_string(5)}.db"
    abs_db_path = get_abs_path(f"./testdata/{dbname}")
    rel_db_path_to_command = f"omniload/testdata/{dbname}"
    uri = f"duckdb:///{rel_db_path_to_command}"

    # Run ingest command
    result = invoke_ingest_command(
        f"stripe://{stripe_table}s?api_key={stripe_token}",
        stripe_table,
        uri,
        f"raw.{stripe_table}s",
        interval_start="2025-04-01",
        interval_end="2025-05-30",
    )

    assert result.exit_code == 0

    # Verify data was loaded
    conn = duckdb.connect(abs_db_path)
    res = conn.sql(f"select count(*) from raw.{stripe_table}s").fetchone()
    assert res, "Database result is empty"
    assert res[0] > 0, f"No {stripe_table} records found"

    # Clean up
    conn.close()
    try:
        os.remove(abs_db_path)
    except Exception:
        pass
