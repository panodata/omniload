import os
from datetime import datetime

import duckdb
import pytest

from omniload.src.stripe_analytics import generate_date_ranges
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

    # omniload provides its test data via `omniload` root folder.
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

    # omniload provides its test data via `omniload` root folder.
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


class TestGenerateDateRanges:
    """Tests for generate_date_ranges function."""

    def test_single_hour(self):
        """Test generating a single hour range."""
        start_ts = int(datetime(2024, 1, 1, 0, 0, 0).timestamp())
        end_ts = int(datetime(2024, 1, 1, 1, 0, 0).timestamp())

        ranges = list(generate_date_ranges(start_ts, end_ts))

        assert len(ranges) == 1
        assert ranges[0]["start_ts"] == start_ts
        assert ranges[0]["end_ts"] == end_ts

    def test_one_day_generates_24_hours(self):
        """Test that a full day generates 24 hourly chunks."""
        start_ts = int(datetime(2024, 1, 1, 0, 0, 0).timestamp())
        end_ts = int(datetime(2024, 1, 2, 0, 0, 0).timestamp())

        ranges = list(generate_date_ranges(start_ts, end_ts))

        assert len(ranges) == 24
        assert ranges[0]["start_ts"] == start_ts
        assert ranges[0]["end_ts"] == int(datetime(2024, 1, 1, 1, 0, 0).timestamp())
        assert ranges[-1]["end_ts"] == end_ts

    def test_partial_hour_start(self):
        """Test starting in the middle of an hour."""
        start_ts = int(datetime(2024, 1, 1, 14, 30, 0).timestamp())
        end_ts = int(datetime(2024, 1, 1, 18, 0, 0).timestamp())

        ranges = list(generate_date_ranges(start_ts, end_ts))

        # 14:30->15:00, 15:00->16:00, 16:00->17:00, 17:00->18:00 = 4 chunks
        assert len(ranges) == 4
        assert ranges[0]["start_ts"] == start_ts
        assert ranges[0]["end_ts"] == int(datetime(2024, 1, 1, 15, 0, 0).timestamp())

    def test_partial_hour_end(self):
        """Test ending in the middle of an hour."""
        start_ts = int(datetime(2024, 1, 1, 10, 0, 0).timestamp())
        end_ts = int(datetime(2024, 1, 1, 12, 45, 0).timestamp())

        ranges = list(generate_date_ranges(start_ts, end_ts))

        # 10:00->11:00, 11:00->12:00, 12:00->12:45 = 3 chunks
        assert len(ranges) == 3
        assert ranges[-1]["end_ts"] == end_ts

    def test_multiple_days(self):
        """Test multiple days generates hourly chunks."""
        start_ts = int(datetime(2024, 1, 1, 0, 0, 0).timestamp())
        end_ts = int(datetime(2024, 1, 3, 0, 0, 0).timestamp())

        ranges = list(generate_date_ranges(start_ts, end_ts))

        # 2 days = 48 hours
        assert len(ranges) == 48

    def test_empty_range(self):
        """Test when start equals end."""
        ts = int(datetime(2024, 1, 1, 0, 0, 0).timestamp())

        ranges = list(generate_date_ranges(ts, ts))

        assert len(ranges) == 0

    def test_start_after_end(self):
        """Test when start is after end returns empty."""
        start_ts = int(datetime(2024, 1, 5, 0, 0, 0).timestamp())
        end_ts = int(datetime(2024, 1, 1, 0, 0, 0).timestamp())

        ranges = list(generate_date_ranges(start_ts, end_ts))

        assert len(ranges) == 0

    def test_week_range(self):
        """Test a full week generates 168 hourly ranges."""
        start_ts = int(datetime(2024, 1, 1, 0, 0, 0).timestamp())
        end_ts = int(datetime(2024, 1, 8, 0, 0, 0).timestamp())

        ranges = list(generate_date_ranges(start_ts, end_ts))

        # 7 days * 24 hours = 168 chunks
        assert len(ranges) == 168

    def test_ranges_are_contiguous(self):
        """Test that ranges cover the entire period without gaps."""
        start_ts = int(datetime(2024, 1, 1, 6, 30, 0).timestamp())
        end_ts = int(datetime(2024, 1, 1, 18, 45, 0).timestamp())

        ranges = list(generate_date_ranges(start_ts, end_ts))

        assert ranges[0]["start_ts"] == start_ts
        assert ranges[-1]["end_ts"] == end_ts
        for i in range(len(ranges) - 1):
            assert ranges[i]["end_ts"] == ranges[i + 1]["start_ts"]

    def test_returns_dict_with_correct_keys(self):
        """Test that each yielded item has the expected keys."""
        start_ts = int(datetime(2024, 1, 1, 0, 0, 0).timestamp())
        end_ts = int(datetime(2024, 1, 1, 1, 0, 0).timestamp())

        ranges = list(generate_date_ranges(start_ts, end_ts))

        assert len(ranges) == 1
        assert "start_ts" in ranges[0]
        assert "end_ts" in ranges[0]
        assert len(ranges[0]) == 2
