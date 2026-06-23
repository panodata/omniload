import os

import duckdb
import pytest

from tests.util import invoke_ingest_command


@pytest.mark.parametrize(
    "hostaway_table",
    [
        "listings",
        "listing_fee_settings",
        "listing_pricing_settings",
        "listing_agreements",
        "cancellation_policies",
        "cancellation_policies_airbnb",
        "cancellation_policies_marriott",
        "cancellation_policies_vrbo",
        "reservations",
        "finance_fields",
        "reservation_payment_methods",
        "reservation_rental_agreements",
        "listing_calendars",
        "conversations",
        "message_templates",
        "bed_types",
        "property_types",
        "countries",
        "account_tax_settings",
        "user_groups",
        "guest_payment_charges",
        "coupons",
        "webhook_reservations",
        "tasks",
    ],
)
def test_hostaway_source_full_refresh(hostaway_table, tmp_path):
    api_key = os.environ.get("OMNILOAD_TEST_HOSTAWAY_API_KEY")
    if not api_key:
        pytest.skip("OMNILOAD_TEST_HOSTAWAY_API_KEY not set")

    abs_db_path = tmp_path / f"test_hostaway_{hostaway_table}.db"
    uri = f"duckdb:///{abs_db_path}"

    result = invoke_ingest_command(
        f"hostaway://?api_key={api_key}",
        hostaway_table,
        uri,
        f"raw.{hostaway_table}",
        interval_start="2020-01-01",
        interval_end="2025-12-31",
    )

    assert result.exit_code == 0

    conn = duckdb.connect(abs_db_path)
    result = conn.sql(f"select count(*) from raw.{hostaway_table}").fetchone()
    assert result is not None, "Database result is empty"
    assert result[0] > 0, f"No rows ingested for table {hostaway_table}"
    conn.close()
