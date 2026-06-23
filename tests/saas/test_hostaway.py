import os

import duckdb
import pytest

from tests.util import invoke_ingest_command
from tests.util.common import get_abs_path, get_random_string


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
def test_hostaway_source_full_refresh(hostaway_table):
    api_key = os.environ.get("OMNILOAD_TEST_HOSTAWAY_API_KEY")
    if not api_key:
        pytest.skip("OMNILOAD_TEST_HOSTAWAY_API_KEY not set")

    dbname = f"test_hostaway_{hostaway_table}_{get_random_string(5)}.db"
    abs_db_path = get_abs_path(f"./testdata/{dbname}")
    rel_db_path_to_command = f"omniload/testdata/{dbname}"
    uri = f"duckdb:///{rel_db_path_to_command}"

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

    conn.close()
    try:
        os.remove(abs_db_path)
    except Exception:
        pass
