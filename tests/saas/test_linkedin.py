import os

import duckdb
import pytest

from tests.util import get_abs_path, get_random_string, invoke_ingest_command


@pytest.mark.parametrize(
    "linkedin_ads_table",
    [
        "ad_accounts",
        "ad_account_users",
        "campaign_groups",
        "campaigns",
        "creatives",
        "conversions",
        "lead_forms",
    ],
)
def test_linkedin_ads_source_full_refresh(linkedin_ads_table):
    api_key = os.environ.get("OMNILOAD_TEST_LINKEDIN_ADS_ACCESS_TOKEN")
    if not api_key:
        pytest.skip("OMNILOAD_TEST_LINKEDIN_ADS_ACCESS_TOKEN not set")

    dbname = f"test_linkedin_{linkedin_ads_table}_{get_random_string(5)}.db"
    abs_db_path = get_abs_path(f"./testdata/{dbname}")
    rel_db_path_to_command = f"omniload/testdata/{dbname}"
    uri = f"duckdb:///{rel_db_path_to_command}"

    result = invoke_ingest_command(
        f"linkedinads://?access_token={api_key}",
        linkedin_ads_table,
        uri,
        f"raw.{linkedin_ads_table}",
    )

    assert result.exit_code == 0

    conn = duckdb.connect(abs_db_path)
    result = conn.sql(f"select count(*) from raw.{linkedin_ads_table}").fetchone()
    assert result is not None, "Database result is empty"
    assert result[0] > 0, f"No records found in table raw.{linkedin_ads_table}"

    conn.close()
    try:
        os.remove(abs_db_path)
    except Exception:
        pass
