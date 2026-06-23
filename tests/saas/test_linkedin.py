import os

import duckdb
import pytest

from tests.util import invoke_ingest_command


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
def test_linkedin_ads_source_full_refresh(linkedin_ads_table, tmp_path):
    api_key = os.environ.get("OMNILOAD_TEST_LINKEDIN_ADS_ACCESS_TOKEN")
    if not api_key:
        pytest.skip("OMNILOAD_TEST_LINKEDIN_ADS_ACCESS_TOKEN not set")

    abs_db_path = tmp_path / f"test_linkedin_{linkedin_ads_table}"
    uri = f"duckdb:///{abs_db_path}"

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
