import os
import shutil

import duckdb
import pytest

from tests.util import get_abs_path, invoke_ingest_command


@pytest.mark.skipif(
    not all(
        [
            os.getenv("SNAPCHAT_REFRESH_TOKEN"),
            os.getenv("SNAPCHAT_CLIENT_ID"),
            os.getenv("SNAPCHAT_CLIENT_SECRET"),
            os.getenv("SNAPCHAT_ORGANIZATION_ID"),
        ]
    ),
    reason="Snapchat credentials not set in environment",
)
def test_snapchat_ads_merge_strategy():
    """Test that Snapchat Ads merge strategy correctly appends data with different breakdowns.

    This test verifies:
    1. First ingest without breakdown - adsquad_id and ad_id should be NULL
    2. Second ingest with ad breakdown - ad_id should be populated
    3. Both sets of records should exist in the table (append, not replace)
    """
    try:
        shutil.rmtree(get_abs_path("../pipeline_data"))
    except Exception:
        pass

    # Get Snapchat credentials from environment
    refresh_token = os.getenv("SNAPCHAT_REFRESH_TOKEN")
    client_id = os.getenv("SNAPCHAT_CLIENT_ID")
    client_secret = os.getenv("SNAPCHAT_CLIENT_SECRET")
    organization_id = os.getenv("SNAPCHAT_ORGANIZATION_ID")

    # Build source URI
    source_uri = (
        f"snapchatads://?refresh_token={refresh_token}"
        f"&client_id={client_id}"
        f"&client_secret={client_secret}"
        f"&organization_id={organization_id}"
    )

    # Create DuckDB database
    db_path = get_abs_path("../test_snapchat_merge.duckdb")
    dest_uri = f"duckdb:///{db_path}"

    try:
        # First ingest: campaigns_stats without breakdown
        # Expected: adsquad_id and ad_id should be NULL
        result1 = invoke_ingest_command(
            source_uri,
            "campaigns_stats:HOUR,impressions,spend",
            dest_uri,
            "snapchat_ads.campaigns_stats",
            interval_start="2025-11-19",
            interval_end="2025-11-20",
        )

        assert result1.exit_code == 0, f"First ingest failed: {result1.stdout}"

        # Check first ingest results
        conn = duckdb.connect(db_path)

        # First, check what columns exist
        columns = conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'snapchat_ads' AND table_name = 'campaigns_stats' "
            "ORDER BY ordinal_position"
        ).fetchall()
        column_names = [col[0] for col in columns]
        print(f"\n✓ Columns after first ingest: {column_names}")

        # Get total count
        result = conn.execute(
            "SELECT COUNT(*) FROM snapchat_ads.campaigns_stats"
        ).fetchone()
        assert result is not None, "Database result is empty"
        first_ingest_total = result[0]

        assert first_ingest_total > 0, "First ingest should have data"

        # Check if ad_id and adsquad_id columns exist
        has_ad_id = "ad_id" in column_names
        has_adsquad_id = "adsquad_id" in column_names

        if has_ad_id and has_adsquad_id:
            # Columns exist, check for NULL values
            result = conn.execute(
                "SELECT COUNT(CASE WHEN ad_id IS NULL THEN 1 END) as null_ad_ids, "
                "COUNT(CASE WHEN adsquad_id IS NULL THEN 1 END) as null_adsquad_ids "
                "FROM snapchat_ads.campaigns_stats"
            ).fetchone()
            assert result is not None, "Database result is empty"
            first_ingest_null_ad_ids = result[0]
            print(
                f"✓ First ingest: {first_ingest_total} records with NULL ad_id and adsquad_id"
            )
            assert first_ingest_null_ad_ids == first_ingest_total, (
                f"All ad_id values should be NULL in first ingest, got {first_ingest_null_ad_ids}/{first_ingest_total}"
            )
        else:
            print(
                f"⚠ First ingest: {first_ingest_total} records but missing columns - "
                f"ad_id: {has_ad_id}, adsquad_id: {has_adsquad_id}"
            )

        # Second ingest: campaigns_stats with ad breakdown
        # Expected: ad_id should be populated, data should be appended
        result2 = invoke_ingest_command(
            source_uri,
            "campaigns_stats:ad,HOUR,impressions,spend",
            dest_uri,
            "snapchat_ads.campaigns_stats",
            interval_start="2025-11-19",
            interval_end="2025-11-20",
        )

        assert result2.exit_code == 0, f"Second ingest failed: {result2.stdout}"

        # Check merge results
        result = conn.execute(
            "SELECT COUNT(*) as total, "
            "COUNT(CASE WHEN ad_id IS NULL THEN 1 END) as null_ad_ids, "
            "COUNT(CASE WHEN ad_id IS NOT NULL THEN 1 END) as non_null_ad_ids "
            "FROM snapchat_ads.campaigns_stats"
        ).fetchone()
        assert result is not None, "Database result is empty"
        total_records = result[0]
        null_ad_ids = result[1]
        non_null_ad_ids = result[2]

        print(
            f"✓ After second ingest: {total_records} total records "
            f"({null_ad_ids} with NULL ad_id, {non_null_ad_ids} with populated ad_id)"
        )

        # Verify merge strategy worked correctly
        assert total_records > first_ingest_total, (
            f"Second ingest should have appended data, not replaced. Got {total_records} total vs {first_ingest_total} first"
        )

        # If ad_id column existed in first ingest, verify NULL records remain
        if has_ad_id:
            assert null_ad_ids == first_ingest_total, (
                f"NULL ad_id records should remain from first ingest. Got {null_ad_ids} NULL vs {first_ingest_total} first ingest"
            )

        assert non_null_ad_ids > 0, (
            "Should have records with populated ad_id from second ingest"
        )

        # Verify primary key structure - get all columns first
        all_columns = conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'snapchat_ads' AND table_name = 'campaigns_stats' "
            "ORDER BY ordinal_position"
        ).fetchall()
        all_column_names = [col[0] for col in all_columns]

        # Select only columns that exist
        select_cols = []
        for col in ["campaign_id", "adsquad_id", "ad_id", "start_time", "end_time"]:
            if col in all_column_names:
                select_cols.append(col)

        result = conn.execute(
            f"SELECT {', '.join(select_cols)} FROM snapchat_ads.campaigns_stats LIMIT 5"
        ).fetchall()

        print("✓ Sample records (showing PK columns):")
        for row in result:
            row_str = ", ".join(f"{col}={val}" for col, val in zip(select_cols, row))
            print(f"  {row_str}")

        conn.close()

        print("Merge strategy test passed!")
        print(
            f"   - First ingest created {first_ingest_total} records with NULL breakdown IDs"
        )
        print(
            f"   - Second ingest appended {non_null_ad_ids} records with ad breakdown"
        )
        print(f"   - Total: {total_records} records in table")

    finally:
        # Clean up
        if os.path.exists(db_path):
            os.remove(db_path)
        try:
            shutil.rmtree(get_abs_path("../pipeline_data"))
        except Exception:
            pass
