import os
from datetime import date, timedelta

import duckdb
import pytest

from omniload.source.snapchat_ads.helpers import (
    parse_stats_table,
    parse_timeseries_stats,
    parse_total_stats,
)
from tests.util import invoke_ingest_command


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
def test_snapchat_ads_merge_strategy(tmp_path):
    """Test that Snapchat Ads merge strategy correctly appends data with different breakdowns.

    This test verifies:
    1. First ingest without breakdown - adsquad_id and ad_id should be NULL
    2. Second ingest with ad breakdown - ad_id should be populated
    3. Both sets of records should exist in the table (append, not replace)
    """

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
    db_path = tmp_path / "test_snapchat_merge.duckdb"
    dest_uri = f"duckdb:///{db_path}"
    interval_end = date.today() - timedelta(days=1)
    interval_start = interval_end - timedelta(days=30)

    if True:
        # First ingest: campaigns_stats without breakdown
        # Expected: adsquad_id and ad_id should be NULL
        result1 = invoke_ingest_command(
            source_uri,
            "campaigns_stats:HOUR:impressions,spend",
            dest_uri,
            "snapchat_ads.campaigns_stats",
            interval_start=interval_start.isoformat(),
            interval_end=interval_end.isoformat(),
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

        if first_ingest_total == 0:
            pytest.skip(
                f"No campaigns_stats data in {interval_start.isoformat()}..{interval_end.isoformat()}"
            )

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
            "campaigns_stats:ad,HOUR:impressions,spend",
            dest_uri,
            "snapchat_ads.campaigns_stats",
            interval_start=interval_start.isoformat(),
            interval_end=interval_end.isoformat(),
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

        assert select_cols, (
            "Expected at least one PK-like column in snapchat_ads.campaigns_stats, "
            f"found columns: {all_column_names}"
        )

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


class TestParseStatsTable:
    """Test parse_stats_table function.

    Table format: <resource-name>:<dimension-like-values>:<metrics>
    """

    def test_with_breakdown(self):
        """Test stats table parsing with breakdown parameter."""
        result = parse_stats_table("campaigns_stats:campaign,DAY:impressions,spend")
        assert result.resource_name == "campaigns_stats"
        assert result.granularity == "DAY"
        assert result.breakdown == "campaign"
        assert result.fields == "impressions,spend"
        assert result.dimension is None
        assert result.pivot is None

    def test_without_breakdown(self):
        """Test stats table parsing without breakdown parameter."""
        result = parse_stats_table("campaigns_stats:DAY:impressions,swipes,spend")
        assert result.resource_name == "campaigns_stats"
        assert result.granularity == "DAY"
        assert result.fields == "impressions,swipes,spend"
        assert result.breakdown is None

    def test_lifetime_granularity(self):
        """Test stats table parsing with LIFETIME granularity."""
        result = parse_stats_table("ads_stats:ad,LIFETIME:impressions")
        assert result.resource_name == "ads_stats"
        assert result.granularity == "LIFETIME"
        assert result.breakdown == "ad"
        assert result.fields == "impressions"

    def test_missing_granularity(self):
        """Test that missing granularity raises ValueError."""
        with pytest.raises(ValueError, match="Granularity is required"):
            parse_stats_table("campaigns_stats:campaign:impressions,spend")

    def test_order_independent(self):
        """Test that dimension-like values can be in any order."""
        # DAY before breakdown
        result1 = parse_stats_table("campaigns_stats:DAY,campaign:impressions")
        assert result1.granularity == "DAY"
        assert result1.breakdown == "campaign"

        # breakdown before DAY
        result2 = parse_stats_table("campaigns_stats:campaign,DAY:impressions")
        assert result2.granularity == "DAY"
        assert result2.breakdown == "campaign"

    def test_with_dimension_and_pivot(self):
        """Test stats table parsing with dimension and pivot parameters."""
        result = parse_stats_table(
            "campaigns_stats:campaign,DAY,GEO,country:impressions"
        )
        assert result.resource_name == "campaigns_stats"
        assert result.granularity == "DAY"
        assert result.breakdown == "campaign"
        assert result.dimension == "GEO"
        assert result.pivot == "country"
        assert result.fields == "impressions"

    def test_default_fields(self):
        """Test that default fields are used when metrics section is omitted."""
        result = parse_stats_table("campaigns_stats:DAY")
        assert result.resource_name == "campaigns_stats"
        assert result.granularity == "DAY"
        assert result.fields == "impressions,spend"  # default

    def test_unknown_parameter_raises_error(self):
        """Test that unknown parameters raise ValueError."""
        with pytest.raises(ValueError, match="Unknown parameter 'invalid'"):
            parse_stats_table("campaigns_stats:DAY,invalid:impressions")


class TestParseTimeseriesStats:
    """Test parse_timeseries_stats function."""

    def test_without_breakdown(self):
        """Test parsing timeseries stats without breakdown."""
        api_response = {
            "timeseries_stats": [
                {
                    "sub_request_status": "SUCCESS",
                    "timeseries_stat": {
                        "id": "campaign-123",
                        "type": "CAMPAIGN",
                        "timeseries": [
                            {
                                "start_time": "2024-01-01T00:00:00.000Z",
                                "end_time": "2024-01-01T01:00:00.000Z",
                                "stats": {"impressions": 100, "spend": 50},
                            }
                        ],
                    },
                }
            ]
        }

        results = list(parse_timeseries_stats(api_response))
        assert len(results) == 1
        assert results[0]["campaign_id"] == "campaign-123"
        assert results[0]["adsquad_id"] is None
        assert results[0]["ad_id"] is None
        assert results[0]["impressions"] == 100
        assert results[0]["spend"] == 50

    def test_with_ad_breakdown(self):
        """Test parsing timeseries stats with ad breakdown."""
        api_response = {
            "timeseries_stats": [
                {
                    "sub_request_status": "SUCCESS",
                    "timeseries_stat": {
                        "id": "campaign-123",
                        "type": "CAMPAIGN",
                        "breakdown_stats": {
                            "ad": [
                                {
                                    "id": "ad-456",
                                    "timeseries": [
                                        {
                                            "start_time": "2024-01-01T00:00:00.000Z",
                                            "end_time": "2024-01-01T01:00:00.000Z",
                                            "stats": {"impressions": 50, "spend": 25},
                                        }
                                    ],
                                }
                            ]
                        },
                    },
                }
            ]
        }

        results = list(parse_timeseries_stats(api_response))
        assert len(results) == 1
        assert results[0]["campaign_id"] == "campaign-123"
        assert results[0]["ad_id"] == "ad-456"
        assert results[0]["adsquad_id"] is None
        assert results[0]["impressions"] == 50
        assert results[0]["spend"] == 25

    def test_with_adsquad_breakdown(self):
        """Test parsing timeseries stats with adsquad breakdown."""
        api_response = {
            "timeseries_stats": [
                {
                    "sub_request_status": "SUCCESS",
                    "timeseries_stat": {
                        "id": "campaign-123",
                        "type": "CAMPAIGN",
                        "breakdown_stats": {
                            "adsquad": [
                                {
                                    "id": "adsquad-789",
                                    "timeseries": [
                                        {
                                            "start_time": "2024-01-01T00:00:00.000Z",
                                            "end_time": "2024-01-01T01:00:00.000Z",
                                            "stats": {"impressions": 75, "spend": 30},
                                        }
                                    ],
                                }
                            ]
                        },
                    },
                }
            ]
        }

        results = list(parse_timeseries_stats(api_response))
        assert len(results) == 1
        assert results[0]["campaign_id"] == "campaign-123"
        assert results[0]["adsquad_id"] == "adsquad-789"
        assert results[0]["ad_id"] is None
        assert results[0]["impressions"] == 75


class TestParseTotalStats:
    """Test parse_total_stats function."""

    def test_without_breakdown(self):
        """Test parsing total stats without breakdown."""
        api_response = {
            "total_stats": [
                {
                    "sub_request_status": "SUCCESS",
                    "total_stat": {
                        "id": "campaign-123",
                        "type": "CAMPAIGN",
                        "start_time": "2024-01-01T00:00:00.000Z",
                        "end_time": "2024-01-31T23:59:59.999Z",
                        "stats": {"impressions": 1000, "spend": 500},
                    },
                }
            ]
        }

        results = list(parse_total_stats(api_response))
        assert len(results) == 1
        assert results[0]["campaign_id"] == "campaign-123"
        assert results[0]["adsquad_id"] is None
        assert results[0]["ad_id"] is None
        assert results[0]["impressions"] == 1000
        assert results[0]["spend"] == 500

    def test_with_ad_breakdown(self):
        """Test parsing total stats with ad breakdown."""
        api_response = {
            "total_stats": [
                {
                    "sub_request_status": "SUCCESS",
                    "total_stat": {
                        "id": "campaign-123",
                        "type": "CAMPAIGN",
                        "start_time": "2024-01-01T00:00:00.000Z",
                        "end_time": "2024-01-31T23:59:59.999Z",
                        "breakdown_stats": {
                            "ad": [
                                {
                                    "id": "ad-456",
                                    "stats": {"impressions": 500, "spend": 250},
                                }
                            ]
                        },
                    },
                }
            ]
        }

        results = list(parse_total_stats(api_response))
        assert len(results) == 1
        assert results[0]["campaign_id"] == "campaign-123"
        assert results[0]["ad_id"] == "ad-456"
        assert results[0]["adsquad_id"] is None
        assert results[0]["impressions"] == 500
        assert results[0]["spend"] == 250
