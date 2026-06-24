import os
from typing import cast

import pendulum
import pytest
import sqlalchemy

from omniload.source.appsflyer.client import (
    exclude_metrics_for_date_range,
    standardize_keys,
)
from tests.util import invoke_ingest_command
from tests.util.common import get_random_string
from tests.warehouse.settings import DESTINATIONS


def appsflyer_test_cases():
    source_uri = "appsflyer://?api_key=" + os.environ.get(
        "OMNILOAD_TEST_APPSFLYER_TOKEN", ""
    )

    def creatives(dest_uri: str):
        schema_rand_prefix = f"testschema_appsflyer_{get_random_string(5)}"
        result = invoke_ingest_command(
            source_uri,
            "creatives",
            dest_uri,
            f"{schema_rand_prefix}.creatives",
            interval_start="2025-04-01",
            interval_end="2025-04-15",
            print_output=False,
        )
        assert result.exit_code == 0

        engine = sqlalchemy.create_engine(dest_uri)
        with engine.begin() as conn:
            res = conn.exec_driver_sql(
                f"select * from {schema_rand_prefix}.creatives"
            ).fetchall()
            columns = [
                col[0]
                for col in conn.exec_driver_sql(  # ty: ignore[unresolved-attribute, unused-ignore-comment, unused-ignore-comment]
                    f"select * from {schema_rand_prefix}.creatives limit 0"
                ).cursor.description
            ]
        engine.dispose()

        assert len(res) > 0
        expected_columns = [
            "_dlt_load_id",
            "_dlt_id",
            "campaign",
            "geo",
            "app_id",
            "install_time",
            "adset_id",
            "adset",
            "ad_id",
            "impressions",
            "clicks",
            "installs",
            "cost",
            "revenue",
            "average_ecpi",
            "loyal_users",
            "uninstalls",
            "roi",
        ]
        assert sorted(columns) == sorted(expected_columns)

    def campaigns(dest_uri: str):
        schema_rand_prefix = f"testschema_appsflyer_{get_random_string(5)}"
        result = invoke_ingest_command(
            source_uri,
            "campaigns",
            dest_uri,
            f"{schema_rand_prefix}.campaigns",
            interval_start="2025-04-01",
            interval_end="2025-04-15",
            print_output=False,
        )
        assert result.exit_code == 0

        engine = sqlalchemy.create_engine(dest_uri)
        with engine.begin() as conn:
            res = conn.exec_driver_sql(
                f"select * from {schema_rand_prefix}.campaigns"
            ).fetchall()
            columns = [
                col[0]
                for col in conn.exec_driver_sql(  # ty: ignore[unresolved-attribute, unused-ignore-comment, unused-ignore-comment]
                    f"select * from {schema_rand_prefix}.campaigns limit 0"
                ).cursor.description
            ]
        engine.dispose()

        assert len(res) > 0
        expected_columns = [
            "_dlt_load_id",
            "_dlt_id",
            "campaign",
            "geo",
            "app_id",
            "install_time",
            "impressions",
            "clicks",
            "installs",
            "cost",
            "revenue",
            "average_ecpi",
            "loyal_users",
            "uninstalls",
            "roi",
            "cohort_day_14_revenue_per_user",
            "cohort_day_14_total_revenue_per_user",
            "cohort_day_1_revenue_per_user",
            "cohort_day_1_total_revenue_per_user",
            "cohort_day_21_revenue_per_user",
            "cohort_day_21_total_revenue_per_user",
            "cohort_day_3_revenue_per_user",
            "cohort_day_3_total_revenue_per_user",
            "cohort_day_7_revenue_per_user",
            "cohort_day_7_total_revenue_per_user",
            "retention_day_7",
        ]
        assert sorted(columns) == sorted(expected_columns)

    def custom(dest_uri: str):
        schema_rand_prefix = f"testschema_appsflyer_{get_random_string(5)}"
        result = invoke_ingest_command(
            source_uri,
            "custom:c,geo,app_id,install_time:impressions,clicks,installs,cost,revenue,average_ecpi,loyal_users",
            dest_uri,
            f"{schema_rand_prefix}.custom",
            interval_start="2025-04-01",
            interval_end="2025-04-15",
            print_output=False,
        )
        assert result.exit_code == 0

        engine = sqlalchemy.create_engine(dest_uri)
        with engine.connect() as conn:
            res = conn.exec_driver_sql(
                f"select * from {schema_rand_prefix}.custom"
            ).fetchall()
            columns = [
                col[0]
                for col in conn.exec_driver_sql(  # ty: ignore[unresolved-attribute, unused-ignore-comment, unused-ignore-comment]
                    f"select * from {schema_rand_prefix}.custom limit 0"
                ).cursor.description
            ]
        engine.dispose()

        assert len(res) > 0
        expected_columns = [
            "_dlt_load_id",
            "_dlt_id",
            "campaign",
            "geo",
            "app_id",
            "install_time",
            "impressions",
            "clicks",
            "installs",
            "cost",
            "revenue",
            "average_ecpi",
            "loyal_users",
        ]
        assert sorted(columns) == sorted(expected_columns)

    return [campaigns, creatives, custom]


@pytest.mark.skipif(
    not os.environ.get("OMNILOAD_TEST_APPSFLYER_TOKEN"),
    reason="OMNILOAD_TEST_APPSFLYER_TOKEN environment variable is not set",
)
@pytest.mark.parametrize("testcase", appsflyer_test_cases())
@pytest.mark.parametrize(
    "dest", list(DESTINATIONS.values()), ids=list(DESTINATIONS.keys())
)
def test_appsflyer_source(testcase, dest):
    testcase(dest.start())
    dest.stop()


def test_exclude_metrics_for_date_range():
    metrics = [
        "cohort_day_1_revenue_per_user",
        "cohort_day_1_total_revenue_per_user",
        "cohort_day_3_revenue_per_user",
        "cohort_day_3_total_revenue_per_user",
    ]

    from_date = "2024-01-01"
    to_date = "2024-01-11"
    now = "2024-01-12"

    with pendulum.travel_to(cast(pendulum.DateTime, pendulum.parse(now))):  # ty: ignore[invalid-context-manager]
        excluded_metrics = exclude_metrics_for_date_range(metrics, from_date, to_date)
        assert excluded_metrics == [
            "cohort_day_1_revenue_per_user",
            "cohort_day_1_total_revenue_per_user",
            "cohort_day_3_revenue_per_user",
            "cohort_day_3_total_revenue_per_user",
        ]


def test_standardize_keys():
    data = [
        {
            "Key One": 100,
            "Key Two": 1000,
        },
        {
            "Key One": 200,
            "Key Two": 2000,
            "cohort_day_1_revenue_per_user": 200,
        },
    ]

    excluded_metrics = ["Key Three"]

    standardized = standardize_keys(data, excluded_metrics)
    assert standardized == [
        {"key_one": 100, "key_two": 1000, "key_three": None},
        {
            "key_one": 200,
            "key_two": 2000,
            "key_three": None,
            "cohort_day_1_revenue_per_user": 200,
        },
    ]
