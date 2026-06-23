import os
import traceback

import pytest
import sqlalchemy

from tests.util import get_random_string, invoke_ingest_command
from tests.warehouse.settings import DESTINATIONS


def revenuecat_test_cases():
    # All RevenueCat source tables
    tables = ["projects", "customers", "products", "entitlements", "offerings"]

    def create_table_test(table_name):
        def table_test(dest_uri: str):
            revenuecat_api_key = os.environ.get("OMNILOAD_TEST_REVENUECAT_API_KEY", "")
            revenuecat_project_id = os.environ.get(
                "OMNILOAD_TEST_REVENUECAT_PROJECT_ID", ""
            )

            if not revenuecat_api_key:
                pytest.skip(
                    "OMNILOAD_TEST_REVENUECAT_API_KEY environment variable is not set"
                )

            # Projects table doesn't need project_id, others do
            if table_name != "projects" and not revenuecat_project_id:
                pytest.skip(
                    "OMNILOAD_TEST_REVENUECAT_PROJECT_ID environment variable is not set"
                )

            # Build source URI
            if table_name == "projects":
                source_uri = f"revenuecat://?api_key={revenuecat_api_key}"
            else:
                source_uri = f"revenuecat://?api_key={revenuecat_api_key}&project_id={revenuecat_project_id}"

            source_table = table_name
            schema_rand_prefix = f"testschema_revenuecat_{get_random_string(5)}"
            dest_table = f"{schema_rand_prefix}.{table_name}_{get_random_string(5)}"

            # Limit customers table to 100 records for faster testing
            yield_limit = 100 if table_name == "customers" else None

            result = invoke_ingest_command(
                source_uri,
                source_table,
                dest_uri,
                dest_table,
                yield_limit=yield_limit,
                print_output=True,
            )

            if result.exit_code != 0:
                # Some RevenueCat resources might not be accessible based on API key permissions
                print(
                    f"RevenueCat {table_name} test failed (likely permissions/access issue)"
                )
                traceback.print_exception(*result.exc_info)

            assert result.exit_code == 0

            engine = sqlalchemy.create_engine(dest_uri)
            with engine.connect() as conn:
                res = conn.exec_driver_sql(
                    f"select count(*) from {dest_table}"
                ).fetchall()
            assert len(res) > 0, "No results"
            count = res[0][0]
            print(f"RevenueCat {table_name} count: {count}")

            # Special validation for projects table - should have at least one project
            if table_name == "projects":
                assert count > 0, "RevenueCat should have at least one project"
            engine.dispose()

        # Set function name for pytest identification
        table_test.__name__ = f"{table_name}_table"
        return table_test

    return [create_table_test(table) for table in tables]


@pytest.mark.skipif(
    not os.environ.get("OMNILOAD_TEST_REVENUECAT_API_KEY"),
    reason="OMNILOAD_TEST_REVENUECAT_API_KEY environment variable is not set",
)
@pytest.mark.parametrize("testcase", revenuecat_test_cases())
@pytest.mark.parametrize(
    "dest", list(DESTINATIONS.values()), ids=list(DESTINATIONS.keys())
)
def test_revenuecat_source(testcase, dest):
    testcase(dest.start())
    dest.stop()
