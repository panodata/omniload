import os
import traceback

import pytest
import sqlalchemy

from tests.util import invoke_ingest_command
from tests.util.common import get_random_string
from tests.warehouse.settings import DESTINATIONS


def linear_test_cases():
    # All Linear source tables
    tables = [
        "issues",
        "projects",
        "users",
        "workflow_states",
        "cycles",
        "attachments",
        "comments",
        "documents",
        "external_users",
        "initiative",
        "integrations",
        "labels",
        "organization",
        "project_updates",
        "team_memberships",
        "initiative_to_project",
        "project_milestone",
        "project_status",
    ]

    def create_table_test(table_name):
        def table_test(dest_uri: str):
            linear_api_key = os.environ.get("OMNILOAD_TEST_LINEAR_API_KEY", "")
            if not linear_api_key:
                pytest.skip(
                    "OMNILOAD_TEST_LINEAR_API_KEY environment variable is not set"
                )

            source_uri = f"linear://?api_key={linear_api_key}"
            source_table = table_name
            schema_rand_prefix = f"testschema_linear_{get_random_string(5)}"
            dest_table = f"{schema_rand_prefix}.{table_name}_{get_random_string(5)}"

            result = invoke_ingest_command(
                source_uri,
                source_table,
                dest_uri,
                dest_table,
                interval_start="2020-01-01",
                interval_end="2025-12-31",
                print_output=True,
            )

            if result.exit_code != 0:
                # Some Linear resources might not be accessible based on workspace permissions
                print(
                    f"Linear {table_name} test failed (likely permissions/access issue)"
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
            print(f"Linear {table_name} count: {count}")

            # Special validation for users table - should have at least one user
            if table_name == "users":
                assert count > 0, "Linear should have at least one user"
            engine.dispose()

        # Set function name for pytest identification
        table_test.__name__ = f"{table_name}_table"
        return table_test

    return [create_table_test(table) for table in tables]


@pytest.mark.skipif(
    not os.environ.get("OMNILOAD_TEST_LINEAR_API_KEY"),
    reason="OMNILOAD_TEST_LINEAR_API_KEY environment variable is not set",
)
@pytest.mark.parametrize("testcase", linear_test_cases())
@pytest.mark.parametrize(
    "dest", list(DESTINATIONS.values()), ids=list(DESTINATIONS.keys())
)
def test_linear_source(testcase, dest):
    testcase(dest.start())
    dest.stop()
