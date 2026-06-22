import os
import traceback

import pytest
import sqlalchemy

from tests.util import get_random_string, invoke_ingest_command
from tests.warehouse.settings import DESTINATIONS


def jira_test_cases():
    # All Jira source tables
    tables = [
        "projects",
        "issues",
        "users",
        "issue_types",
        "statuses",
        "priorities",
        "resolutions",
    ]

    def create_table_test(table_name):
        def table_test(dest_uri: str):
            jira_base_url = os.environ.get("OMNILOAD_TEST_JIRA_BASE_URL", "")
            jira_email = os.environ.get("OMNILOAD_TEST_JIRA_EMAIL", "")
            jira_api_token = os.environ.get("OMNILOAD_TEST_JIRA_API_TOKEN", "")

            if not jira_base_url or not jira_email or not jira_api_token:
                pytest.skip(
                    "OMNILOAD_TEST_JIRA_BASE_URL, OMNILOAD_TEST_JIRA_EMAIL, or OMNILOAD_TEST_JIRA_API_TOKEN environment variables are not set"
                )

            # Extract domain from base_url (remove https:// if present)
            domain = jira_base_url.replace("https://", "").replace("http://", "")
            source_uri = (
                f"jira://{domain}?email={jira_email}&api_token={jira_api_token}"
            )
            source_table = table_name
            schema_rand_prefix = f"testschema_jira_{get_random_string(5)}"
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
                # Some Jira resources might not be accessible based on workspace permissions
                print(
                    f"Jira {table_name} test failed (likely permissions/access issue)"
                )
                traceback.print_exception(*result.exc_info)

            assert result.exit_code == 0

            with sqlalchemy.create_engine(dest_uri).connect() as conn:
                res = conn.exec_driver_sql(
                    f"select count(*) from {dest_table}"
                ).fetchall()
                assert len(res) >= 0  # Just verify the table exists and query works
                count = res[0][0]
                print(f"Jira {table_name} count: {count}")

                # Special validation for certain tables that should always have data
                if table_name == "projects":
                    assert count > 0, "Jira should have at least one project"
                elif table_name == "issue_types":
                    assert count > 0, "Jira should have at least one issue type"
                elif table_name == "statuses":
                    assert count > 0, "Jira should have at least one status"
                # project_versions and project_components can be empty, so no assertion for them

        # Set function name for pytest identification
        table_test.__name__ = f"{table_name}_table"
        return table_test

    return [create_table_test(table) for table in tables]


@pytest.mark.skipif(
    not all(
        [
            os.environ.get("OMNILOAD_TEST_JIRA_BASE_URL"),
            os.environ.get("OMNILOAD_TEST_JIRA_EMAIL"),
            os.environ.get("OMNILOAD_TEST_JIRA_API_TOKEN"),
        ]
    ),
    reason="OMNILOAD_TEST_JIRA_BASE_URL, OMNILOAD_TEST_JIRA_EMAIL, or OMNILOAD_TEST_JIRA_API_TOKEN environment variables are not set",
)
@pytest.mark.parametrize("testcase", jira_test_cases())
@pytest.mark.parametrize(
    "dest", list(DESTINATIONS.values()), ids=list(DESTINATIONS.keys())
)
def test_jira_source(testcase, dest):
    testcase(dest.start())
    dest.stop()
