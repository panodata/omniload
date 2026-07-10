import os
import traceback
from datetime import date
from urllib.parse import quote_plus

import pytest

from tests.util import invoke_ingest_command
from tests.util.common import get_random_string
from tests.util.db import get_query_result
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
            source_uri = f"jira://{domain}?email={quote_plus(jira_email)}&api_token={quote_plus(jira_api_token)}"
            source_table = table_name
            schema_rand_prefix = f"testschema_jira_{get_random_string(5)}"
            dest_table = f"{schema_rand_prefix}.{table_name}_{get_random_string(5)}"

            result = invoke_ingest_command(
                source_uri,
                source_table,
                dest_uri,
                dest_table,
                interval_start="2020-01-01",
                interval_end=date.today().isoformat(),
                print_output=True,
            )

            if result.exit_code != 0:
                # Some Jira resources might not be accessible based on workspace permissions
                print(
                    f"Jira {table_name} test failed (likely permissions/access issue)"
                )
                traceback.print_exception(*result.exc_info)

            assert result.exit_code == 0

            res = get_query_result(dest_uri, f"select count(*) from {dest_table}")
            assert len(res) > 0, "No results"  # Verify the query returned a result
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
