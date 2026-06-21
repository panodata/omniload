import os
import traceback

import pytest
import sqlalchemy

from tests.util import get_random_string, invoke_ingest_command
from tests.warehouse.container import DESTINATIONS


def airtable_test_cases():
    def table_with_base_id(dest_uri: str):
        source_uri = "airtable://?access_token=" + os.environ.get(
            "OMNILOAD_TEST_AIRTABLE_TOKEN", ""
        )
        source_table = os.environ.get("OMNILOAD_TEST_AIRTABLE_TABLE", "")
        schema_rand_prefix = f"testschema_airtable_{get_random_string(5)}"
        dest_table = f"{schema_rand_prefix}.output_{get_random_string(5)}"
        result = invoke_ingest_command(
            source_uri,
            source_table,
            dest_uri,
            dest_table,
            print_output=False,
        )
        if result.exit_code != 0:
            traceback.print_exception(*result.exc_info)

        assert result.exit_code == 0

        engine = sqlalchemy.create_engine(dest_uri)
        with engine.connect() as conn:
            res = conn.exec_driver_sql(f"select count(*) from {dest_table}").fetchall()
        engine.dispose()

        assert len(res) > 0
        assert res[0][0] > 0

    return [table_with_base_id]


@pytest.mark.skipif(
    not os.environ.get("OMNILOAD_TEST_AIRTABLE_TOKEN")
    or not os.environ.get("OMNILOAD_TEST_AIRTABLE_TABLE"),
    reason="OMNILOAD_TEST_AIRTABLE_TOKEN and OMNILOAD_TEST_AIRTABLE_TABLE environment variables are not set",
)
@pytest.mark.parametrize("testcase", airtable_test_cases())
@pytest.mark.parametrize(
    "dest", list(DESTINATIONS.values()), ids=list(DESTINATIONS.keys())
)
def test_airtable_source(testcase, dest):
    testcase(dest.start())
    dest.stop()
