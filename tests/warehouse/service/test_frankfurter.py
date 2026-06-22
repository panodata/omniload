import sys
from typing import Callable, Iterable

import pendulum
import pytest
import sqlalchemy

from omniload.src.errors import UnsupportedResourceError
from tests.util import get_random_string, has_exception, invoke_ingest_command
from tests.warehouse.settings import DESTINATIONS

if sys.version_info < (3, 11):
    pytest.skip("Skipping tests for Python <= 3.10", allow_module_level=True)


def frankfurter_test_cases() -> Iterable[Callable]:
    def invalid_source_table(dest_uri):
        schema = f"testschema_frankfurter_{get_random_string(5)}"
        dest_table = f"{schema}.frankfurter_{get_random_string(5)}"
        result = invoke_ingest_command(
            "frankfurter://",
            "invalid table",
            dest_uri,
            dest_table,
            print_output=False,
        )
        assert result.exit_code != 0
        assert has_exception(result.exception, UnsupportedResourceError)

    def interval_start_does_not_exceed_interval_end(dest_uri):
        schema = f"testschema_frankfurter_{get_random_string(5)}"
        dest_table = f"{schema}.frankfurter_{get_random_string(5)}"
        result = invoke_ingest_command(
            "frankfurter://",
            "exchange_rates",
            dest_uri,
            dest_table,
            interval_start="2025-04-11",
            interval_end="2025-04-10",
            print_output=False,
        )
        assert result.exit_code != 0
        assert has_exception(result.exception, ValueError)
        assert "Interval-end cannot be before interval-start." in str(result.exception)

    def interval_start_can_equal_interval_end(dest_uri):
        schema = f"testschema_frankfurter_{get_random_string(5)}"
        dest_table = f"{schema}.frankfurter_{get_random_string(5)}"
        result = invoke_ingest_command(
            "frankfurter://",
            "exchange_rates",
            dest_uri,
            dest_table,
            interval_start="2025-04-10",
            interval_end="2025-04-10",
            print_output=False,
        )
        assert result.exit_code == 0

    def interval_start_does_not_exceed_current_date(dest_uri):
        schema = f"testschema_frankfurter_{get_random_string(5)}"
        dest_table = f"{schema}.frankfurter_{get_random_string(5)}"
        start_date = pendulum.now().add(days=1).format("YYYY-MM-DD")
        result = invoke_ingest_command(
            "frankfurter://",
            "exchange_rates",
            dest_uri,
            dest_table,
            interval_start=start_date,
            print_output=False,
        )
        assert result.exit_code != 0
        assert has_exception(result.exception, ValueError)
        assert "Interval-start cannot be in the future." in str(result.exception)

    def interval_end_does_not_exceed_current_date(dest_uri):
        schema = f"testschema_frankfurter_{get_random_string(5)}"
        dest_table = f"{schema}.frankfurter_{get_random_string(5)}"
        start_date = pendulum.now().subtract(days=1).format("YYYY-MM-DD")
        end_date = pendulum.now().add(days=1).format("YYYY-MM-DD")
        result = invoke_ingest_command(
            "frankfurter://",
            "exchange_rates",
            dest_uri,
            dest_table,
            interval_start=start_date,
            interval_end=end_date,
            print_output=False,
        )
        assert result.exit_code != 0
        assert has_exception(result.exception, ValueError)
        assert "Interval-end cannot be in the future." in str(result.exception)

    def exchange_rate_on_specific_date(dest_uri):
        schema = f"testschema_frankfurter_{get_random_string(5)}"
        dest_table = f"{schema}.frankfurter_{get_random_string(5)}"
        start_date = "2025-01-03"
        end_date = "2025-01-03"
        result = invoke_ingest_command(
            "frankfurter://?base=EUR",
            "exchange_rates",
            dest_uri,
            dest_table,
            interval_start=start_date,
            interval_end=end_date,
            print_output=False,
        )
        assert result.exit_code == 0, f"Ingestion failed: {result.output}"

        dest_engine = sqlalchemy.create_engine(dest_uri)
        query = f"SELECT rate FROM {dest_table} WHERE currency_code = 'GBP'"
        with dest_engine.connect() as conn:
            rows = conn.exec_driver_sql(query).fetchall()
        dest_engine.dispose()

        # Assert that the rate for GBP is 0.82993
        assert len(rows) > 0, "No data found for GBP"
        assert abs(rows[0][0] - 0.82993) <= 1e-6, (
            f"Expected rate 0.82993, but got {rows[0][0]}"
        )

    return [
        invalid_source_table,
        interval_start_does_not_exceed_interval_end,
        interval_start_can_equal_interval_end,
        interval_start_does_not_exceed_current_date,
        interval_end_does_not_exceed_current_date,
        exchange_rate_on_specific_date,
    ]


@pytest.mark.parametrize(
    "dest", list(DESTINATIONS.values()), ids=list(DESTINATIONS.keys())
)
@pytest.mark.parametrize("test_case", frankfurter_test_cases())
def test_frankfurter(dest, test_case):
    test_case(dest.start())
    dest.stop()
