from typing import Callable, Iterable

import pytest

from omniload.src.errors import MissingValueError, UnsupportedResourceError
from tests.util import has_exception, invoke_ingest_command


def applovin_test_cases() -> Iterable[Callable]:
    def missing_api_key():
        result = invoke_ingest_command(
            "applovin://",
            "publisher-report",
            "duckdb:///out.db",
            "public.publisher_report",
            print_output=False,
        )
        assert result.exit_code != 0
        assert has_exception(result.exception, MissingValueError)

    def invalid_source_table():
        result = invoke_ingest_command(
            "applovin://?api_key=123",
            "unknown-report",
            "duckdb:///out.db",
            "public.unknown_report",
            print_output=False,
        )
        assert result.exit_code != 0
        assert has_exception(result.exception, UnsupportedResourceError)

    return [
        missing_api_key,
        invalid_source_table,
    ]


@pytest.mark.parametrize("testcase", applovin_test_cases())
def test_applovin_source(testcase):
    testcase()
