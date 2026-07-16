import sys
from pathlib import Path

import pytest

from tests.util import invoke_ingest_command


@pytest.fixture(scope="session")
def csv_testfile(testdata_path) -> Path:
    """Supply a CSV file to all test cases."""
    return testdata_path / "create_replace.csv"


def test_csv_source_without_hints(csv_testfile, tmp_path):
    """Read a CSV file without any reader hints"""

    # Define output file.
    csv_outfile = tmp_path / "output.csv"

    # Invoke data loading.
    result = invoke_ingest_command(
        source_uri=f"file://{csv_testfile}",
        dest_uri=f"file://{csv_outfile}",
        print_output=False,
    )
    assert result.exit_code == 0, result.stderr

    # Validate output file content.
    content = csv_outfile.read_text().splitlines()
    assert content[0] == "symbol,date,is_enabled,name"
    assert content[1] == "A,2024-04-19,True,AGILENT TECHNOLOGIES INC"


@pytest.mark.skipif(
    sys.version_info < (3, 14), reason="requires Python 3.14 or greater"
)
def test_csv_source_with_columns(csv_testfile, tmp_path):
    """Read a CSV file using the `columns=[...]` reader hint"""

    # Define output file.
    csv_outfile = tmp_path / "output.csv"

    # Invoke data loading.
    result = invoke_ingest_command(
        source_uri=f'file://{csv_testfile}#columns=["symbol","date"]',
        dest_uri=f"file://{csv_outfile}",
        print_output=False,
    )
    assert result.exit_code == 0, result.stderr

    # Validate output file content.
    content = csv_outfile.read_text().splitlines()
    assert content[0] == "symbol,date"
    assert content[1] == "A,2024-04-19"


@pytest.mark.skipif(
    sys.version_info < (3, 14), reason="requires Python 3.14 or greater"
)
def test_csv_source_with_wrong_columns(csv_testfile, tmp_path):
    """Read a CSV file using the `columns=[...]` reader hint with an unknown column"""

    # Define output file.
    csv_outfile = tmp_path / "output.csv"

    # Invoke data loading.
    result = invoke_ingest_command(
        source_uri=f'file://{csv_testfile}#columns=["unknown","date"]',
        dest_uri=f"file://{csv_outfile}",
        print_output=False,
    )
    assert result.exit_code != 0, result.stderr

    # Validate the error message and that no output file exists.
    assert 'unable to find column "unknown"' in str(result.exception)
    assert not csv_outfile.exists(), f"File {csv_outfile} exists but shouldn't"


@pytest.mark.skipif(
    sys.version_info < (3, 14), reason="requires Python 3.14 or greater"
)
def test_csv_source_without_header(csv_testfile, tmp_path):
    """Read a CSV file using the `has_header=false` reader hint"""

    # Define output file.
    csv_outfile = tmp_path / "output.csv"

    # Invoke data loading.
    result = invoke_ingest_command(
        source_uri=f"file://{csv_testfile}#has_header=false",
        dest_uri=f"file://{csv_outfile}",
        print_output=False,
    )
    assert result.exit_code == 0, result.stderr

    # Validate output file content.
    content = csv_outfile.read_text().splitlines()
    assert content[0] == "column_1,column_2,column_3,column_4"
    assert content[1] == "symbol,date,isEnabled,name"
    assert content[2] == "A,2024-04-19,True,AGILENT TECHNOLOGIES INC"


@pytest.mark.skipif(
    sys.version_info < (3, 14), reason="requires Python 3.14 or greater"
)
def test_csv_source_with_n_rows(csv_testfile, tmp_path):
    """Read a CSV file using the `n_rows=2` reader hint to limit the number of rows read"""

    # Define output file.
    csv_outfile = tmp_path / "output.csv"

    # Invoke data loading.
    result = invoke_ingest_command(
        source_uri=f"file://{csv_testfile}#n_rows=2",
        dest_uri=f"file://{csv_outfile}",
        print_output=False,
    )
    assert result.exit_code == 0, result.stderr

    # Validate output file content: header plus exactly two data rows.
    content = csv_outfile.read_text().splitlines()
    assert len(content) == 3
    assert content[1] == "A,2024-04-19,True,AGILENT TECHNOLOGIES INC"
    assert content[2] == "AA,2024-04-19,True,ALCOA CORP"
