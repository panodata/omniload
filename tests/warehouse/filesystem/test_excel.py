from importlib.resources import as_file, files
from pathlib import Path

import polars as pl
import pytest

from tests.util import invoke_ingest_command


@pytest.fixture(scope="session")
def xlsx_testfile(tmp_path_factory) -> Path:
    """Supply an Excel workbook file (XLSX) for testing purposes, synthesized from an existing CSV file asset."""
    csv_traversable = files("omniload.testdata") / "create_replace.csv"
    tmp_path = tmp_path_factory.mktemp("testdrive")
    xlsx = tmp_path / "test.xlsx"
    with as_file(csv_traversable) as csv:
        pl.read_csv(csv).write_excel(xlsx, worksheet="ticker-symbols")
    return xlsx


def test_excel_source_success_with_sheet(xlsx_testfile, tmp_path):
    """Read a specific worksheet from an Excel workbook file"""

    # Define output file.
    csv_outfile = tmp_path / "output.csv"

    # Invoke data loading.
    result = invoke_ingest_command(
        source_uri=f"file://{xlsx_testfile}#sheet_name=ticker-symbols",
        dest_uri=f"file://{csv_outfile}",
        print_output=False,
    )
    assert result.exit_code == 0, result.stderr

    # Validate output file content.
    content = csv_outfile.read_text().splitlines()
    assert content[0] == "symbol,date,is_enabled,name"
    assert content[1] == "A,2024-04-19,True,AGILENT TECHNOLOGIES INC"


def test_excel_source_success_without_sheet(xlsx_testfile, tmp_path):
    """Without worksheet name, read the first worksheet from an Excel workbook file"""

    # Define output file.
    csv_outfile = tmp_path / "output.csv"

    # Invoke data loading.
    result = invoke_ingest_command(
        source_uri=f"file://{xlsx_testfile}",
        dest_uri=f"file://{csv_outfile}",
        print_output=False,
    )
    assert result.exit_code == 0

    # Validate output file content.
    content = csv_outfile.read_text().splitlines()
    assert content[0] == "symbol,date,is_enabled,name"
    assert content[1] == "A,2024-04-19,True,AGILENT TECHNOLOGIES INC"


def test_excel_source_unknown_sheet(xlsx_testfile, tmp_path):
    """Reading an unknown worksheet from an Excel workbook fails"""

    # Define output file.
    csv_outfile = tmp_path / "output.csv"

    # Invoke data loading.
    result = invoke_ingest_command(
        source_uri=f"file://{xlsx_testfile}#sheet_name=unknown",
        dest_uri=f"file://{csv_outfile}",
        print_output=False,
    )
    assert result.exit_code != 0, result.stderr

    assert "no matching sheet found when `sheet_name` is 'unknown'" in str(
        result.exception
    )
    assert not csv_outfile.exists(), f"File {csv_outfile} exists but shouldn't"
