import sys
from collections import OrderedDict
from pathlib import Path

import polars as pl
import pytest

from tests.util import invoke_ingest_command


@pytest.fixture(scope="session")
def ods_testfile(tmp_path_factory, testdata_path) -> Path:
    """Supply an OpenOffice workbook file (ODS) for testing purposes, synthesized from an existing CSV file asset."""
    import pyexcel_ods3

    csv_file = testdata_path / "create_replace.csv"
    tmp_path = tmp_path_factory.mktemp("testdrive")
    ods_file = tmp_path / "test.ods"

    workbook = OrderedDict()
    df = pl.read_csv(csv_file)
    data = [df.columns]
    data.extend(df.rows())
    workbook.update({"ticker-symbols": data})
    pyexcel_ods3.save_data(str(ods_file), workbook)

    return ods_file


@pytest.fixture(scope="session")
def xlsx_testfile(tmp_path_factory, testdata_path) -> Path:
    """Supply an Excel workbook file (XLSX) for testing purposes, synthesized from an existing CSV file asset."""
    csv_file = testdata_path / "create_replace.csv"
    tmp_path = tmp_path_factory.mktemp("testdrive")
    xlsx_file = tmp_path / "test.xlsx"
    pl.read_csv(csv_file).write_excel(xlsx_file, worksheet="ticker-symbols")
    return xlsx_file


@pytest.mark.parametrize("spreadsheet_fixture", ["ods_testfile", "xlsx_testfile"])
def test_spreadsheet_source_with_sheet_name(request, spreadsheet_fixture, tmp_path):
    """Read a specific worksheet from a workbook file by name"""

    # Define input and output files.
    spreadsheet_testfile = request.getfixturevalue(spreadsheet_fixture)
    csv_outfile = tmp_path / "output.csv"

    # Invoke data loading.
    result = invoke_ingest_command(
        source_uri=f"file://{spreadsheet_testfile}#sheet_name=ticker-symbols",
        dest_uri=f"file://{csv_outfile}",
        print_output=False,
    )
    assert result.exit_code == 0, result.stderr

    # Validate output file content.
    content = csv_outfile.read_text().splitlines()
    assert content[0] == "symbol,date,is_enabled,name"
    assert content[1] == "A,2024-04-19,True,AGILENT TECHNOLOGIES INC"


@pytest.mark.parametrize("spreadsheet_fixture", ["ods_testfile", "xlsx_testfile"])
def test_spreadsheet_source_without_sheet(request, spreadsheet_fixture, tmp_path):
    """Without worksheet name, read the first worksheet from a workbook file"""

    # Define input and output files.
    spreadsheet_testfile = request.getfixturevalue(spreadsheet_fixture)
    csv_outfile = tmp_path / "output.csv"

    # Invoke data loading.
    result = invoke_ingest_command(
        source_uri=f"file://{spreadsheet_testfile}",
        dest_uri=f"file://{csv_outfile}",
        print_output=False,
    )
    assert result.exit_code == 0

    # Validate output file content.
    content = csv_outfile.read_text().splitlines()
    assert content[0] == "symbol,date,is_enabled,name"
    assert content[1] == "A,2024-04-19,True,AGILENT TECHNOLOGIES INC"


@pytest.mark.parametrize("spreadsheet_fixture", ["ods_testfile", "xlsx_testfile"])
def test_spreadsheet_source_with_unknown_sheet_name(
    request, spreadsheet_fixture, tmp_path
):
    """Reading an unknown worksheet from a workbook fails"""

    # Define input and output files.
    spreadsheet_testfile = request.getfixturevalue(spreadsheet_fixture)
    csv_outfile = tmp_path / "output.csv"

    # Invoke data loading.
    result = invoke_ingest_command(
        source_uri=f"file://{spreadsheet_testfile}#sheet_name=unknown",
        dest_uri=f"file://{csv_outfile}",
        print_output=False,
    )
    assert result.exit_code != 0, result.stderr

    # Validate the error message and that no output file exists.
    assert "no matching sheet found when `sheet_name` is 'unknown'" in str(
        result.exception
    )
    assert not csv_outfile.exists(), f"File {csv_outfile} exists but shouldn't"


@pytest.mark.skipif(
    sys.version_info < (3, 14), reason="requires Python 3.14 or greater"
)
@pytest.mark.parametrize("spreadsheet_fixture", ["ods_testfile", "xlsx_testfile"])
def test_spreadsheet_source_with_sheet_id(request, spreadsheet_fixture, tmp_path):
    """Read a specific worksheet from a workbook file by id"""

    # Define input and output files.
    spreadsheet_testfile = request.getfixturevalue(spreadsheet_fixture)
    csv_outfile = tmp_path / "output.csv"

    # Invoke data loading.
    result = invoke_ingest_command(
        source_uri=f"file://{spreadsheet_testfile}#sheet_id=1",
        dest_uri=f"file://{csv_outfile}",
        print_output=True,
    )
    assert result.exit_code == 0, result.stderr

    # Validate output file content.
    content = csv_outfile.read_text().splitlines()
    assert content[0] == "symbol,date,is_enabled,name"
    assert content[1] == "A,2024-04-19,True,AGILENT TECHNOLOGIES INC"


@pytest.mark.skipif(
    sys.version_info < (3, 14), reason="requires Python 3.14 or greater"
)
@pytest.mark.parametrize("spreadsheet_fixture", ["ods_testfile", "xlsx_testfile"])
def test_spreadsheet_source_with_unknown_sheet_id(
    request, spreadsheet_fixture, tmp_path
):
    """Read a specific worksheet from a workbook file by id that does not exist"""

    # Define input and output files.
    spreadsheet_testfile = request.getfixturevalue(spreadsheet_fixture)
    csv_outfile = tmp_path / "output.csv"

    # Invoke data loading.
    result = invoke_ingest_command(
        source_uri=f"file://{spreadsheet_testfile}#sheet_id=99",
        dest_uri=f"file://{csv_outfile}",
        print_output=True,
    )
    assert result.exit_code != 0, result.stderr

    # Validate the error message and that no output file exists.
    assert "no matching sheet found when `sheet_id` is 99" in str(result.exception)
    assert not csv_outfile.exists(), f"File {csv_outfile} exists but shouldn't"


@pytest.mark.skipif(
    sys.version_info < (3, 14), reason="requires Python 3.14 or greater"
)
@pytest.mark.parametrize("spreadsheet_fixture", ["ods_testfile", "xlsx_testfile"])
def test_spreadsheet_source_without_header(request, spreadsheet_fixture, tmp_path):
    """Read a specific worksheet from a workbook file using the `has_header=false` option"""

    # Define input and output files.
    spreadsheet_testfile = request.getfixturevalue(spreadsheet_fixture)
    csv_outfile = tmp_path / "output.csv"

    # Invoke data loading.
    result = invoke_ingest_command(
        source_uri=f"file://{spreadsheet_testfile}#sheet_name=ticker-symbols&has_header=false",
        dest_uri=f"file://{csv_outfile}",
        print_output=False,
    )
    assert result.exit_code == 0, result.stderr

    # Validate output file content.
    content = csv_outfile.read_text().splitlines()
    assert content[0] == "column_1,column_2,column_3,column_4"
    assert content[1] == "symbol,date,isEnabled,name"
    assert content[2] == "A,2024-04-19,true,AGILENT TECHNOLOGIES INC"


@pytest.mark.skipif(
    sys.version_info < (3, 14), reason="requires Python 3.14 or greater"
)
@pytest.mark.parametrize("spreadsheet_fixture", ["ods_testfile", "xlsx_testfile"])
def test_spreadsheet_source_with_columns(request, spreadsheet_fixture, tmp_path):
    """Read a specific worksheet from a workbook file using the `columns=a,b,c` option"""

    # Define input and output files.
    spreadsheet_testfile = request.getfixturevalue(spreadsheet_fixture)
    csv_outfile = tmp_path / "output.csv"

    # Invoke data loading.
    result = invoke_ingest_command(
        source_uri=f'file://{spreadsheet_testfile}#columns=["symbol","date"]',
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
@pytest.mark.parametrize("spreadsheet_fixture", ["ods_testfile", "xlsx_testfile"])
def test_spreadsheet_source_with_wrong_columns(request, spreadsheet_fixture, tmp_path):
    """Read a specific worksheet from a workbook file using the `columns=a,b,c` option"""

    # Define input and output files.
    spreadsheet_testfile = request.getfixturevalue(spreadsheet_fixture)
    csv_outfile = tmp_path / "output.csv"

    # Invoke data loading.
    result = invoke_ingest_command(
        source_uri=f'file://{spreadsheet_testfile}#columns=["unknown","date"]',
        dest_uri=f"file://{csv_outfile}",
        print_output=False,
    )
    assert result.exit_code != 0, result.stderr

    # Validate the error message and that no output file exists.
    assert 'column with name "unknown" not found' in str(result.exception)
    assert not csv_outfile.exists(), f"File {csv_outfile} exists but shouldn't"
