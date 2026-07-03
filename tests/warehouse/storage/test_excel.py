from pathlib import Path

from tests.util import invoke_ingest_command


def test_excel_source(tmp_path):

    # Define input and output paths.
    assets_path = Path(__file__).parent.parent.parent / "assets"
    outfile = tmp_path / "output.csv"

    # Invoke data loading.
    result = invoke_ingest_command(
        f"file://{assets_path}/create_replace.xlsx",
        "ticker-symbols",
        f"csv://{outfile}",
        "test.foo",
        print_output=False,
    )
    assert result.exit_code == 0

    # Validate output file content.
    content = Path(outfile).read_text().splitlines()
    assert content[0] == "symbol,date,is_enabled,name"
    assert content[1] == "A,2024-04-19,True,AGILENT TECHNOLOGIES INC"
