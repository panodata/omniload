from pathlib import Path

from tests.util import invoke_ingest_command


def test_excel_source(tmp_path):
    assets_path = Path(__file__).parent.parent.parent / "assets"
    dest = tmp_path / "output.csv"
    result = invoke_ingest_command(
        f"excel://{assets_path}/create_replace.xlsx",
        "create_replace",
        f"csv://{dest}",
        "test.foo",
        print_output=False,
    )
    assert result.exit_code == 0

    content = Path(dest).read_text()
    assert "AGILENT TECHNOLOGIES INC" in content
