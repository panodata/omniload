import polars as pl

from tests.util import invoke_ingest_command


def test_read(tmp_path):
    in_path = tmp_path / "delta-catalog"
    df = pl.DataFrame(
        [
            {"temperature": 42.42, "humidity": 84.84},
            {"temperature": 42.42, "humidity": 84.84},
        ]
    )
    df.write_delta(in_path / "delta-schema" / "delta-table")

    out_path = tmp_path / "delta.csv"
    result = invoke_ingest_command(
        f"file+delta://{in_path}",
        "delta-schema.delta-table",
        f"file://{out_path}",
        "",
        print_output=False,
    )
    assert result.exit_code == 0
    assert out_path.exists()


def test_write(tmp_path):
    out_path = tmp_path / "delta-catalog"
    result = invoke_ingest_command(
        "file://tests/assets/create_replace.csv",
        "",
        f"file+delta://{out_path}",
        "delta-schema.delta-table",
        print_output=False,
    )
    assert result.exit_code == 0
    assert out_path.exists()
