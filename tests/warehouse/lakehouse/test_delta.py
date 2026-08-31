import polars as pl

from tests.util import invoke_ingest_command


def test_read(tmp_path):
    """Basic test for reading from Delta Lake tables."""
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

    out_records = len(out_path.read_text().splitlines())
    assert out_records == 3, f"Wrong number of records in {out_path}: {out_records}"


def test_write(tmp_path):
    """Basic test for writing to Delta Lake tables."""
    catalog_path = tmp_path / "delta-catalog"
    out_path = catalog_path / "delta_schema" / "delta_table"
    result = invoke_ingest_command(
        "file://tests/assets/create_replace.csv",
        "",
        f"file+delta://{catalog_path}",
        "delta-schema.delta-table",
        print_output=False,
    )
    assert result.exit_code == 0
    assert catalog_path.exists()

    out_frame = pl.read_delta(out_path)
    out_records = len(out_frame)
    assert out_records == 20, f"Wrong number of records in {out_path}: {out_records}"
