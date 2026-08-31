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


def _make_catalog(catalog_path, rows=2):
    """Write a Delta Lake table the ingest command can read back."""
    frame = pl.DataFrame(
        [{"temperature": 42.42, "humidity": 84.84} for _ in range(rows)]
    )
    frame.write_delta(catalog_path / "delta-schema" / "delta-table")


def _read(catalog_path, dest_db, inc_strategy=None):
    return invoke_ingest_command(
        f"file+delta://{catalog_path}",
        "delta-schema.delta-table",
        f"duckdb:///{dest_db}",
        "analytics.events",
        inc_strategy=inc_strategy,
        print_output=False,
    )


def _count(dest_db):
    import duckdb

    connection = duckdb.connect(str(dest_db))
    try:
        return connection.sql("SELECT count(*) FROM analytics.events").fetchall()[0][0]
    finally:
        connection.close()


def test_read_replaces_by_default(tmp_path):
    """Every run reads the whole table, so a second run resets the destination
    rather than loading a second copy."""
    catalog_path = tmp_path / "delta-catalog"
    _make_catalog(catalog_path)
    dest_db = tmp_path / "warehouse.duckdb"
    for _ in range(2):
        assert _read(catalog_path, dest_db).exit_code == 0

    assert _count(dest_db) == 2


def test_read_appends_on_request(tmp_path):
    """`--incremental-strategy append` reaches the load instead of being discarded
    in favour of the connector's own `replace`."""
    catalog_path = tmp_path / "delta-catalog"
    _make_catalog(catalog_path)
    dest_db = tmp_path / "warehouse.duckdb"
    for _ in range(2):
        assert _read(catalog_path, dest_db, inc_strategy="append").exit_code == 0

    assert _count(dest_db) == 4


def test_read_rejects_a_key_dependent_strategy(tmp_path, caplog):
    """A whole-table read exposes no incremental key, so merge fails naming the
    Delta Lake source rather than silently replacing."""
    catalog_path = tmp_path / "delta-catalog"
    _make_catalog(catalog_path)
    result = _read(catalog_path, tmp_path / "warehouse.duckdb", inc_strategy="merge")

    assert result.exit_code != 0
    assert "'file+delta' source does not expose" in caplog.text
