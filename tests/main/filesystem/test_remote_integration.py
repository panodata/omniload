from pathlib import Path

import pytest

from omniload import run_ingest
from tests.util.container.impl.floci import FlociContainer
from tests.warehouse.manager import FLOCI_IMAGE


@pytest.fixture(scope="session")
def floci():
    """
    Provide a S3 service container for the whole test session.
    """
    container = FlociContainer(image=FLOCI_IMAGE)
    container.start()
    s3 = container.get_client("s3")  # ty: ignore[invalid-argument-type, missing-argument]
    s3.create_bucket(Bucket="test-bucket")
    s3.upload_file(
        Filename="tests/assets/create_replace.csv",
        Bucket="test-bucket",
        Key="path/to/create_replace.csv",
    )
    yield container
    container.stop()


def duckdb_table_cardinality(db_path: Path, table_name: str) -> int:
    import duckdb

    db = duckdb.connect(db_path)
    result = db.execute(f"SELECT * FROM {table_name}").fetchall()
    count = len(result)
    db.close()
    return count


def test_s3_source(floci, tmp_path):
    s3_endpoint = floci.get_url()
    db_path = tmp_path / "db.duckdb"
    result = run_ingest(
        source_uri=f"s3://?endpoint_url={s3_endpoint}&access_key_id=test&secret_access_key=test",
        dest_uri=f"duckdb:///{db_path}",
        source_table="test-bucket/path/to/create_replace.csv",
        dest_table="testdrive.data",
    )
    if result is None:
        raise RuntimeError("Ingest failed")
    package = result.asdict()["load_packages"][0]
    assert package["state"] == "loaded"

    count = duckdb_table_cardinality(db_path, "testdrive.data")
    assert count == 20, f"Wrong number of records: {count}. Expected: 20"
