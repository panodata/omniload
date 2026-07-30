from pathlib import Path
from urllib.parse import quote

import pytest
from azure.storage.blob import BlobServiceClient
from testcontainers.azurite import AzuriteContainer

from dlt_filesystem.source.error import NoFilesFoundError
from omniload import run_ingest
from tests.dlt_filesystem.gcs import GCSFakeServerContainer
from tests.util import invoke_ingest_command
from tests.util.common import has_exception
from tests.util.container.impl.floci import FlociContainer
from tests.warehouse.manager import FLOCI_IMAGE

pytestmark = pytest.mark.integration


@pytest.fixture(scope="session")
def floci():
    """Provide S3 service container for the whole test session."""
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


AZURE_API_VERSION = "2025-11-05"


@pytest.fixture(autouse=True)
def adlfs_patch(monkeypatch):
    """
    Patch defining api_version into the client used by adlfs until that patch has landed.
    https://github.com/fsspec/adlfs/pull/555
    """
    import adlfs
    from adlfs.spec import _USER_AGENT, AIOBlobServiceClient

    def _create_aio_blob_service_client_from_connection_string_with_api_version(
        connection_string: str,
    ) -> AIOBlobServiceClient:
        return AIOBlobServiceClient.from_connection_string(
            conn_str=connection_string,
            user_agent=_USER_AGENT,
            api_version=AZURE_API_VERSION,
        )

    monkeypatch.setattr(
        adlfs.spec,
        "_create_aio_blob_service_client_from_connection_string",
        _create_aio_blob_service_client_from_connection_string_with_api_version,
    )


@pytest.fixture(scope="session")
def azurite():
    """Provide Azure Storage service container for the whole test session."""
    container = AzuriteContainer()
    container.start()
    connection_string = container.get_connection_string()
    client = BlobServiceClient.from_connection_string(
        connection_string,
        api_version=AZURE_API_VERSION,
    )
    client.create_container("test-container")
    cc = client.get_container_client("test-container")
    cc.upload_blob(
        name="path/to/create_replace.csv",
        data=Path("tests/assets/create_replace.csv").read_text(),
    )
    yield container
    container.stop()


@pytest.fixture(scope="session")
def gcsfakeserver():
    """Provide GCS emulator container for the whole test session."""
    import google.cloud.storage
    from google.auth.credentials import AnonymousCredentials

    container = GCSFakeServerContainer()
    container.start()
    endpoint_url = container.get_endpoint_url()

    client = google.cloud.storage.Client(
        credentials=AnonymousCredentials(),
        project="test",
        client_options={"api_endpoint": endpoint_url},
    )
    bucket = client.create_bucket("test-bucket")
    blob = bucket.blob("path/to/create_replace.csv")
    blob.upload_from_filename("tests/assets/create_replace.csv")
    yield container
    container.stop()


def duckdb_table_cardinality(db_path: Path, table_name: str) -> int:
    """Return number of records in database table."""
    import duckdb

    db = duckdb.connect(db_path)
    result = db.execute(f"SELECT * FROM {table_name}").fetchall()
    count = len(result)
    db.close()
    return count


def duckdb_table_exists(db_path: Path, schema_name: str, table_name: str) -> bool:
    """Return whether a table exists in a DuckDB database."""
    import duckdb

    db = duckdb.connect(db_path)
    result = db.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = ? AND table_name = ?",
        [schema_name, table_name],
    ).fetchone()
    db.close()
    return result is not None


def test_s3_source(floci, tmp_path):
    """S3 source integration test."""
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


@pytest.mark.parametrize(
    "source_table",
    [
        "test-bucket/path/to/missing.csv",
        "missing-bucket/path/to/missing.csv",
    ],
)
def test_s3_missing_concrete_source_fails(floci, tmp_path, source_table):
    """The CLI fails when an S3 concrete path or its bucket does not exist."""
    s3_endpoint = floci.get_url()
    result = invoke_ingest_command(
        f"s3://?endpoint_url={s3_endpoint}&access_key_id=test&secret_access_key=test",
        source_table,
        f"duckdb:///{tmp_path / 'db.duckdb'}",
        "testdrive.data",
        print_output=False,
    )

    assert result.exit_code != 0
    assert has_exception(result.exception, NoFilesFoundError)
    assert f"s3://{source_table.split('/', 1)[0]}/" in str(result.exception)
    assert source_table.split("/", 1)[1] in str(result.exception)


def test_s3_unmatched_glob_succeeds(floci, tmp_path):
    """An S3 wildcard remains a valid empty source selection."""
    s3_endpoint = floci.get_url()
    db_path = tmp_path / "db.duckdb"
    result = run_ingest(
        source_uri=f"s3://?endpoint_url={s3_endpoint}&access_key_id=test&secret_access_key=test",
        dest_uri=f"duckdb:///{db_path}",
        source_table="test-bucket/path/to/*.nomatch.csv",
        dest_table="testdrive.data",
    )

    assert result is not None
    assert not duckdb_table_exists(db_path, "testdrive", "data")


def test_azure_source(azurite, tmp_path):
    """Azure Storage source integration test."""
    connection_string = azurite.get_connection_string()
    db_path = tmp_path / "db.duckdb"
    result = run_ingest(
        source_uri=f"az://?connection_string={quote(connection_string)}&api_version=2025-11-05",
        dest_uri=f"duckdb:///{db_path}",
        source_table="test-container/path/to/create_replace.csv",
        dest_table="testdrive.data",
    )
    if result is None:
        raise RuntimeError("Ingest failed")
    package = result.asdict()["load_packages"][0]
    assert package["state"] == "loaded"

    count = duckdb_table_cardinality(db_path, "testdrive.data")
    assert count == 20, f"Wrong number of records: {count}. Expected: 20"


def test_gcs_source(gcsfakeserver, tmp_path):
    """Google Cloud Storage source integration test."""
    endpoint_url = gcsfakeserver.get_endpoint_url()
    db_path = tmp_path / "db.duckdb"
    result = run_ingest(
        source_uri=f"gs://?endpoint_url={endpoint_url}&token=anon&project=test",
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
