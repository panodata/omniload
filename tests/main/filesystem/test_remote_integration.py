import gzip
import io
import json
import sqlite3
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from time import sleep
from urllib.parse import parse_qsl, quote, urlencode
from uuid import uuid4

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from azure.storage.blob import BlobServiceClient
from dlt.sources.filesystem import FileItemDict

from dlt_filesystem.source.error import NoFilesFoundError
from dlt_filesystem.staging import RemoteObjectNotFoundError
from omniload import run_ingest
from tests.util import invoke_ingest_command
from tests.util.common import has_exception
from tests.warehouse.manager import get_remote_filesystem_services

pytestmark = pytest.mark.integration

AZURE_API_VERSION = "2025-11-05"
REMOTE_BACKENDS = ["s3", "azure", "gcs"]
FAST_SOURCE_BACKENDS = ["s3", "azure"]
DESTINATION_BACKENDS = ["s3", "azure"]
FORMAT_MATRIX = [
    pytest.param(backend, file_format, id=f"{backend}-{file_format}")
    for backend in FAST_SOURCE_BACKENDS
    for file_format in ["csv", "jsonl", "parquet", "csv.gz"]
] + [pytest.param("gcs", "csv", id="gcs-csv")]
DATABASE_MATRIX = [
    pytest.param("s3", "duckdb", id="s3-duckdb"),
    pytest.param("azure", "sqlite", id="azure-sqlite"),
    pytest.param("gcs", "duckdb", id="gcs-duckdb"),
]

CSV_ROWS = b"name,value\nAlice,1\nBob,2\n"
WIDGET_ROWS = [(1, "alpha"), (2, "beta"), (3, "gamma")]


@dataclass(frozen=True)
class RemoteFilesystemEmulator:
    """A per-test namespace on a controller-managed remote filesystem emulator."""

    backend: str
    source_uri: str
    destination_uri: str | None
    namespace: str
    upload: Callable[[str, bytes], None]
    objects: Callable[[str], dict[str, bytes]]

    def source_table(self, path: str) -> str:
        return f"{self.namespace}/{path}"

    def object_uri(self, path: str) -> str:
        """Address one object by the URI its storage service already names it with."""
        scheme, _, query = self.source_uri.partition("?")
        return f"{scheme}{self.namespace}/{path}?{query}"

    def destination_table(self, prefix: str, table: str = "data") -> str:
        return f"{self.namespace}/{prefix}/{table}"


@pytest.fixture
def adlfs_patch(monkeypatch):
    """Set the Azure API version until fsspec/adlfs#555 is released."""
    import adlfs
    from adlfs.spec import _USER_AGENT, AIOBlobServiceClient

    def create_client(connection_string: str) -> AIOBlobServiceClient:
        return AIOBlobServiceClient.from_connection_string(
            conn_str=connection_string,
            user_agent=_USER_AGENT,
            api_version=AZURE_API_VERSION,
        )

    monkeypatch.setattr(
        adlfs.spec,
        "_create_aio_blob_service_client_from_connection_string",
        create_client,
    )


@pytest.fixture
def s3_emulator() -> RemoteFilesystemEmulator:
    import boto3

    endpoint_url = get_remote_filesystem_services()["s3"].start()
    client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        region_name="us-west-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",  # noqa: S106
    )
    bucket = f"omniload-{uuid4().hex}"
    client.create_bucket(Bucket=bucket)

    def upload(path: str, data: bytes) -> None:
        client.put_object(Bucket=bucket, Key=path, Body=data)

    def objects(prefix: str) -> dict[str, bytes]:
        response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        return {
            item["Key"]: client.get_object(Bucket=bucket, Key=item["Key"])[
                "Body"
            ].read()
            for item in response.get("Contents", [])
        }

    uri = f"s3://?endpoint_url={endpoint_url}&access_key_id=test&secret_access_key=test"
    return RemoteFilesystemEmulator("s3", uri, uri, bucket, upload, objects)


@pytest.fixture
def azure_emulator(adlfs_patch) -> RemoteFilesystemEmulator:
    connection_string = get_remote_filesystem_services()["azure"].start()
    client = BlobServiceClient.from_connection_string(
        connection_string,
        api_version=AZURE_API_VERSION,
    )
    container_name = f"omniload-{uuid4().hex}"
    container = client.create_container(container_name)

    def upload(path: str, data: bytes) -> None:
        container.upload_blob(name=path, data=data, overwrite=True)

    def objects(prefix: str) -> dict[str, bytes]:
        return {
            blob.name: container.download_blob(blob.name).readall()
            for blob in container.list_blobs(name_starts_with=prefix)
        }

    uri = (
        "az://?connection_string="
        f"{quote(connection_string, safe='')}&api_version={AZURE_API_VERSION}"
    )
    return RemoteFilesystemEmulator("azure", uri, uri, container_name, upload, objects)


@pytest.fixture
def gcs_emulator() -> RemoteFilesystemEmulator:
    import google.cloud.storage
    from google.auth.credentials import AnonymousCredentials

    endpoint_url = get_remote_filesystem_services()["gcs"].start()
    client = google.cloud.storage.Client(
        credentials=AnonymousCredentials(),
        project="test",
        client_options={"api_endpoint": endpoint_url},
    )
    bucket_name = f"omniload-{uuid4().hex}"
    bucket = client.create_bucket(bucket_name)

    def upload(path: str, data: bytes) -> None:
        bucket.blob(path).upload_from_string(data)

    def objects(prefix: str) -> dict[str, bytes]:
        return {
            blob.name: blob.download_as_bytes()
            for blob in client.list_blobs(bucket, prefix=prefix)
        }

    uri = f"gs://?endpoint_url={endpoint_url}&token=anon&project=test"
    return RemoteFilesystemEmulator("gcs", uri, None, bucket_name, upload, objects)


@pytest.fixture
def remote_filesystem(request) -> RemoteFilesystemEmulator:
    fixture_names = {
        "s3": "s3_emulator",
        "azure": "azure_emulator",
        "gcs": "gcs_emulator",
    }
    return request.getfixturevalue(fixture_names[request.param])


def duckdb_table_cardinality(db_path: Path, table_name: str) -> int:
    """Return the number of records in a DuckDB table."""
    with duckdb.connect(db_path) as db:
        row = db.execute(f"SELECT count(*) FROM {table_name}").fetchone()
    assert row is not None
    return row[0]


def duckdb_table_exists(db_path: Path, schema_name: str, table_name: str) -> bool:
    """Return whether a table exists in a DuckDB database."""
    with duckdb.connect(db_path) as db:
        result = db.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ?",
            [schema_name, table_name],
        ).fetchone()
    return result is not None


def format_payload(file_format: str) -> tuple[str, bytes]:
    """Build a two-row fixture in a supported remote-reader format."""
    if file_format == "csv":
        return "csv", CSV_ROWS
    if file_format == "csv.gz":
        return "csv.gz", gzip.compress(CSV_ROWS)
    if file_format == "jsonl":
        rows = [{"name": "Alice", "value": 1}, {"name": "Bob", "value": 2}]
        return "jsonl", b"".join(json.dumps(row).encode() + b"\n" for row in rows)
    if file_format == "parquet":
        buffer = io.BytesIO()
        pq.write_table(pa.table({"name": ["Alice", "Bob"], "value": [1, 2]}), buffer)
        return "parquet", buffer.getvalue()
    raise ValueError(f"Unknown test format: {file_format}")


def database_payload(engine: str, path: Path) -> bytes:
    """Build a small single-table database and return it as uploadable bytes."""
    if engine == "duckdb":
        with duckdb.connect(path) as db:
            db.execute("CREATE TABLE widgets (id INTEGER, name VARCHAR)")
            db.executemany("INSERT INTO widgets VALUES (?, ?)", WIDGET_ROWS)
    elif engine == "sqlite":
        with closing(sqlite3.connect(path)) as db, db:
            db.execute("CREATE TABLE widgets (id INTEGER, name TEXT)")
            db.executemany("INSERT INTO widgets VALUES (?, ?)", WIDGET_ROWS)
    else:
        raise ValueError(f"Unknown test database engine: {engine}")
    return path.read_bytes()


def parquet_rows(objects: dict[str, bytes]) -> list[dict]:
    """Read all destination parquet data objects returned by an emulator SDK."""
    parquet_objects = {
        key: value for key, value in objects.items() if key.endswith(".parquet")
    }
    assert parquet_objects, f"No parquet data object found in {sorted(objects)}"
    rows = []
    for data in parquet_objects.values():
        rows.extend(pq.read_table(io.BytesIO(data)).to_pylist())
    return rows


@pytest.mark.parametrize(
    "remote_filesystem,file_format", FORMAT_MATRIX, indirect=["remote_filesystem"]
)
def test_remote_source_format_matrix(remote_filesystem, file_format, tmp_path):
    """Core formats, including compression, load through the writable emulators.

    A fake-gcs-server load is much slower than S3 or Azure locally, so GCS keeps one
    concrete CSV read without repeating formats already exercised through the same
    shared reader layer.
    """
    suffix, payload = format_payload(file_format)
    key = f"formats/data.{suffix}"
    remote_filesystem.upload(key, payload)

    db_path = tmp_path / "format.duckdb"
    result = run_ingest(
        source_uri=remote_filesystem.source_uri,
        dest_uri=f"duckdb:///{db_path}",
        source_table=remote_filesystem.source_table(key),
        dest_table="testdrive.data",
        progress="log",
    )

    assert result is not None
    assert duckdb_table_cardinality(db_path, "testdrive.data") == 2


@pytest.mark.parametrize("remote_filesystem", FAST_SOURCE_BACKENDS, indirect=True)
def test_remote_glob_concatenates_matching_objects(remote_filesystem, tmp_path):
    """A remote glob reads every matching object and ignores other formats.

    GCS keeps concrete and unmatched reads because each fake-gcs-server glob adds
    several minutes locally, beyond this lane's five-minute serial budget.
    """
    remote_filesystem.upload("glob/part-1.csv", CSV_ROWS)
    remote_filesystem.upload("glob/part-2.csv", CSV_ROWS)
    remote_filesystem.upload("glob/ignored.jsonl", b'{"name":"Ignored","value":0}\n')

    db_path = tmp_path / "glob.duckdb"
    result = run_ingest(
        source_uri=remote_filesystem.source_uri,
        dest_uri=f"duckdb:///{db_path}",
        source_table=remote_filesystem.source_table("glob/*.csv"),
        dest_table="testdrive.data",
        progress="log",
    )

    assert result is not None
    assert duckdb_table_cardinality(db_path, "testdrive.data") == 4


@pytest.mark.parametrize("remote_filesystem", REMOTE_BACKENDS, indirect=True)
def test_remote_unmatched_glob_succeeds(remote_filesystem, tmp_path):
    """A wildcard with no remote matches remains a valid empty selection."""
    db_path = tmp_path / "unmatched.duckdb"
    result = run_ingest(
        source_uri=remote_filesystem.source_uri,
        dest_uri=f"duckdb:///{db_path}",
        source_table=remote_filesystem.source_table("missing/*.csv"),
        dest_table="testdrive.data",
        progress="log",
    )

    assert result is not None
    assert not duckdb_table_exists(db_path, "testdrive", "data")


@pytest.mark.parametrize(
    "missing_bucket", [False, True], ids=["missing-object", "missing-bucket"]
)
def test_s3_missing_concrete_source_fails(s3_emulator, tmp_path, missing_bucket):
    """The CLI fails when an S3 concrete object or its bucket does not exist."""
    bucket = "missing-bucket" if missing_bucket else s3_emulator.namespace
    source_table = f"{bucket}/path/to/missing.csv"
    result = invoke_ingest_command(
        s3_emulator.source_uri,
        source_table,
        f"duckdb:///{tmp_path / 'missing.duckdb'}",
        "testdrive.data",
        print_output=False,
    )

    assert result.exit_code != 0
    assert has_exception(result.exception, NoFilesFoundError)
    assert f"s3://{bucket}/" in str(result.exception)
    assert "path/to/missing.csv" in str(result.exception)


@pytest.mark.parametrize(
    "remote_filesystem,engine", DATABASE_MATRIX, indirect=["remote_filesystem"]
)
def test_remote_database_source_stages_and_cleans_up(
    remote_filesystem, engine, tmp_path
):
    """A remote database object loads through SQL extraction and leaves nothing staged.

    The object keeps the URI its storage service names it with, so the extension
    routes it to the SQL source and ``--source-table`` selects a table inside it.
    """
    key = f"databases/source.{engine}"
    remote_filesystem.upload(
        key, database_payload(engine, tmp_path / f"source.{engine}")
    )

    staging_root = tmp_path / "staging"
    # The destination file name doubles as the DuckDB catalog name, so keep it
    # distinct from the `remote` dataset the rows land in.
    db_path = tmp_path / f"{remote_filesystem.backend}-remote.duckdb"
    result = run_ingest(
        source_uri=remote_filesystem.object_uri(key),
        dest_uri=f"duckdb:///{db_path}",
        source_table="main.widgets",
        dest_table="remote.widgets",
        remote_database_staging_root=str(staging_root),
        progress="log",
    )

    assert result is not None
    assert duckdb_table_cardinality(db_path, "remote.widgets") == 3
    assert list(staging_root.iterdir()) == []


def test_missing_remote_database_fails_and_removes_staging(s3_emulator, tmp_path):
    """A missing object reports its safe location and leaves no staged files."""
    staging_root = tmp_path / "staging"
    options = dict(parse_qsl(s3_emulator.source_uri.partition("?")[2]))
    options["secret_access_key"] = "do-not-report"  # noqa: S105 - leak canary
    source_uri = (
        f"s3://{s3_emulator.namespace}/databases/missing.duckdb?{urlencode(options)}"
    )

    with pytest.raises(RemoteObjectNotFoundError) as excinfo:
        run_ingest(
            source_uri=source_uri,
            dest_uri=f"duckdb:///{tmp_path / 'missing.duckdb'}",
            source_table="main.widgets",
            dest_table="remote.widgets",
            remote_database_staging_root=str(staging_root),
            progress="log",
        )

    assert f"s3://{s3_emulator.namespace}/databases/missing.duckdb" in str(
        excinfo.value
    )
    assert "do-not-report" not in str(excinfo.value)
    assert list(staging_root.iterdir()) == []


@pytest.mark.parametrize("remote_filesystem", DESTINATION_BACKENDS, indirect=True)
def test_remote_destination_writes_native_parquet(remote_filesystem):
    """S3 and Azure destinations write data readable through their native SDKs."""
    prefix = "destination"
    result = run_ingest(
        source_uri="csv://tests/assets/create_replace.csv",
        dest_uri=remote_filesystem.destination_uri,
        source_table="stocks",
        dest_table=remote_filesystem.destination_table(prefix, "stocks"),
        loader_file_format="parquet",
        progress="log",
    )

    assert result is not None
    rows = parquet_rows(remote_filesystem.objects(f"{prefix}/stocks"))
    assert len(rows) == 20
    assert {row["symbol"] for row in rows} >= {"A", "AA", "AAOI"}


def test_s3_to_azure_round_trip(s3_emulator, azure_emulator):
    """Rows read from S3 can be written to Azure without a local staging source."""
    source_key = "round-trip/create_replace.csv"
    s3_emulator.upload(source_key, Path("tests/assets/create_replace.csv").read_bytes())
    prefix = "round-trip"

    result = run_ingest(
        source_uri=s3_emulator.source_uri,
        dest_uri=azure_emulator.destination_uri,
        source_table=s3_emulator.source_table(source_key),
        dest_table=azure_emulator.destination_table(prefix, "stocks"),
        loader_file_format="parquet",
        progress="log",
    )

    assert result is not None
    rows = parquet_rows(azure_emulator.objects(f"{prefix}/stocks"))
    assert len(rows) == 20
    assert {row["symbol"] for row in rows} >= {"A", "AA", "AAOI"}


def test_s3_filesystem_incremental_uses_persistent_cursor(
    s3_emulator, tmp_path, monkeypatch
):
    """Only new S3 objects load, and run options stay out of the S3 constructor."""
    import s3fs

    captured_constructor_kwargs = []
    original_class = s3fs.S3FileSystem

    class CapturingS3FileSystem(original_class):
        def __init__(self, *args, **kwargs):
            captured_constructor_kwargs.append(kwargs.copy())
            super().__init__(*args, **kwargs)

    CapturingS3FileSystem.clear_instance_cache()
    monkeypatch.setattr(s3fs, "S3FileSystem", CapturingS3FileSystem)

    opened = []
    original_open = FileItemDict.open

    def recording_open(self, *args, **kwargs):
        opened.append(str(self["file_url"]).rsplit("/", 1)[-1])
        return original_open(self, *args, **kwargs)

    monkeypatch.setattr(FileItemDict, "open", recording_open)

    s3_emulator.upload("incremental/part-1.csv", CSV_ROWS)
    db_path = tmp_path / "incremental.duckdb"
    pipelines_dir = tmp_path / "pipelines"
    load_kwargs = {
        "source_uri": s3_emulator.source_uri,
        "dest_uri": f"duckdb:///{db_path}",
        "source_table": s3_emulator.source_table("incremental/*.csv"),
        "dest_table": "testdrive.data",
        "filesystem_incremental": True,
        "pipelines_dir": str(pipelines_dir),
        "progress": "log",
    }

    assert run_ingest(**load_kwargs) is not None
    counts = [duckdb_table_cardinality(db_path, "testdrive.data")]
    assert opened == ["part-1.csv"]

    opened.clear()
    run_ingest(**load_kwargs)
    counts.append(duckdb_table_cardinality(db_path, "testdrive.data"))
    assert opened == []

    # S3 LastModified values have one-second resolution. Cross a full timestamp
    # boundary so the incremental cursor cannot treat the new object as a tie.
    sleep(1.1)
    s3_emulator.upload("incremental/part-2.csv", b"name,value\nCarol,3\n")
    assert run_ingest(**load_kwargs) is not None
    counts.append(duckdb_table_cardinality(db_path, "testdrive.data"))
    assert opened == ["part-2.csv"]

    assert counts == [2, 2, 3]
    assert pipelines_dir.is_dir()
    assert captured_constructor_kwargs
    assert all(
        "filesystem_incremental" not in kwargs for kwargs in captured_constructor_kwargs
    )
