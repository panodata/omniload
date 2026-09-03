import gzip
import io
import json
import sqlite3
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
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

#: The newest `x-ms-version` this Azurite release serves. The Python SDK used
#: to arrange fixtures sends a newer one by default; Arrow, which the source
#: reads through, offers no such override and relies on the emulator being
#: started with `--skipApiVersionCheck` (see `AZURITE_COMMAND`).
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

    uri = (
        f"s3://?endpoint_url={endpoint_url}&access_key_id=test"
        "&secret_access_key=test&region=us-west-1"
    )
    return RemoteFilesystemEmulator("s3", uri, uri, bucket, upload, objects)


@pytest.fixture
def azure_emulator() -> RemoteFilesystemEmulator:
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

    uri = f"az://?connection_string={quote(connection_string, safe='')}"
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


def test_r2_scheme_reads_s3_compatible_storage(s3_emulator, tmp_path):
    """`r2://` lists and reads, though dlt's own listing knows no such scheme.

    R2 speaks the S3 API, so the S3 emulator serves it; the scheme is what this
    covers, because listing resolves each file's modification date per scheme.
    """
    s3_emulator.upload("r2/part-1.csv", CSV_ROWS)
    s3_emulator.upload("r2/part-2.csv", CSV_ROWS)

    db_path = tmp_path / "r2.duckdb"
    result = run_ingest(
        source_uri=s3_emulator.source_uri.replace("s3://", "r2://", 1),
        dest_uri=f"duckdb:///{db_path}",
        source_table=s3_emulator.source_table("r2/*.csv"),
        dest_table="testdrive.data",
        progress="log",
    )

    assert result is not None
    assert duckdb_table_cardinality(db_path, "testdrive.data") == 4


@pytest.mark.parametrize("scheme", ["s3", "r2"])
def test_s3_compatible_glob_reads_keys_with_url_delimiters(
    s3_emulator, tmp_path, scheme
):
    """Discovered object keys keep literal URL delimiters when they are opened."""
    s3_emulator.upload("delimiters/part#1.csv", CSV_ROWS)
    s3_emulator.upload("delimiters/part?2.csv", CSV_ROWS)

    db_path = tmp_path / f"{scheme}-url-delimiters.duckdb"
    result = run_ingest(
        source_uri=s3_emulator.source_uri.replace("s3://", f"{scheme}://", 1),
        dest_uri=f"duckdb:///{db_path}",
        source_table=s3_emulator.source_table("delimiters/*.csv"),
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
        source_uri="file://tests/assets/create_replace.csv",
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
    import pyarrow.fs

    captured_constructor_kwargs = []
    original_class = pyarrow.fs.S3FileSystem

    def capturing_s3_filesystem(*args, **kwargs):
        captured_constructor_kwargs.append(kwargs.copy())
        return original_class(*args, **kwargs)

    monkeypatch.setattr(pyarrow.fs, "S3FileSystem", capturing_s3_filesystem)

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


@pytest.mark.parametrize("scheme", ["az", "adls", "abfss"])
def test_azure_schemes_read_through_one_client(azure_emulator, tmp_path, scheme):
    """Each Azure user-scheme is an alias onto the same Arrow client."""
    azure_emulator.upload(f"schemes/{scheme}.csv", CSV_ROWS)

    db_path = tmp_path / f"{scheme}.duckdb"
    result = run_ingest(
        source_uri=azure_emulator.source_uri.replace("az://", f"{scheme}://", 1),
        dest_uri=f"duckdb:///{db_path}",
        source_table=azure_emulator.source_table(f"schemes/{scheme}.csv"),
        dest_table="testdrive.data",
        progress="log",
    )

    assert result is not None
    assert duckdb_table_cardinality(db_path, "testdrive.data") == 2


def test_azure_glob_reads_blob_names_with_url_delimiters(azure_emulator, tmp_path):
    """Discovered blob names keep literal URL delimiters when they are opened.

    A blob name is an opaque string, so it may carry characters that mean
    something in a URL: the delimiters `#` and `?`, and an escape that must
    reach the wire without being decoded first.
    """
    azure_emulator.upload("delimiters/part#1.csv", CSV_ROWS)
    azure_emulator.upload("delimiters/part?2.csv", CSV_ROWS)
    azure_emulator.upload("delimiters/part%2F3.csv", CSV_ROWS)

    db_path = tmp_path / "azure-url-delimiters.duckdb"
    result = run_ingest(
        source_uri=azure_emulator.source_uri,
        dest_uri=f"duckdb:///{db_path}",
        source_table=azure_emulator.source_table("delimiters/*.csv"),
        dest_table="testdrive.data",
        progress="log",
    )

    assert result is not None
    assert duckdb_table_cardinality(db_path, "testdrive.data") == 6


class RecordingArrowFilesystem:
    """Forward to a native Arrow client, keeping every request it was handed."""

    def __init__(self, native, calls: list):
        self._native = native
        self._calls = calls

    def get_file_info(self, selector):
        self._calls.append(selector)
        return self._native.get_file_info(selector)

    def __getattr__(self, name):
        return getattr(self._native, name)


def _azure_arrow_filesystem(connection_string: str, calls: list):
    """Build the client the source builds, recording each native request."""
    from dlt_filesystem.source.impl.remote import AzureSource, _AzureArrowFSWrapper
    from dlt_filesystem.util.auth import (
        azure_arrow_filesystem_kwargs,
        parse_azure_blob_auth,
    )

    auth = parse_azure_blob_auth({"connection_string": [connection_string]})
    source = AzureSource()
    native = source.fs_class(**azure_arrow_filesystem_kwargs(auth))
    return _AzureArrowFSWrapper(RecordingArrowFilesystem(native, calls))


def test_azure_selection_shapes_use_one_native_listing_request(azure_emulator):
    """Every selection shape costs one Arrow request, recursive only when needed."""
    from pyarrow.fs import FileSelector

    from dlt_filesystem.source.lister import glob_files

    azure_emulator.upload("listing/part-1.csv", CSV_ROWS)
    azure_emulator.upload("listing/nested/part-2.csv", CSV_ROWS)
    source_fields = dict(parse_qsl(azure_emulator.source_uri.partition("?")[2]))
    connection_string = source_fields["connection_string"]
    bucket_url = f"az://{azure_emulator.namespace}"
    prefix = f"{azure_emulator.namespace}/listing"

    def discover(file_glob: str) -> tuple[list[str], list]:
        calls: list = []
        fs = _azure_arrow_filesystem(connection_string, calls)
        files = list(glob_files(fs, bucket_url, file_glob))
        return [file["relative_path"] for file in files], calls

    shallow, shallow_calls = discover("listing/*.csv")
    assert shallow == ["listing/part-1.csv"]
    assert len(shallow_calls) == 1
    assert isinstance(shallow_calls[0], FileSelector)
    assert shallow_calls[0].base_dir == prefix
    assert shallow_calls[0].recursive is False

    recursive, recursive_calls = discover("listing/**/*.csv")
    assert sorted(recursive) == ["listing/nested/part-2.csv", "listing/part-1.csv"]
    assert len(recursive_calls) == 1
    assert recursive_calls[0].base_dir == prefix
    assert recursive_calls[0].recursive is True

    concrete, concrete_calls = discover("listing/part-1.csv")
    assert concrete == ["listing/part-1.csv"]
    assert concrete_calls == [[f"{prefix}/part-1.csv"]]

    unmatched, unmatched_calls = discover("listing/missing/*.csv")
    assert unmatched == []
    assert len(unmatched_calls) == 1
    assert unmatched_calls[0].allow_not_found is True


def test_azure_filesystem_incremental_uses_persistent_cursor(
    azure_emulator, tmp_path, monkeypatch
):
    """Only new blobs load, and run options stay out of the Azure constructor."""
    import pyarrow.fs

    captured_constructor_kwargs = []
    original_class = pyarrow.fs.AzureFileSystem

    def capturing_azure_filesystem(*args, **kwargs):
        captured_constructor_kwargs.append(kwargs.copy())
        return original_class(*args, **kwargs)

    monkeypatch.setattr(pyarrow.fs, "AzureFileSystem", capturing_azure_filesystem)

    opened = []
    original_open = FileItemDict.open

    def recording_open(self, *args, **kwargs):
        opened.append(str(self["file_url"]).rsplit("/", 1)[-1])
        return original_open(self, *args, **kwargs)

    monkeypatch.setattr(FileItemDict, "open", recording_open)

    azure_emulator.upload("incremental/part-1.csv", CSV_ROWS)
    db_path = tmp_path / "incremental.duckdb"
    pipelines_dir = tmp_path / "pipelines"
    load_kwargs = {
        "source_uri": azure_emulator.source_uri,
        "dest_uri": f"duckdb:///{db_path}",
        "source_table": azure_emulator.source_table("incremental/*.csv"),
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

    # Azure last-modified values have one-second resolution. Cross a full
    # timestamp boundary so the cursor cannot treat the new blob as a tie.
    sleep(1.1)
    azure_emulator.upload("incremental/part-2.csv", b"name,value\nCarol,3\n")
    assert run_ingest(**load_kwargs) is not None
    counts.append(duckdb_table_cardinality(db_path, "testdrive.data"))
    assert opened == ["part-2.csv"]

    assert counts == [2, 2, 3]
    assert pipelines_dir.is_dir()
    assert captured_constructor_kwargs
    assert all(
        "filesystem_incremental" not in kwargs for kwargs in captured_constructor_kwargs
    )


def test_azure_sas_token_reads_through_both_carriers(azure_emulator, tmp_path):
    """A SAS-authenticated read works from a URI parameter and a connection string.

    Both carriers yield the token without the `?` Arrow needs, and Arrow appends
    it to the account URL verbatim, so a construction-only test cannot tell a
    usable token from an unusable one. The emulator validates the signature, so
    this fails on a mangled token as well as a misplaced one.
    """
    from azure.core.utils import parse_connection_string
    from azure.storage.blob import (
        AccountSasPermissions,
        ResourceTypes,
        generate_account_sas,
    )

    source_fields = dict(parse_qsl(azure_emulator.source_uri.partition("?")[2]))
    settings = parse_connection_string(source_fields["connection_string"])
    account = settings["accountname"]
    blob_endpoint = settings["blobendpoint"]
    sas = generate_account_sas(
        account_name=account,
        account_key=settings["accountkey"],
        resource_types=ResourceTypes(service=True, container=True, object=True),
        permission=AccountSasPermissions(read=True, list=True),
        expiry=datetime.now(timezone.utc) + timedelta(hours=1),
    )
    # A generated signature is base64, so it carries the characters that a
    # carrier could mangle on the way through a query string.
    assert any(character in sas for character in "%+/=")

    azure_emulator.upload("sas/part-1.csv", CSV_ROWS)
    carriers = {
        "fields": (
            f"az://?account_name={account}&sas_token={quote(sas, safe='')}"
            f"&account_host={quote(blob_endpoint, safe='')}"
        ),
        "connection_string": "az://?connection_string="
        + quote(
            f"AccountName={account};BlobEndpoint={blob_endpoint};"
            f"SharedAccessSignature={sas};",
            safe="",
        ),
    }

    for name, source_uri in carriers.items():
        db_path = tmp_path / f"sas-{name}.duckdb"
        result = run_ingest(
            source_uri=source_uri,
            dest_uri=f"duckdb:///{db_path}",
            source_table=azure_emulator.source_table("sas/*.csv"),
            dest_table="testdrive.data",
            progress="log",
        )

        assert result is not None, name
        assert duckdb_table_cardinality(db_path, "testdrive.data") == 2, name
