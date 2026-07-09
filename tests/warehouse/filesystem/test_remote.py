import csv
import gzip
import importlib.util
import io
import json
from typing import Callable, Iterable
from unittest.mock import patch

import fsspec
import pyarrow.csv
import pytest
import sqlalchemy
from dlt.common.storages.fsspec_filesystem import glob_files
from fsspec.implementations.memory import MemoryFileSystem
from fsspec.registry import _registry as _fsspec_registry
from pyarrow import parquet as pya_parquet

from omniload.error import InvalidBlobTableError, MissingValueError
from omniload.target.filesystem.api import AzureDestination, S3Destination
from tests.util import invoke_ingest_command
from tests.util.common import get_random_string, has_exception
from tests.warehouse.settings import DESTINATIONS

# The MessagePack reader needs the optional `iterable` extra (iterabledata + msgpack). Probe
# with find_spec so this module imports without them; the msgpack case is added only when
# both are present.
HAS_MSGPACK = (
    importlib.util.find_spec("iterable") is not None
    and importlib.util.find_spec("msgpack") is not None
)
HAS_CBOR = (
    importlib.util.find_spec("iterable") is not None
    and importlib.util.find_spec("cbor2") is not None
)


def fs_test_cases(
    protocol: str,
    target_fs: str,
    auth: str,
) -> Iterable[Callable]:
    """
    Tests for filesystem based sources
    """
    testdata = (
        "name,phone,email,country\n"
        "Rajah Roach,1-459-646-7421,adipiscing.ligula@outlook.net,Austria\n"
        "Kiayada Jackson,(341) 484-6523,velit.egestas.lacinia@hotmail.couk,Norway\n"
        "Bradley Grant,1-329-268-4178,leo.cras@hotmail.org,Chile\n"
        "Damian Velasquez,(462) 744-9637,phasellus.fermentum@outlook.ca,South Africa\n"
        "Rina Nicholson,(201) 971-6463,neque.nullam.ut@yahoo.net,Brazil\n"
    )
    testdata_extended = (
        "name,phone,email,country\n"
        "Irene Douglas,(223) 971-6463,flying.fish.kick@gmail.com,UK\n"
    )
    test_fs = MemoryFileSystem()

    # for CSV tests
    with test_fs.open("/data.csv", "w") as f:
        f.write(testdata)
    with test_fs.open("/data.csv.gz", "wb") as f:
        with gzip.GzipFile(fileobj=f, mode="wb") as gz:
            gz.write(testdata.encode())

    # for Glob tests
    with test_fs.open("/data2.csv", "w") as f:
        f.write(testdata_extended)

    # For Parquet tests
    with test_fs.open("/data.parquet", "wb") as f:
        table = pyarrow.csv.read_csv(io.BytesIO(testdata.encode()))
        pya_parquet.write_table(table, f)
    with io.BytesIO() as buf:
        pya_parquet.write_table(table, buf)
        buf.seek(0)
        with test_fs.open("/data.parquet.gz", "wb") as f:
            with gzip.GzipFile(fileobj=f, mode="wb") as gz:
                gz.write(buf.getvalue())

    # For JSONL tests
    with test_fs.open("/data.jsonl", "w") as f:
        reader = csv.DictReader(io.StringIO(testdata))
        for row in reader:
            json.dump(row, f)
            f.write("\n")
    with test_fs.open("/data.jsonl.gz", "wb") as f:
        with gzip.GzipFile(fileobj=f, mode="wb") as gz:
            reader = csv.DictReader(io.StringIO(testdata))
            for row in reader:
                gz.write(json.dumps(row).encode())
                gz.write(b"\n")

    # For MessagePack tests (read through the iterabledata bridge). Written as a stream of
    # concatenated maps, the same on-disk form the reader expects.
    if HAS_MSGPACK:
        import msgpack

        msgpack_rows = [
            {"name": row["name"], "country": row["country"]}
            for row in csv.DictReader(io.StringIO(testdata))
        ]
        with test_fs.open("/data.msgpack", "wb") as f:
            for row in msgpack_rows:
                f.write(msgpack.packb(row, use_bin_type=True))

    # For CBOR tests (read through the iterabledata bridge). Written as a single top-level
    # array, the shape the reader requires (concatenated objects would truncate to the first).
    if HAS_CBOR:
        import cbor2

        cbor_rows = [
            {"name": row["name"], "country": row["country"]}
            for row in csv.DictReader(io.StringIO(testdata))
        ]
        with test_fs.open("/data.cbor", "wb") as f:
            f.write(cbor2.dumps(cbor_rows))

    # for testing unsupported files
    with test_fs.open("/bin/data.bin", "w") as f:
        f.write("BINARY")

    def glob_files_override(fs_client, _, file_glob):
        return glob_files(fs_client, "memory://", file_glob)

    def assert_rows(dest_uri, dest_table, n):
        engine = sqlalchemy.create_engine(dest_uri)
        with engine.connect() as conn:
            rows = conn.exec_driver_sql(f"select count(*) from {dest_table}").fetchall()
        engine.dispose()
        assert len(rows) == 1
        assert rows[0] == (n,)

    def test_empty_source_uri(dest_uri):
        """
        When the source URI is empty, an error should be raised.
        """
        schema = f"testschema_fs_{get_random_string(5)}"
        result = invoke_ingest_command(
            f"{protocol}://bucket?{auth}",
            "",
            dest_uri,
            f"{schema}.test",
            print_output=False,
        )
        assert has_exception(result.exception, InvalidBlobTableError)

    def test_unsupported_file_format(dest_uri):
        """
        When the source file is not one of [csv, parquet, jsonl] it should
        raise an exception
        """
        with (
            patch(target_fs),
            patch(
                "omniload.source.filesystem.adapter.glob_files",
                wraps=glob_files_override,
            ),
        ):
            schema_rand_prefix = f"testschema_fs_{get_random_string(5)}"
            dest_table = f"{schema_rand_prefix}.fs_{get_random_string(5)}"
            result = invoke_ingest_command(
                f"{protocol}://bucket?{auth}",
                "/bin/data.bin",
                dest_uri,
                dest_table,
                print_output=False,
            )
            assert result.exit_code != 0
            assert has_exception(result.exception, ValueError)

    def test_missing_credentials(dest_uri):
        """
        When the credentials are missing, an error should be raised.
        """
        schema_rand_prefix = f"testschema_fs_{get_random_string(5)}"
        dest_table = f"{schema_rand_prefix}.fs_{get_random_string(5)}"
        result = invoke_ingest_command(
            f"{protocol}://bucket",
            "/data.csv",
            dest_uri,
            dest_table,
            print_output=False,
        )
        assert result.exit_code != 0

    def test_csv_load(dest_uri):
        """
        When the source URI is a CSV file, the data should be ingested.
        """
        with (
            patch(target_fs) as target_fs_mock,
            patch(
                "omniload.source.filesystem.adapter.glob_files",
                wraps=glob_files_override,
            ),
        ):
            target_fs_mock.return_value = test_fs
            schema_rand_prefix = f"testschema_fs_{get_random_string(5)}"
            dest_table = f"{schema_rand_prefix}.fs_{get_random_string(5)}"
            result = invoke_ingest_command(
                f"{protocol}://bucket?{auth}",
                "/data.csv",
                dest_uri,
                dest_table,
            )
            assert result.exit_code == 0
            assert_rows(dest_uri, dest_table, 5)

    def test_csv_gz_load(dest_uri):
        """When the source URI is a gzipped CSV file, the data should be ingested."""
        with (
            patch(target_fs) as target_fs_mock,
            patch(
                "omniload.source.filesystem.adapter.glob_files",
                wraps=glob_files_override,
            ),
        ):
            target_fs_mock.return_value = test_fs
            schema_rand_prefix = f"testschema_fs_{get_random_string(5)}"
            dest_table = f"{schema_rand_prefix}.fs_{get_random_string(5)}"
            result = invoke_ingest_command(
                f"{protocol}://bucket?{auth}",
                "/data.csv.gz",
                dest_uri,
                dest_table,
            )
            assert result.exit_code == 0
            assert_rows(dest_uri, dest_table, 5)

    def test_parquet_load(dest_uri):
        """
        When the source URI is a Parquet file, the data should be ingested.
        """
        with (
            patch(target_fs) as target_fs_mock,
            patch(
                "omniload.source.filesystem.adapter.glob_files",
                wraps=glob_files_override,
            ),
        ):
            target_fs_mock.return_value = test_fs
            schema_rand_prefix = f"testschema_fs_{get_random_string(5)}"
            dest_table = f"{schema_rand_prefix}.fs_{get_random_string(5)}"
            result = invoke_ingest_command(
                f"{protocol}://bucket?{auth}",
                "/data.parquet",
                dest_uri,
                dest_table,
            )
            assert result.exit_code == 0
            assert_rows(dest_uri, dest_table, 5)

    def test_parquet_gz_load(dest_uri):
        """When the source URI is a gzipped Parquet file, the data should be ingested."""
        with (
            patch(target_fs) as target_fs_mock,
            patch(
                "omniload.source.filesystem.adapter.glob_files",
                wraps=glob_files_override,
            ),
        ):
            target_fs_mock.return_value = test_fs
            schema_rand_prefix = f"testschema_fs_{get_random_string(5)}"
            dest_table = f"{schema_rand_prefix}.fs_{get_random_string(5)}"
            result = invoke_ingest_command(
                f"{protocol}://bucket?{auth}",
                "/data.parquet.gz",
                dest_uri,
                dest_table,
            )
            assert result.exit_code == 0
            assert_rows(dest_uri, dest_table, 5)

    def test_jsonl_load(dest_uri):
        """
        When the source URI is a JSONL file, the data should be ingested.
        """
        with (
            patch(target_fs) as target_fs_mock,
            patch(
                "omniload.source.filesystem.adapter.glob_files",
                wraps=glob_files_override,
            ),
        ):
            target_fs_mock.return_value = test_fs
            schema_rand_prefix = f"testschema_fs_{get_random_string(5)}"
            dest_table = f"{schema_rand_prefix}.fs_{get_random_string(5)}"
            result = invoke_ingest_command(
                f"{protocol}://bucket?{auth}",
                "/data.jsonl",
                dest_uri,
                dest_table,
            )
            assert result.exit_code == 0
            assert_rows(dest_uri, dest_table, 5)

    def test_jsonl_gz_load(dest_uri):
        """When the source URI is a gzipped JSONL file, the data should be ingested."""
        with (
            patch(target_fs) as target_fs_mock,
            patch(
                "omniload.source.filesystem.adapter.glob_files",
                wraps=glob_files_override,
            ),
        ):
            target_fs_mock.return_value = test_fs
            schema_rand_prefix = f"testschema_fs_{get_random_string(5)}"
            dest_table = f"{schema_rand_prefix}.fs_{get_random_string(5)}"
            result = invoke_ingest_command(
                f"{protocol}://bucket?{auth}",
                "/data.jsonl.gz",
                dest_uri,
                dest_table,
            )
            assert result.exit_code == 0
            assert_rows(dest_uri, dest_table, 5)

    def test_msgpack_load(dest_uri):
        """When the source URI is a MessagePack file, the data should be ingested through the
        iterabledata bridge over the source's own fsspec handle (no separate storage auth)."""
        with (
            patch(target_fs) as target_fs_mock,
            patch(
                "omniload.source.filesystem.adapter.glob_files",
                wraps=glob_files_override,
            ),
        ):
            target_fs_mock.return_value = test_fs
            schema_rand_prefix = f"testschema_fs_{get_random_string(5)}"
            dest_table = f"{schema_rand_prefix}.fs_{get_random_string(5)}"
            result = invoke_ingest_command(
                f"{protocol}://bucket?{auth}",
                "/data.msgpack",
                dest_uri,
                dest_table,
            )
            assert result.exit_code == 0
            assert_rows(dest_uri, dest_table, 5)

    def test_cbor_load(dest_uri):
        """When the source URI is a CBOR file, the data should be ingested through the
        iterabledata bridge over the source's own fsspec handle (no separate storage auth)."""
        with (
            patch(target_fs) as target_fs_mock,
            patch(
                "omniload.source.filesystem.adapter.glob_files",
                wraps=glob_files_override,
            ),
        ):
            target_fs_mock.return_value = test_fs
            schema_rand_prefix = f"testschema_fs_{get_random_string(5)}"
            dest_table = f"{schema_rand_prefix}.fs_{get_random_string(5)}"
            result = invoke_ingest_command(
                f"{protocol}://bucket?{auth}",
                "/data.cbor",
                dest_uri,
                dest_table,
            )
            assert result.exit_code == 0
            assert_rows(dest_uri, dest_table, 5)

    def test_glob_load(dest_uri):
        """
        When the source URI is a glob pattern, all files matching the pattern should be ingested
        """
        with (
            patch(target_fs) as target_fs_mock,
            patch(
                "omniload.source.filesystem.adapter.glob_files",
                wraps=glob_files_override,
            ),
        ):
            target_fs_mock.return_value = test_fs
            schema_rand_prefix = f"testschema_fs_{get_random_string(5)}"
            dest_table = f"{schema_rand_prefix}.fs_{get_random_string(5)}"
            result = invoke_ingest_command(
                f"{protocol}://bucket?{auth}",
                "/*.csv",
                dest_uri,
                dest_table,
            )
            assert result.exit_code == 0
            assert_rows(dest_uri, dest_table, 6)

    def test_compound_table_name(dest_uri):
        """
        When table contains both the bucket name and the file glob,
        loads should be successful.
        """
        with (
            patch(target_fs) as target_fs_mock,
            patch(
                "omniload.source.filesystem.adapter.glob_files",
                wraps=glob_files_override,
            ),
        ):
            target_fs_mock.return_value = test_fs
            schema_rand_prefix = f"testschema_fs_{get_random_string(5)}"
            dest_table = f"{schema_rand_prefix}.fs_{get_random_string(5)}"
            result = invoke_ingest_command(
                f"{protocol}://?{auth}",
                "bucket/*.csv",
                dest_uri,
                dest_table,
            )
            assert result.exit_code == 0
            assert_rows(dest_uri, dest_table, 6)

    def test_uri_precedence(dest_uri):
        """
        When file glob is present in both URI and Source Table,
        the URI glob should be used
        """

        with (
            patch(target_fs) as target_fs_mock,
            patch(
                "omniload.source.filesystem.adapter.glob_files",
                wraps=glob_files_override,
            ),
        ):
            target_fs_mock.return_value = test_fs
            schema_rand_prefix = f"testschema_fs_{get_random_string(5)}"
            dest_table = f"{schema_rand_prefix}.fs_{get_random_string(5)}"
            result = invoke_ingest_command(
                f"{protocol}://bucket/*.csv?{auth}",
                "/path/to/file",  # if this is used, it should result in an error
                dest_uri,
                dest_table,
            )
            assert result.exit_code == 0
            assert_rows(dest_uri, dest_table, 6)

    cases = [
        test_empty_source_uri,
        test_missing_credentials,
        test_unsupported_file_format,
        test_csv_load,
        test_csv_gz_load,
        test_parquet_load,
        test_parquet_gz_load,
        test_jsonl_load,
        test_jsonl_gz_load,
        test_glob_load,
        test_compound_table_name,
        test_uri_precedence,
    ]
    if HAS_MSGPACK:
        cases.append(test_msgpack_load)
    if HAS_CBOR:
        cases.append(test_cbor_load)
    return cases


@pytest.mark.parametrize(
    "dest", list(DESTINATIONS.values()), ids=list(DESTINATIONS.keys())
)
@pytest.mark.parametrize(
    "test_case",
    fs_test_cases(
        "gs",
        "gcsfs.GCSFileSystem",
        "credentials_base64=e30K",  # base 64 for "{}"
    ),
)
def test_gcs(dest, test_case):
    test_case(dest.start())
    dest.stop()


@pytest.mark.parametrize(
    "dest", list(DESTINATIONS.values()), ids=list(DESTINATIONS.keys())
)
@pytest.mark.parametrize(
    "test_case",
    fs_test_cases(
        "s3",
        "s3fs.S3FileSystem",
        "access_key_id=KEY&secret_access_key=SECRET",
    ),
)
def test_s3(dest, test_case):
    test_case(dest.start())
    dest.stop()


def test_s3_destination():
    # should raise an error if endpoint_url doesn't have a scheme or a host
    with pytest.raises(ValueError, match="Invalid endpoint_url"):
        S3Destination().dlt_dest(
            uri="s3://?access_key_id=KEY&secret_access_key=SECRET&endpoint_url=localhost:9000",
            dest_table="bucket/test_table",
        )


# adlfs serves Azure Blob and ADLS Gen2 through one AzureBlobFileSystem and the
# az:// bucket-url scheme, so az://, adls://, and abfss:// share the exact same
# source read matrix (each is a registry alias onto AzureSource). They run
# through the same parametrized harness as S3/GCS, so a regression in shared
# filesystem plumbing fails Azure too.
@pytest.mark.parametrize(
    "dest", list(DESTINATIONS.values()), ids=list(DESTINATIONS.keys())
)
@pytest.mark.parametrize(
    "test_case",
    fs_test_cases(
        "az",
        "adlfs.AzureBlobFileSystem",
        "account_name=acct&account_key=a2V5",
    ),
)
def test_az(dest, test_case):
    test_case(dest.start())
    dest.stop()


@pytest.mark.parametrize(
    "dest", list(DESTINATIONS.values()), ids=list(DESTINATIONS.keys())
)
@pytest.mark.parametrize(
    "test_case",
    fs_test_cases(
        "adls",
        "adlfs.AzureBlobFileSystem",
        "account_name=acct&account_key=a2V5",
    ),
)
def test_adls(dest, test_case):
    test_case(dest.start())
    dest.stop()


@pytest.mark.parametrize(
    "dest", list(DESTINATIONS.values()), ids=list(DESTINATIONS.keys())
)
@pytest.mark.parametrize(
    "test_case",
    fs_test_cases(
        "abfss",
        "adlfs.AzureBlobFileSystem",
        "account_name=acct&account_key=a2V5",
    ),
)
def test_abfss(dest, test_case):
    test_case(dest.start())
    dest.stop()


# A real MemoryFileSystem subclass standing in for adlfs.AzureBlobFileSystem in
# destination tests. dlt resolves an az:// *destination* through fsspec's
# url_to_fs (not through omniload code), which calls class-level methods on the
# resolved filesystem class, so the mock must be a real MemoryFileSystem
# subclass rather than a MagicMock. It shares MemoryFileSystem's class-level
# store (writes are readable back) and records every construction's kwargs into
# a module-level list, so tests can assert the dlt -> to_adlfs_credentials ->
# adlfs mapping actually reached fsspec. It is a single shared class (not a
# fresh subclass per test) because fsspec caches the resolved class for a
# protocol process-wide; each test clears the capture list and instance cache up
# front, and _write_to_azure force-registers it for the "az" protocol.
_AZURE_FS_CAPTURED: list[dict] = []


class _CapturingAzureBlobFileSystem(MemoryFileSystem):
    protocol = "abfs"

    def __init__(self, *args, **kwargs):
        _AZURE_FS_CAPTURED.append(kwargs)
        super().__init__()


def _azure_dest_parquet_rows(container: str) -> int:
    """Row count across the parquet data files a destination wrote for ``container``."""
    total = 0
    prefix = f"az://{container}/output/"
    for key, value in MemoryFileSystem.store.items():
        if key.startswith(prefix) and key.endswith(".parquet"):
            data = (
                value.getvalue()
                if hasattr(value, "getvalue")
                else bytes(value.getbuffer())
            )
            total += pya_parquet.read_table(io.BytesIO(data)).num_rows
    return total


def _write_to_azure(auth: str, scheme: str = "az"):
    """Run a ``csv://`` -> ``<scheme>://`` ingest against a mocked adlfs backend.

    ``scheme`` is the destination URI scheme (``az`` / ``adls`` / ``abfss``);
    all three dispatch to ``AzureDestination`` and resolve to an ``az://``
    bucket internally, so the fsspec registration is always for ``"az"``.

    Uses the 20-row ``create_replace.csv`` fixture and forces parquet so the
    landed rows are deterministic to read back. Returns
    ``(result, container, captured_kwargs)``; the container name is unique per
    call so the shared in-memory store never collides across tests.
    """
    container = f"container_{get_random_string(8)}"
    _AZURE_FS_CAPTURED.clear()
    # fsspec caches resolved classes and instances process-wide; force our
    # capturing class for "az" and drop any cached instance so __init__ (and
    # therefore the capture) runs for this call regardless of prior tests.
    _CapturingAzureBlobFileSystem.clear_instance_cache()
    prev_az = _fsspec_registry.get("az")
    fsspec.register_implementation("az", _CapturingAzureBlobFileSystem, clobber=True)
    try:
        with patch("adlfs.AzureBlobFileSystem", _CapturingAzureBlobFileSystem):
            result = invoke_ingest_command(
                "csv://omniload/testdata/create_replace.csv",
                "testschema.input",
                f"{scheme}://?{auth}",
                f"{container}/output",
                loader_file_format="parquet",
                print_output=False,
            )
    finally:
        # restore any prior "az" registration rather than blindly dropping it
        if prev_az is not None:
            _fsspec_registry["az"] = prev_az
        else:
            _fsspec_registry.pop("az", None)
    return result, container, list(_AZURE_FS_CAPTURED)


def test_azure_destination_write_through():
    """A mocked ``az://`` write lands rows in the mocked filesystem.

    Exercises the ``AzureCredentials`` -> ``to_adlfs_credentials`` -> adlfs
    mapping through the real BlobFS/dlt pipeline, which an error-path-only test
    never touches (an ``azure_storage_account_name`` vs ``account_name`` typo
    would otherwise slip through). S3/GCS have no equivalent destination
    write-through test; this closes that gap for the shared blob base class.
    """
    result, container, captured = _write_to_azure("account_name=acct&account_key=a2V5")
    assert result.exit_code == 0
    assert _azure_dest_parquet_rows(container) == 20
    # the account_name + key actually reached the adlfs backend
    assert any(c.get("account_name") == "acct" for c in captured)
    assert any(c.get("account_key") == "a2V5" for c in captured)


def test_azure_destination_service_principal_write_through():
    """A service-principal ``az://`` write lands rows and forwards the triplet."""
    result, container, captured = _write_to_azure(
        "account_name=acct&tenant_id=t&client_id=c&client_secret=s"
    )
    assert result.exit_code == 0
    assert _azure_dest_parquet_rows(container) == 20
    assert any(
        c.get("tenant_id") == "t"
        and c.get("client_id") == "c"
        and c.get("client_secret") == "s"
        for c in captured
    )


def test_azure_destination_abfss_write_through():
    """The ``abfss://`` alias dispatches to AzureDestination and writes through
    the ``az://`` backend, matching the source-side alias parametrization."""
    result, container, _ = _write_to_azure(
        "account_name=acct&account_key=a2V5", scheme="abfss"
    )
    assert result.exit_code == 0
    assert _azure_dest_parquet_rows(container) == 20


def test_azure_destination_encoded_account_key():
    """A base64 account_key (``+`` ``/`` ``=``), URL-encoded in the URI, survives
    ``parse_qs`` and reaches adlfs decoded."""
    # account_key "a2V5+x/==" percent-encoded
    result, container, captured = _write_to_azure(
        "account_name=acct&account_key=a2V5%2Bx%2F%3D%3D"
    )
    assert result.exit_code == 0
    assert _azure_dest_parquet_rows(container) == 20
    assert any(c.get("account_key") == "a2V5+x/==" for c in captured)


def test_azure_destination_encoded_sas_token():
    """A SAS token embedding its own ``&`` / ``=`` pairs, URL-encoded in the
    URI, reaches adlfs intact rather than shattering into junk params."""
    # sas_token "sv=2021&sig=a/b+c==" percent-encoded
    result, container, captured = _write_to_azure(
        "account_name=acct&sas_token=sv%3D2021%26sig%3Da%2Fb%2Bc%3D%3D"
    )
    assert result.exit_code == 0
    assert _azure_dest_parquet_rows(container) == 20
    assert any(c.get("sas_token") == "sv=2021&sig=a/b+c==" for c in captured)


def test_azure_destination_account_host():
    """``account_host`` (sovereign cloud / custom endpoint) is forwarded to adlfs."""
    result, container, captured = _write_to_azure(
        "account_name=acct&account_key=a2V5"
        "&account_host=acct.blob.core.chinacloudapi.cn"
    )
    assert result.exit_code == 0
    assert any(
        c.get("account_host") == "acct.blob.core.chinacloudapi.cn" for c in captured
    )


def test_azure_destination_missing_credentials():
    with pytest.raises(MissingValueError):
        AzureDestination().dlt_dest(
            uri="az://?account_name=acct",
            dest_table="container/test_table",
        )


def test_azure_destination_missing_account_name():
    with pytest.raises(MissingValueError, match="account_name"):
        AzureDestination().dlt_dest(
            uri="az://?account_key=a2V5",
            dest_table="container/test_table",
        )


def test_azure_destination_partial_service_principal():
    # a partial SP triplet names the missing field instead of falling through to
    # an opaque adlfs/Azure-SDK error
    with pytest.raises(MissingValueError, match="client_secret"):
        AzureDestination().dlt_dest(
            uri="az://?account_name=acct&tenant_id=t&client_id=c",
            dest_table="container/test_table",
        )


def test_azure_destination_conflicting_auth():
    # supplying both account-key and service-principal material is rejected
    # rather than silently picking one
    with pytest.raises(ValueError, match="Conflicting Azure credentials"):
        AzureDestination().dlt_dest(
            uri=(
                "az://?account_name=acct&account_key=a2V5"
                "&tenant_id=t&client_id=c&client_secret=s"
            ),
            dest_table="container/test_table",
        )


def test_azure_destination_account_key_and_sas_conflict():
    # account_key and sas_token are mutually exclusive; supplying both is
    # rejected rather than leaving the pick to dlt/adlfs
    with pytest.raises(ValueError, match="Conflicting Azure credentials"):
        AzureDestination().dlt_dest(
            uri="az://?account_name=acct&account_key=a2V5&sas_token=sv%3Dx",
            dest_table="container/test_table",
        )


def test_azure_source_encoded_credentials(tmp_path):
    """A base64 account_key, URL-encoded in the *source* URI, reaches adlfs
    decoded (the source constructs the filesystem directly)."""
    test_fs = MemoryFileSystem()
    with test_fs.open("/data.csv", "w") as f:
        f.write("name,country\nAda,UK\nBob,US\n")

    def glob_files_override(fs_client, _, file_glob):
        return glob_files(fs_client, "memory://", file_glob)

    dest_uri = f"duckdb:///{tmp_path / 'azure_src.db'}"
    with (
        patch("adlfs.AzureBlobFileSystem") as fs_mock,
        patch(
            "omniload.source.filesystem.adapter.glob_files",
            wraps=glob_files_override,
        ),
    ):
        fs_mock.return_value = test_fs
        schema = f"testschema_fs_{get_random_string(5)}"
        result = invoke_ingest_command(
            "az://bucket?account_name=acct&account_key=a2V5%2Bx%2F%3D%3D",
            "/data.csv",
            dest_uri,
            f"{schema}.out",
        )
        assert result.exit_code == 0
        # adlfs was constructed with the DECODED key, not the raw percent-encoding
        _, kwargs = fs_mock.call_args
        assert kwargs.get("account_name") == "acct"
        assert kwargs.get("account_key") == "a2V5+x/=="
