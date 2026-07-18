import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import pytest

from omniload.core.factory import SourceDestinationFactory


@dataclass
class Item:
    uri: str
    table: Optional[str] = None


# Create private key file in PKCS8 format and compute fingerprint.
"""
openssl genrsa -out test.pem 512
openssl pkcs8 -nocrypt -topk8 -in test.pem -out test-pkcs8.pem
openssl rsa -pubout -outform DER -in test-pkcs8.pem | openssl md5 -c
"""
private_key_file = Path("tests/assets/privatekey.pem").absolute()
private_key_fingerprint = (
    Path("tests/assets/privatekey-fingerprint.txt").read_text().strip()
)

# A collection of filesystem source URIs without table parameter.
URIS = [
    # TODO: Review if account name is not already provided in the hostname per `acme`.
    "abfss://schrott@acme.dfs.core.windows.net/path/to/data.parquet?account_name=acme&account_key=secret",
    "adls://schrott@acme.dfs.core.windows.net/path/to/data.parquet?account_name=acme&account_key=secret",
    "az://schrott@acme.dfs.core.windows.net/path/to/data.parquet?account_name=acme&account_key=secret",
    # TODO: Mock `gs` backend.
    # ValueError: Provided token is either not valid, or expired.
    "gs://table-bucket-name/path/to/data.parquet?credentials_path=/path/to/service-account.json",
    # FIXME: KeyError: 'refresh_token'
    "gs://table-bucket-name/path/to/data.parquet?credentials_base64=eyJrZXkiOiAidmFsdWUifQ==",
    # TODO: Mock `hdfs` backend.
    #       OSError: Unable to load libjvm
    Item(
        uri="hdfs://example.com:8020/path/to/data.parquet?user=test",
        table="",
    ),
    Item(
        uri="hdfs://example.com:8020?user=test",
        table="path/to/data.parquet",
    ),
    f'oci://bucket@namespace/prefix/path/to/data.parquet?iam_type=api_key&config={{"user":"ocid1.user.oc1..24g4uzg","region":"us-ashburn-1","tenancy":"ocid1.tenancy.oc1..23423r3","key_file":"{private_key_file}","fingerprint":"{private_key_fingerprint}"}}',
    "oss://bucket/path/to/data.parquet?endpoint=http://oss-cn-hangzhou.aliyuncs.com/&key=foo&secret=bar",
    "oss://bucket/path/to/data.parquet?endpoint=https://oss-me-east-1.aliyuncs.com/&token=foobar",
    Item(
        uri="oss://?endpoint=https://oss-me-east-1.aliyuncs.com/&token=foobar",
        table="bucket/path/to/data.parquet",
    ),
    # TODO: Consolidate R2 and S3 to just use `key` and `secret`, like the fsspec implementations are
    #       doing it across the board, and like the OSS wrapper already started inheriting it.
    "r2://bucket/path/to/data.parquet?access_key_id=foo&secret_access_key=bar",
    "s3://bucket/path/to/data.parquet?access_key_id=foo&secret_access_key=bar",
    "sftp://username:password@example.com:2222/path/to/data.parquet",
]


@pytest.mark.parametrize("source_uri", URIS, ids=[str(item) for item in URIS])
def test_init_generic_filesystems(source_uri, mocker):
    """Initialize all available filesystem implementations without table parameter"""
    if isinstance(source_uri, Item):
        uri = source_uri.uri
        table = source_uri.table
    else:
        uri = source_uri
        table = ""
    parsed_uri = urlparse(uri)

    # Testing a few modules make problems on Windows.
    no_hdfs = parsed_uri.scheme == "hdfs" and sys.version_info < (3, 11)
    no_oci = parsed_uri.scheme == "oci" and sys.platform == "win32"
    if no_hdfs or no_oci:
        pytest.skip(f"{parsed_uri.scheme}:// fails testing on this test matrix slot")

    # Monkeypatch modules that would otherwise fail on initialization.
    if parsed_uri.scheme in ["gs", "sftp"]:
        pytest.skip(f"{parsed_uri.scheme}:// needs monkeypatching to make it testable")

    if "pyarrow.fs" in sys.modules:
        del sys.modules["pyarrow.fs"]
    if "pyarrow._fs" in sys.modules:
        del sys.modules["pyarrow._fs"]
    mocker.patch("pyarrow.fs.HadoopFileSystem")

    factory = SourceDestinationFactory(uri, "file://")
    source = factory.get_source()
    dlt_source = source.dlt_source(
        uri=uri,
        # TODO: Make `table` argument optional.
        #       AzureSource.dlt_source() missing 1 required positional argument: 'table'
        table=table,
    )
    assert dlt_source.name == "read_parquet"
    assert dlt_source.section == "readers"
    assert dlt_source._parent.name == "filesystem"
    assert dlt_source._parent.section == "adapter"


def test_init_http_filesystem():
    """Initialize HTTP filesystem implementation without table parameter"""
    source_uri = "http://example.org/path/to/data.parquet"
    factory = SourceDestinationFactory(source_uri, "file://")
    source = factory.get_source()
    dlt_source = source.dlt_source(
        uri=source_uri,
        # TODO: Make `table` parameter optional.
        #       AzureSource.dlt_source() missing 1 required positional argument: 'table'
        table="",
    )
    assert dlt_source.name == "http_source"
    assert dlt_source.section == "adapter"


def test_init_unknown_filesystem():
    """Initialize unknown filesystem implementation"""
    factory = SourceDestinationFactory("unknown://", "file://")
    with pytest.raises(NotImplementedError) as exc_info:
        factory.get_source()
    assert exc_info.match("Unsupported source scheme: unknown")
