import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import pytest
from fsspec.implementations.memory import MemoryFileSystem

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
assets_dir = Path(__file__).resolve().parents[2] / "assets"

private_key_file = (assets_dir / "privatekey.pem").as_posix()
private_key_fingerprint = (
    (assets_dir / "privatekey-fingerprint.txt").read_text().strip()
)

# A collection of filesystem source URIs without table parameter.
URIS = [
    # TODO: Review if account name is not already provided in the hostname per `acme`.
    "abfss://schrott@acme.dfs.core.windows.net/path/to/data.parquet?account_name=acme&account_key=secret",
    "adls://schrott@acme.dfs.core.windows.net/path/to/data.parquet?account_name=acme&account_key=secret",
    "az://schrott@acme.dfs.core.windows.net/path/to/data.parquet?account_name=acme&account_key=secret",
    # TODO: Review note on the README at https://github.com/fsspec/dropboxdrivefs:
    #       > Use `dropbox:///folder1/folder2/etc`. Yes, with three /// ! What happen if not, for some reasons
    #       > the dropbox api will remove everything before the first / in the path keep only what is after.
    #       Currently, we are using two slashes, because otherwise the machinery fails. In this
    #       spirit, it is essential to run a few cycles of user testing.
    "dropbox://path/to/data.parquet?token=secret",
    Item(
        uri="ftp://username:password@intranet.example.org/path/to/data.parquet?tls=tls",
        table="",
    ),
    # TODO: Two FTP tests currently don't work. Why?
    # Item(
    #    uri="ftp://username:password@intranet.example.org?tls=tls",
    #    table="/path/to/data.parquet",
    # ),
    "gs://table-bucket-name/path/to/data.parquet?credentials_path=/path/to/service-account.json",
    "gs://table-bucket-name/path/to/data.parquet?credentials_base64=eyJjbGllbnRfaWQiOiAiZm9vIiwgImNsaWVudF9zZWNyZXQiOiAiYmFyIiwgInJlZnJlc2hfdG9rZW4iOiAiYW55dGhpbmcifQ==",
    Item(
        uri="hdfs://example.com:8020/path/to/data.parquet?user=test",
        table="",
    ),
    Item(
        uri="hdfs://example.com:8020?user=test",
        table="path/to/data.parquet",
    ),
    Item(
        uri="http+webdav://public.example.org/path/to/data.parquet",
        table="",
    ),
    Item(
        uri="https+webdav://username:password@cloud.example.org:4443/remote.php/webdav",
        table="path/to/data.parquet",
    ),
    "msgd://site_name/drive_name/path/to/data.parquet?client_id=1d2befad-2f22-4124-a779-b147dfeca342&tenant_id=6b337423-f504-4060-a91b-e9eaaf782609&client_secret=abc~xyz789EXAMPLE_foo",
    f'oci://bucket@namespace/prefix/path/to/data.parquet?iam_type=api_key&config={{"user":"ocid1.user.oc1..24g4uzg","region":"us-ashburn-1","tenancy":"ocid1.tenancy.oc1..23423r3","key_file":"{private_key_file}","fingerprint":"{private_key_fingerprint}"}}',
    "onedrive://drive_name/path/to/data.parquet?client_id=1d2befad-2f22-4124-a779-b147dfeca342&tenant_id=6b337423-f504-4060-a91b-e9eaaf782609&client_secret=abc~xyz789EXAMPLE_foo",
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
    Item(
        uri="sftp://username:password@intranet.example.org:2222",
        table="/path/to/data.parquet",
    ),
    "sftp://username:password@intranet.example.org:2222/path/to/data.parquet",
    "sharepoint://site_name/drive_name/path/to/data.parquet?client_id=1d2befad-2f22-4124-a779-b147dfeca342&tenant_id=6b337423-f504-4060-a91b-e9eaaf782609&client_secret=abc~xyz789EXAMPLE_foo",
    "smb://workgroup;user:password@server.example.org:445/path/to/data.parquet",
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

    # Testing a few modules has problems on Windows.
    no_hdfs = parsed_uri.scheme == "hdfs" and sys.version_info < (3, 11)
    no_oci = parsed_uri.scheme == "oci" and sys.platform == "win32"
    if no_hdfs or no_oci:
        pytest.skip(f"{parsed_uri.scheme}:// fails testing on this test matrix slot")

    # Apply monkeypatching to make a few filesystem implementations ready for unit testing.

    # GCS is fine with this environment variable being mocked.
    mocker.patch.dict(os.environ, {"FETCH_RAW_TOKEN_EXPIRY": "false"})

    # Must patch the whole class, because can't patch details which are immutable.
    mocker.patch("pyarrow.fs.HadoopFileSystem", MemoryFileSystem)

    # It's enough to mock the `_connect` method with SFTP and SMB.
    mocker.patch("fsspec.implementations.sftp.SFTPFileSystem._connect")
    mocker.patch("fsspec.implementations.smb.SMBFileSystem._connect")

    # For FTP, let's mock the low-level libraries.
    mocker.patch("ftplib.FTP")
    mocker.patch("ftplib.FTP_TLS")

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

    # Remark: Unfortunately can't inspect the fsspec instance,
    #         because there is no reference to it.


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
