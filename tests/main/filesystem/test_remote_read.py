from urllib.parse import urlparse

import pytest

from omniload.core.factory import SourceDestinationFactory

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
    "s3://bucket/path/to/data.parquet?access_key_id=foo&secret_access_key=bar",
    "sftp://username:password@example.com:2222/path/to/data.parquet",
]


@pytest.mark.parametrize("source_uri", URIS)
def test_init_generic_filesystems(source_uri):
    """Initialize all available filesystem implementations without table parameter"""
    parsed_uri = urlparse(source_uri)
    if parsed_uri.scheme in ["gs", "sftp"]:
        pytest.skip(f"{parsed_uri.scheme}:// needs monkeypatching to make it testable")
    factory = SourceDestinationFactory(source_uri, "file://")
    source = factory.get_source()
    dlt_source = source.dlt_source(
        uri=source_uri,
        # TODO: AzureSource.dlt_source() missing 1 required positional argument: 'table'
        table="",
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
    with pytest.raises(ValueError) as exc_info:
        factory.get_source()
    assert exc_info.match("Unsupported source scheme: unknown")
