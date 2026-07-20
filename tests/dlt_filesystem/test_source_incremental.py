import re
from typing import Any
from unittest.mock import patch

from dlt.extract.incremental import Incremental
from fsspec import AbstractFileSystem
from fsspec.implementations.arrow import ArrowFSWrapper
from pyarrow.fs import LocalFileSystem

from dlt_filesystem.source.adapter import readers
from dlt_filesystem.source.api import resource_for_reader
from dlt_filesystem.source.impl.local import LocalFilesystemSource
from dlt_filesystem.source.impl.remote import (
    AzureSource,
    GCSSource,
    S3Source,
    SFTPSource,
    _endpoint_namespace,
)
from dlt_filesystem.source.model import FilesystemReference


def _reference(
    tmp_path,
    *,
    fs: AbstractFileSystem | None = None,
    bucket_url: str | None = None,
    file_glob: str = "*.csv",
    reader_name: str = "read_csv",
    storage_namespace: str = "file",
    filesystem_incremental: bool = True,
    hints: dict[str, str] | None = None,
    column_types: dict[str, Any] | None = None,
) -> FilesystemReference:
    return FilesystemReference(
        fs=fs or ArrowFSWrapper(LocalFileSystem()),
        bucket_url=bucket_url or str(tmp_path),
        file_glob=file_glob,
        reader_name=reader_name,
        storage_namespace=storage_namespace,
        filesystem_incremental=filesystem_incremental,
        hints=hints or {},
        column_types=column_types,
    )


def test_incremental_hint_is_applied_to_metadata_only_parent_lister(tmp_path):
    (tmp_path / "people.csv").write_text("name\nAlice\n")

    resource = resource_for_reader(_reference(tmp_path))
    lister = resource._parent

    assert resource.name == "read_csv"
    assert resource.table_name == "read_csv"
    assert lister.name == _reference(tmp_path).incremental_resource_name
    assert re.fullmatch(r"filesystem_[0-9a-f]{32}", lister.name)
    incremental = lister.incremental
    assert isinstance(incremental, Incremental)
    assert incremental.cursor_path == "modification_date"
    assert incremental.range_start == "closed"
    assert incremental.primary_key == "file_url"

    file_items = list(lister)
    assert len(file_items) == 1
    assert "file_content" not in file_items[0]


def test_disabled_mode_preserves_the_existing_lister_and_output_identity(tmp_path):
    (tmp_path / "people.csv").write_text("name\nAlice\n")

    resource = resource_for_reader(_reference(tmp_path, filesystem_incremental=False))

    assert resource.name == "read_csv"
    assert resource.table_name == "read_csv"
    assert resource._parent.name == "filesystem"
    assert resource._parent.incremental is None


def test_every_reader_stays_downstream_of_the_incremental_lister(tmp_path):
    fs = ArrowFSWrapper(LocalFileSystem())
    reader_names = readers(str(tmp_path), fs, file_glob="*.none").resources.keys()

    for reader_name in reader_names:
        resource = resource_for_reader(
            _reference(tmp_path, fs=fs, reader_name=reader_name)
        )

        assert resource.name == reader_name
        assert resource.table_name == reader_name
        incremental = resource._parent.incremental
        assert isinstance(incremental, Incremental)
        assert incremental.cursor_path == "modification_date"


def test_incremental_resource_name_is_source_scoped_and_secret_free(tmp_path):
    baseline = _reference(tmp_path)

    same_identity = _reference(
        tmp_path,
        fs=ArrowFSWrapper(LocalFileSystem()),
        hints={"sheet_name": "rotated-secret-does-not-matter"},
        column_types={"name": {"data_type": "text"}},
    )
    assert same_identity.incremental_resource_name == baseline.incremental_resource_name

    changed_namespace = _reference(tmp_path, storage_namespace="s3:https://minio-a")
    changed_bucket = _reference(tmp_path, bucket_url="s3://another-bucket")
    changed_glob = _reference(tmp_path, file_glob="archive/*.csv")

    assert (
        changed_namespace.incremental_resource_name
        != baseline.incremental_resource_name
    )
    assert (
        changed_bucket.incremental_resource_name != baseline.incremental_resource_name
    )
    assert changed_glob.incremental_resource_name != baseline.incremental_resource_name


def _captured_reference(call) -> FilesystemReference:
    assert call.call_count == 1
    return call.call_args.args[0]


def test_filesystem_sources_thread_incremental_identity_without_auth_material(tmp_path):
    local_path = tmp_path / "local" / "*.csv"
    with patch("dlt_filesystem.source.api.resource_for_reader") as build:
        LocalFilesystemSource().dlt_source(
            f"file://{local_path}", "", filesystem_incremental=True
        )
    local = _captured_reference(build)

    with (
        patch("dlt_filesystem.source.api.resource_for_reader") as build,
        patch("gcsfs.GCSFileSystem"),
    ):
        GCSSource().dlt_source(
            "gs://?credentials_base64=e30K",
            "bucket/*.csv",
            filesystem_incremental=True,
        )
    gcs = _captured_reference(build)

    with (
        patch("dlt_filesystem.source.api.resource_for_reader") as build,
        patch("s3fs.S3FileSystem"),
    ):
        S3Source().dlt_source(
            "s3://?access_key_id=KEY&secret_access_key=SECRET&endpoint_url=https://minio.example:9000",
            "bucket/*.csv",
            filesystem_incremental=True,
        )
    s3 = _captured_reference(build)

    with (
        patch("dlt_filesystem.source.api.resource_for_reader") as build,
        patch("dlt_filesystem.source.impl.remote._azure_fs"),
    ):
        AzureSource().dlt_source(
            "az://?account_name=account&account_key=SECRET",
            "container/*.csv",
            filesystem_incremental=True,
        )
    azure = _captured_reference(build)

    with (
        patch("dlt_filesystem.source.api.resource_for_reader") as build,
        patch("fsspec.filesystem"),
    ):
        SFTPSource().dlt_source(
            "sftp://user:SECRET@sftp.example:2222",
            "/*.csv",
            filesystem_incremental=True,
        )
    sftp = _captured_reference(build)

    assert all(ref.filesystem_incremental for ref in (local, gcs, s3, azure, sftp))
    assert local.storage_namespace == "file"
    assert gcs.storage_namespace == "gcs"
    assert s3.storage_namespace == "s3:minio.example:9000"
    assert azure.storage_namespace == "azure:account:azure-public"
    assert sftp.storage_namespace == "sftp:sftp.example:2222:user"
    assert all(
        "SECRET" not in ref.incremental_resource_name
        for ref in (local, gcs, s3, azure, sftp)
    )
    assert all(
        "SECRET" not in ref.storage_namespace for ref in (local, gcs, s3, azure, sftp)
    )


def test_endpoint_namespace_excludes_credentials_and_query_values():
    assert (
        _endpoint_namespace(
            "https://user:password@MINIO.example:9000/storage/?token=secret",
            "default",
        )
        == "minio.example:9000/storage"
    )


def test_endpoint_namespace_normalizes_bare_host_and_url_forms():
    assert _endpoint_namespace("account.blob.example", "default") == (
        _endpoint_namespace("https://account.blob.example", "default")
    )


def test_endpoint_namespace_brackets_ipv6_hosts():
    assert (
        _endpoint_namespace("https://[::1]:9000/bucket", "default")
        == "[::1]:9000/bucket"
    )


def test_endpoint_namespace_falls_back_when_hostname_is_missing():
    assert _endpoint_namespace("/just/a/path", "default") == "default"


def test_auth_rotation_does_not_change_incremental_resource_names():
    with (
        patch("dlt_filesystem.source.api.resource_for_reader") as first_build,
        patch("gcsfs.GCSFileSystem"),
    ):
        GCSSource().dlt_source("gs://?credentials_base64=e30K", "bucket/*.csv")
    with (
        patch("dlt_filesystem.source.api.resource_for_reader") as second_build,
        patch("gcsfs.GCSFileSystem"),
    ):
        GCSSource().dlt_source("gs://?credentials_base64=e30=", "bucket/*.csv")
    assert (
        _captured_reference(first_build).incremental_resource_name
        == _captured_reference(second_build).incremental_resource_name
    )

    with (
        patch("dlt_filesystem.source.api.resource_for_reader") as first_build,
        patch("s3fs.S3FileSystem"),
    ):
        S3Source().dlt_source(
            "s3://?access_key_id=OLD&secret_access_key=OLD_SECRET",
            "bucket/*.csv",
        )
    with (
        patch("dlt_filesystem.source.api.resource_for_reader") as second_build,
        patch("s3fs.S3FileSystem"),
    ):
        S3Source().dlt_source(
            "s3://?access_key_id=NEW&secret_access_key=NEW_SECRET",
            "bucket/*.csv",
        )
    assert (
        _captured_reference(first_build).incremental_resource_name
        == _captured_reference(second_build).incremental_resource_name
    )

    with (
        patch("dlt_filesystem.source.api.resource_for_reader") as first_build,
        patch("dlt_filesystem.source.impl.remote._azure_fs"),
    ):
        AzureSource().dlt_source(
            "az://?account_name=account&account_key=OLD_SECRET",
            "container/*.csv",
        )
    with (
        patch("dlt_filesystem.source.api.resource_for_reader") as second_build,
        patch("dlt_filesystem.source.impl.remote._azure_fs"),
    ):
        AzureSource().dlt_source(
            "az://?account_name=account&account_key=NEW_SECRET",
            "container/*.csv",
        )
    assert (
        _captured_reference(first_build).incremental_resource_name
        == _captured_reference(second_build).incremental_resource_name
    )

    with (
        patch("dlt_filesystem.source.api.resource_for_reader") as first_build,
        patch("fsspec.filesystem"),
    ):
        SFTPSource().dlt_source("sftp://user:OLD_SECRET@sftp.example:22", "/*.csv")
    with (
        patch("dlt_filesystem.source.api.resource_for_reader") as second_build,
        patch("fsspec.filesystem"),
    ):
        SFTPSource().dlt_source("sftp://user:NEW_SECRET@sftp.example:22", "/*.csv")
    assert (
        _captured_reference(first_build).incremental_resource_name
        == _captured_reference(second_build).incremental_resource_name
    )
