import logging
from typing import Any, ClassVar

import pytest
from fsspec.implementations.memory import MemoryFileSystem

from dlt_filesystem.staging import (
    RemoteObject,
    RemoteObjectNotFoundError,
    RemoteObjectSizeError,
    _filesystem_for,
    materialize_remote_object,
)


class _IsolatedMemoryFileSystem(MemoryFileSystem):
    """Keep staging-test objects out of fsspec's process-global memory store."""

    store: ClassVar[dict[str, Any]] = {}

    def __init__(self) -> None:
        super().__init__()
        self.pseudo_dirs = [""]


@pytest.fixture
def memory_filesystem():
    filesystem = _IsolatedMemoryFileSystem()
    filesystem.store.clear()
    filesystem.pipe_file("bucket/path/database.duckdb", b"database bytes")
    yield filesystem
    filesystem.store.clear()


def _remote() -> RemoteObject:
    return RemoteObject.from_uri(
        "s3://bucket/path/database.duckdb"
        "?access_key_id=access&secret_access_key=do-not-log"
    )


def test_remote_object_rejects_a_scheme_it_cannot_stage():
    with pytest.raises(ValueError, match="Unsupported storage scheme 'sftp'"):
        RemoteObject.from_uri("sftp://host/path/database.duckdb")


def test_filesystem_for_serves_an_s3_compatible_alias_through_s3fs():
    """R2 is S3-compatible, so its objects are staged with the S3 client."""
    from s3fs import S3FileSystem

    remote = RemoteObject.from_uri(
        "r2://bucket/path/database.duckdb"
        "?access_key_id=access&secret_access_key=secret"
        "&endpoint_url=https://account.r2.cloudflarestorage.com"
    )

    assert remote.backend == "s3"
    assert isinstance(_filesystem_for(remote), S3FileSystem)


def test_materialize_remote_object_lives_for_context_and_is_removed(
    tmp_path, memory_filesystem, mocker, caplog
):
    mocker.patch(
        "dlt_filesystem.staging._filesystem_for", return_value=memory_filesystem
    )

    with caplog.at_level(logging.INFO, logger="dlt_filesystem.staging"):
        with materialize_remote_object(
            _remote(), filename="database", staging_root=tmp_path
        ) as local_path:
            assert local_path.exists()
            assert local_path.read_bytes() == b"database bytes"
            assert local_path.parent.parent == tmp_path

    assert not local_path.exists()
    assert list(tmp_path.iterdir()) == []
    assert "s3://bucket/path/database.duckdb" in caplog.text
    assert "do-not-log" not in caplog.text


def test_materialize_remote_object_cleans_up_when_consumer_fails(
    tmp_path, memory_filesystem, mocker
):
    mocker.patch(
        "dlt_filesystem.staging._filesystem_for", return_value=memory_filesystem
    )

    with pytest.raises(RuntimeError, match="extract failed"):
        with materialize_remote_object(
            _remote(), filename="database", staging_root=tmp_path
        ) as local_path:
            assert local_path.exists()
            raise RuntimeError("extract failed")

    assert list(tmp_path.iterdir()) == []


def test_materialize_remote_object_reports_missing_object_without_credentials(
    tmp_path, memory_filesystem, mocker
):
    mocker.patch(
        "dlt_filesystem.staging._filesystem_for", return_value=memory_filesystem
    )
    remote = RemoteObject.from_uri(
        "s3://bucket/path/missing.duckdb"
        "?access_key_id=access&secret_access_key=do-not-report"
    )

    with pytest.raises(RemoteObjectNotFoundError) as excinfo:
        with materialize_remote_object(
            remote, filename="database", staging_root=tmp_path
        ):
            pass

    assert "s3://bucket/path/missing.duckdb" in str(excinfo.value)
    assert "do-not-report" not in str(excinfo.value)
    assert list(tmp_path.iterdir()) == []


def test_materialize_remote_object_rejects_short_read(
    tmp_path, memory_filesystem, mocker
):
    mocker.patch(
        "dlt_filesystem.staging._filesystem_for", return_value=memory_filesystem
    )
    mocker.patch.object(
        memory_filesystem,
        "info",
        return_value={"name": "database.duckdb", "size": 99, "type": "file"},
    )

    with pytest.raises(RemoteObjectSizeError, match="expected 99 bytes"):
        with materialize_remote_object(
            _remote(), filename="database", staging_root=tmp_path
        ):
            pass

    assert list(tmp_path.iterdir()) == []


def test_materialize_remote_object_checks_available_disk_before_download(
    tmp_path, memory_filesystem, mocker
):
    mocker.patch(
        "dlt_filesystem.staging._filesystem_for", return_value=memory_filesystem
    )
    mocker.patch(
        "dlt_filesystem.staging.shutil.disk_usage",
        return_value=mocker.Mock(free=1),
    )
    open_spy = mocker.spy(memory_filesystem, "open")

    with pytest.raises(OSError, match="Not enough local disk space"):
        with materialize_remote_object(
            _remote(), filename="database", staging_root=tmp_path
        ):
            pass

    open_spy.assert_not_called()
    assert list(tmp_path.iterdir()) == []
