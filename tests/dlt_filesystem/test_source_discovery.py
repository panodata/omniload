from unittest.mock import patch

import pytest
from fsspec.implementations.memory import MemoryFileSystem

from dlt_filesystem.source.core import infer_resource, resource_for_reader
from dlt_filesystem.source.error import NoFilesFoundError
from dlt_filesystem.source.model import FilesystemLocator, FilesystemReference
from tests.util.common import has_exception


def _reference(*, require_file_match: bool) -> FilesystemReference:
    return FilesystemReference(
        fs=MemoryFileSystem(),
        bucket_url="memory://bucket",
        file_glob="missing.csv",
        reader_name="read_csv",
        require_file_match=require_file_match,
    )


def test_final_reader_lists_files_once_and_only_during_extraction():
    with patch(
        "dlt_filesystem.source.adapter.glob_files", return_value=iter(())
    ) as glob_files:
        resource = resource_for_reader(_reference(require_file_match=False))

        glob_files.assert_not_called()
        assert list(resource) == []

    glob_files.assert_called_once()


def test_concrete_selection_failure_is_enforced_at_shared_choke_point():
    with patch("dlt_filesystem.source.adapter.glob_files", return_value=iter(())):
        resource = resource_for_reader(_reference(require_file_match=True))

        with pytest.raises(Exception) as exc_info:
            list(resource)

    assert has_exception(exc_info, NoFilesFoundError)


def test_locator_based_source_threads_concrete_match_requirement():
    locator = FilesystemLocator(
        name="Memory",
        fs_class=MemoryFileSystem,
        uri="memory://bucket",
        path="missing.csv",
    )

    with patch("dlt_filesystem.source.adapter.glob_files", return_value=iter(())):
        resource = infer_resource(MemoryFileSystem(), locator)

        with pytest.raises(Exception) as exc_info:
            list(resource)

    assert has_exception(exc_info, NoFilesFoundError)


def test_missing_file_error_redacts_bucket_url_credentials():
    error = NoFilesFoundError(
        "dbfs://?host=https%3A%2F%2Fexample.com&token=secret",
        "missing.csv",
    )

    assert str(error) == (
        "No files found at 'dbfs://' for concrete source selection 'missing.csv'"
    )
    assert "secret" not in str(error)

    error = NoFilesFoundError(
        "sftp://user:password@example.com/?token=secret",
        "missing.csv",
    )
    assert "user" not in str(error)
    assert "password" not in str(error)
    assert "secret" not in str(error)
    assert "sftp://example.com/" in str(error)


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("bucket/missing.csv", True),
        ("bucket/*.csv", False),
        ("", False),
    ],
)
def test_locator_threads_raw_selection_classification(path, expected):
    locator = FilesystemLocator(
        name="Memory",
        fs_class=MemoryFileSystem,
        uri="memory://",
        path=path,
        accept_no_bucket_name=not path,
    )

    assert locator.require_file_match is expected
