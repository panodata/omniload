"""Source-level unit tests: named `#key=value` hints thread into FilesystemReference.

Mock-only (no Docker, no credentials): `resource_for_reader` is patched to capture
the constructed `FilesystemReference`, and each source's fs constructor is patched so
nothing connects. The warehouse S3/GCS matrix keeps the end-to-end regression cover;
these prove the hint plumbing per source in isolation.
"""

from unittest.mock import MagicMock, patch

import pytest

from dlt_filesystem.source.impl.local import LocalFilesystemSource
from dlt_filesystem.source.impl.remote import GCSSource, S3Source, SFTPSource

RESOURCE_FOR_READER = "dlt_filesystem.source.adapter.resource_for_reader"

GCS_URI = "gs://?credentials_base64=e30K"  # base64 for "{}"
S3_URI = "s3://?access_key_id=KEY&secret_access_key=SECRET"


def _captured_ref(rfr: MagicMock):
    assert rfr.call_count == 1
    return rfr.call_args.args[0]


@pytest.mark.parametrize(
    ("uri", "expected_hints"),
    [
        # no fragment -> hints default to {}
        ("file://book.csv", {}),
        # a named hint on a plain resolvable file
        ("file://book.xlsx#sheet_name=foo", {"sheet_name": "foo"}),
        # format hint (#xlsx resolves the reader) and named hint coexist
        ("file://feed.dat#xlsx&sheet_name=foo", {"sheet_name": "foo"}),
    ],
)
def test_local_threads_hints(uri: str, expected_hints: dict[str, str]):
    with (
        patch(RESOURCE_FOR_READER) as rfr,
        patch("fsspec.filesystem"),
    ):
        LocalFilesystemSource().dlt_source(uri, "")
    assert _captured_ref(rfr).hints == expected_hints


@pytest.mark.parametrize(
    ("table", "expected_hints"),
    [
        ("bucket/book.csv", {}),
        ("bucket/book.xlsx#sheet_name=foo", {"sheet_name": "foo"}),
        ("bucket/feed.dat#xlsx&sheet_name=foo", {"sheet_name": "foo"}),
    ],
)
def test_gcs_threads_hints(table: str, expected_hints: dict[str, str]):
    with (
        patch(RESOURCE_FOR_READER) as rfr,
        patch("gcsfs.GCSFileSystem"),
    ):
        GCSSource().dlt_source(GCS_URI, table)
    assert _captured_ref(rfr).hints == expected_hints


@pytest.mark.parametrize(
    ("table", "expected_hints"),
    [
        ("bucket/book.csv", {}),
        ("bucket/book.xlsx#sheet_name=foo", {"sheet_name": "foo"}),
        ("bucket/feed.dat#xlsx&sheet_name=foo", {"sheet_name": "foo"}),
    ],
)
def test_s3_threads_hints(table: str, expected_hints: dict[str, str]):
    with (
        patch(RESOURCE_FOR_READER) as rfr,
        patch("s3fs.S3FileSystem"),
    ):
        S3Source().dlt_source(S3_URI, table)
    assert _captured_ref(rfr).hints == expected_hints


def test_s3_threads_hints_from_uri_path_form():
    """The deprecated `s3://bucket/path#frag` URI-path form carries the fragment in
    parsed_uri.fragment; blob_hints reconstructs it so hints still thread."""
    with (
        patch(RESOURCE_FOR_READER) as rfr,
        patch("s3fs.S3FileSystem"),
        pytest.warns(DeprecationWarning),
    ):
        S3Source().dlt_source(
            "s3://bucket/book.xlsx?access_key_id=KEY&secret_access_key=SECRET#sheet_name=foo",
            "",
        )
    assert _captured_ref(rfr).hints == {"sheet_name": "foo"}


def test_s3_blob_hints_track_the_loaded_file_when_both_forms_given():
    """When a URI path and a --source-table are both supplied (a contradictory,
    deprecated shape), parse_uri loads the URI-path file, so blob_hints must read
    the URI fragment, not the ignored table's."""
    with (
        patch(RESOURCE_FOR_READER) as rfr,
        patch("s3fs.S3FileSystem"),
        pytest.warns(DeprecationWarning),
    ):
        S3Source().dlt_source(
            "s3://bucket/loaded.xlsx?access_key_id=KEY&secret_access_key=SECRET#sheet_name=uri",
            "ignored/other.xlsx#sheet_name=table",
        )
    assert _captured_ref(rfr).hints == {"sheet_name": "uri"}


@pytest.mark.parametrize(
    ("table", "expected_hints"),
    [
        ("/book.csv", {}),
        ("/book.xlsx#sheet_name=foo", {"sheet_name": "foo"}),
        ("/feed.dat#xlsx&sheet_name=foo", {"sheet_name": "foo"}),
    ],
)
def test_sftp_threads_hints(table: str, expected_hints: dict[str, str]):
    with (
        patch(RESOURCE_FOR_READER) as rfr,
        patch("fsspec.filesystem"),
    ):
        SFTPSource().dlt_source("sftp://user:pass@host:22", table)
    assert _captured_ref(rfr).hints == expected_hints
