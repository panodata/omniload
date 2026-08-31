"""Unit coverage for the Delta Lake source and destination adapters.

These exercise URI parsing, table-name grammar, credential handling and option
forwarding, none of which need a Delta table on disk. The end-to-end read and
write loads live in ``tests/warehouse/lakehouse/test_delta.py``.
"""

import contextlib
from unittest import mock

import pytest
from dlt.common.storages.fsspec_filesystem import fsspec_from_config

from dlt_filesystem.error import MissingConnectorOption
from omniload.error import ValidationError
from omniload.source.deltalake.adapter import DEFAULT_BATCH_SIZE, deltalake_source
from omniload.source.deltalake.api import DeltaLakeSource
from omniload.target.deltalake import DeltaLakeDestination


def passed_params(destination):
    """Return the parameters the adapter hands to the dlt filesystem destination.

    Deliberately not `destination.configuration(...)`. Resolving a configuration
    reaches the ambient environment for anything the URI does not supply, so a
    test written against a resolved config passes on a machine that happens to
    carry AWS or GCP configuration and fails on a runner that does not.
    """
    return destination.config_params


def configuration(destination, dataset_name="analytics"):
    """Resolve the configuration a dlt filesystem destination loads with.

    Only for the case whose point is what dlt does with the parameters. It
    supplies its own credentials in the URI, so resolution does not fall through
    to the environment.
    """
    spec = destination.spec()._bind_dataset_name(dataset_name=dataset_name)
    return destination.configuration(spec)


# --- destination: credentials reach dlt, and stay out of the storage path ---


def test_destination_query_becomes_typed_credentials():
    """URI credentials resolve onto dlt's AwsCredentials fields rather than being
    handed over as raw query values dlt has no field to map them onto."""
    destination = DeltaLakeDestination().dlt_dest(
        "s3+delta://my-bucket/lakehouse?access_key_id=AKIA123&secret_access_key=SECRET"
    )
    credentials = passed_params(destination)["credentials"]

    assert credentials.aws_access_key_id == "AKIA123"
    assert credentials.aws_secret_access_key == "SECRET"  # noqa: S105 - fixture value


def test_destination_query_is_not_part_of_the_object_key():
    """The query never travels in the bucket URL: dlt reads that as the storage
    path, which would make the secret part of the object key."""
    destination = DeltaLakeDestination().dlt_dest(
        "s3+delta://my-bucket/lakehouse?access_key_id=AKIA123&secret_access_key=SECRET"
    )
    config = configuration(destination)
    _, path = fsspec_from_config(config)

    assert config.bucket_url == "s3://my-bucket/lakehouse"
    assert path == "my-bucket/lakehouse"
    assert "access_key_id" not in path
    assert "SECRET" not in path


def test_destination_without_query_leaves_credentials_to_dlt():
    """A bare bucket URL passes no credentials at all, so dlt resolves them from
    the environment as it does for every other filesystem destination."""
    destination = DeltaLakeDestination().dlt_dest("s3+delta://my-bucket/lakehouse")

    assert passed_params(destination) == {"bucket_url": "s3://my-bucket/lakehouse"}


def test_destination_partial_credentials_name_the_missing_option():
    """Half a credential set asked for that credential to be used, so it fails
    naming what is absent rather than silently falling back to the environment."""
    with pytest.raises(MissingConnectorOption, match="secret_access_key"):
        DeltaLakeDestination().dlt_dest(
            "s3+delta://my-bucket/lakehouse?access_key_id=AKIA123"
        )


def test_destination_local_catalog_needs_no_credentials():
    destination = DeltaLakeDestination().dlt_dest("file+delta:///data/delta-catalog")

    assert passed_params(destination) == {"bucket_url": "file:///data/delta-catalog"}


@pytest.mark.parametrize(
    ("uri", "bucket_url"),
    [
        (
            "lakefs+delta://repo/main/lakehouse?host=lakefs.example.com&verify_ssl=false",
            "lakefs://repo/main/lakehouse",
        ),
        (
            "hdfs+delta://namenode:8020/lakehouse?user=alice",
            "hdfs://namenode:8020/lakehouse",
        ),
    ],
)
def test_destination_strips_the_query_for_unmodelled_protocols(uri, bucket_url):
    """lakeFS and HDFS have no dlt credential spec. Their query is still kept out
    of the bucket URL, and not forwarded raw to the fsspec backend, which would
    hand a constructor the string "false" where it expects False."""
    assert passed_params(DeltaLakeDestination().dlt_dest(uri)) == {
        "bucket_url": bucket_url
    }


def test_destination_builds_gcs_credentials(tmp_path):
    """Every cloud destination scheme in the registry constructs, not just S3."""
    key_file = tmp_path / "sa.json"
    key_file.write_text('{"type": "service_account", "project_id": "demo"}')
    destination = DeltaLakeDestination().dlt_dest(
        f"gs+delta://my-bucket/lakehouse?credentials_path={key_file}"
    )
    params = passed_params(destination)

    assert params["bucket_url"] == "gs://my-bucket/lakehouse"
    assert params["credentials"]["project_id"] == "demo"


def test_destination_builds_azure_credentials():
    destination = DeltaLakeDestination().dlt_dest(
        "az+delta://my-container/lakehouse?account_name=demo&account_key=c2VjcmV0"
    )
    params = passed_params(destination)

    assert params["bucket_url"] == "az://my-container/lakehouse"
    assert params["credentials"].azure_storage_account_name == "demo"


# --- table names: one grammar across source and destination ---


def test_destination_names_delta_lake_in_its_table_error():
    """The destination reports itself, not the 'SQL destination' base class."""
    with pytest.raises(ValidationError, match="'Delta Lake' table name"):
        DeltaLakeDestination().dlt_run_params("file+delta:///catalog", "events")


def scanned_uri(table, uri="file+delta:///catalog"):
    """Return the catalog URI the source hands to Polars for one table name."""
    seen = {}

    def record(target, storage_options=None):
        seen["uri"] = target
        raise StopIteration

    source = DeltaLakeSource().dlt_source(uri=uri, table=table)
    resource = next(iter(source.resources.values()))
    with mock.patch("polars.scan_delta", record):
        with contextlib.suppress(Exception):
            list(resource)
    return seen.get("uri")


def test_source_reads_a_directory_whose_name_contains_a_dot():
    """A catalog directory named `my.schema` is addressable by quoting it, the
    spelling the rest of the project already accepts. Splitting on a raw dot
    could not reach it."""
    assert scanned_uri('"my.schema".events') == "file:///catalog/my.schema/events"


def test_source_builds_one_directory_per_component():
    assert scanned_uri("analytics.events") == "file:///catalog/analytics/events"


@pytest.mark.parametrize(
    "table",
    [
        '"..".events',
        '".".events',
        '"a/b".events',
        '"a\\b".events',
        'analytics."../x"',
    ],
)
def test_source_rejects_a_component_that_is_not_one_directory(table):
    """A quoted component is one identifier to the grammar but one directory
    here, so a separator or a parent reference must not widen the address."""
    with pytest.raises(ValidationError, match="single directory name"):
        DeltaLakeSource().dlt_source(uri="file+delta:///catalog", table=table)


def test_source_rejects_an_unqualified_table_name(tmp_path):
    with pytest.raises(ValidationError, match="'Delta Lake' table name"):
        DeltaLakeSource().dlt_source(uri=f"file+delta://{tmp_path}", table="events")


def test_source_leaves_a_unity_catalog_table_name_alone():
    """Unity Catalog addresses the table in the URI, so the adapter appends no
    path components and does not impose the two-component grammar."""
    assert (
        scanned_uri("", uri="uc+delta://catalog.schema.table")
        == "uc://catalog.schema.table"
    )


# --- run options the source used to drop ---


def test_source_rejects_a_requested_incremental_key(tmp_path):
    """--incremental-key is cleared before dlt_source is called, so the guard has
    to read the requested value to reject it rather than ignore it."""
    with pytest.raises(ValueError, match="incrementality on its own"):
        DeltaLakeSource().dlt_source(
            uri=f"file+delta://{tmp_path}",
            table="analytics.events",
            incremental_key=None,
            requested_incremental_key="updated_at",
        )


def test_source_honours_run_disposition():
    """Delta Lake accepts an explicit run-level --incremental-strategy, unlike the
    SaaS sources whose resource-level disposition must win."""
    assert DeltaLakeSource().handles_incrementality() is True
    assert DeltaLakeSource().honours_run_disposition() is True


def test_source_forwards_page_size_as_the_batch_size(tmp_path, monkeypatch):
    """--page-size reaches the Polars batch size instead of being dropped. It
    always arrives with a value, so it is what sets the batch size in a CLI run."""
    seen = {}

    def record(uri, table, batch_size=None):
        seen["batch_size"] = batch_size
        return ()

    monkeypatch.setattr(
        "omniload.source.deltalake.adapter.deltalake_source", record, raising=True
    )
    DeltaLakeSource().dlt_source(
        uri=f"file+delta://{tmp_path}", table="analytics.events", page_size=1000
    )

    assert seen["batch_size"] == 1000


def test_adapter_falls_back_when_no_batch_size_is_given(monkeypatch):
    """A direct call to the adapter, which the CLI path never makes, still gets a
    batch size rather than passing None to Polars."""
    seen = {}

    def record(target, storage_options=None, **kwargs):
        raise StopIteration

    def collect(chunk_size=None, **kwargs):
        seen["chunk_size"] = chunk_size
        return iter(())

    class Frame:
        collect_batches = staticmethod(collect)

    monkeypatch.setattr("polars.scan_delta", lambda *a, **k: Frame())
    resource = next(
        iter(
            deltalake_source(
                "file:///catalog", "a.b", batch_size=None
            ).resources.values()
        )
    )
    list(resource)

    assert seen["chunk_size"] == DEFAULT_BATCH_SIZE
