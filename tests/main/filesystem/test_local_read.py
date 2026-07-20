from dlt_filesystem.source.fsspec.local import LocalFilesystemSource
from omniload.core.factory import SourceDestinationFactory


def test_factory_dispatches_file_scheme_to_local_source():
    factory = SourceDestinationFactory(
        "file://tests/assets/create_replace.csv", "duckdb:///tmp/x.duckdb"
    )
    assert isinstance(factory.get_source(), LocalFilesystemSource)
