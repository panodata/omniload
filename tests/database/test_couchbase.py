import os

import pytest
import sqlalchemy

from tests.database.container import COUCHBASE_IMAGE, DESTINATIONS, CouchbaseContainer
from tests.util import invoke_ingest_command


@pytest.mark.parametrize(
    "dest", list(DESTINATIONS.values()), ids=list(DESTINATIONS.keys())
)
def test_couchbase_source_local(dest):
    """
    Test Couchbase source with local containerized Couchbase instance.

    NOTE: This test requires local Couchbase Server to be stopped first,
    as it uses 1:1 port mapping (8091, 11210, etc.) to avoid SDK connection issues.
    """
    couchbase = CouchbaseContainer(COUCHBASE_IMAGE)
    couchbase.start()

    # Insert test documents
    test_documents = [
        {
            "id": 1,
            "name": "Document 1",
            "nested_parent": {
                "key1": "value1",
                "key2": {"nested1": "value1"},
                "key3": [{"nested3": "value1"}],
            },
            "key4": ["value1", "value2", "value3"],
            "value": 100,
        },
        {
            "id": 2,
            "name": "Document 2",
            "nested_parent": {
                "key1": "value2",
                "key2": {"nested1": "value2"},
                "key3": [{"nested3": "value2"}],
            },
            "key4": ["value1", "value2", "value3"],
            "value": 200,
        },
        {
            "id": 3,
            "name": "Document 3",
            "nested_parent": {
                "key1": "value3",
                "key2": {"nested1": "value3"},
                "key3": [{"nested3": "value3"}],
            },
            "key4": ["value1", "value2", "value3"],
            "value": 300,
        },
    ]

    couchbase.insert_documents(test_documents)

    dest_uri = dest.start()

    try:
        # Build source URI without bucket (bucket will be in table name)
        source_uri = couchbase.get_connection_url()
        source_table = f"{couchbase.bucket_name}.{couchbase.scope_name}.{couchbase.collection_name}"

        result = invoke_ingest_command(
            source_uri,
            source_table,
            dest_uri,
            "raw.test_couchbase_collection",
        )

        assert result.exit_code == 0, (
            f"Command failed with exit code {result.exit_code}"
        )

        with sqlalchemy.create_engine(dest_uri).connect() as conn:
            res = conn.exec_driver_sql(
                "select * from raw.test_couchbase_collection order by id"
            ).fetchall()

            assert len(res) == 3, f"Expected 3 documents, got {len(res)}"

            # Verify documents were ingested correctly
            # Check essential fields (id, name, value, and at least one nested field)
            ids = [row[0] for row in res]
            names = [row[1] for row in res]
            values = [row[2] for row in res]  # value column

            assert ids == [1, 2, 3], f"Expected ids [1, 2, 3], got {ids}"
            assert names == [
                "Document 1",
                "Document 2",
                "Document 3",
            ], f"Expected names, got {names}"
            assert values == [
                100,
                200,
                300,
            ], f"Expected values [100, 200, 300], got {values}"

            # Check that nested_parent__key1 was flattened correctly
            nested_values = [row[3] for row in res]
            assert nested_values == [
                "value1",
                "value2",
                "value3",
            ], f"Expected nested values, got {nested_values}"
    finally:
        dest.stop()
        couchbase.stop()


@pytest.mark.skipif(
    not os.environ.get("COUCHBASE_CAPELLA_USERNAME")
    or not os.environ.get("COUCHBASE_CAPELLA_PASSWORD"),
    reason="Couchbase Capella credentials not set",
)
@pytest.mark.parametrize(
    "dest", list(DESTINATIONS.values()), ids=list(DESTINATIONS.keys())
)
def test_couchbase_capella_source(dest):
    """
    Test Couchbase Capella (cloud) as a source with bucket in URI.
    Uses SSL connection with bucket specified in URI path.

    Required environment variables:
    - COUCHBASE_CAPELLA_USERNAME
    - COUCHBASE_CAPELLA_PASSWORD
    - COUCHBASE_CAPELLA_HOST
    - COUCHBASE_CAPELLA_BUCKET
    - COUCHBASE_CAPELLA_SCOPE
    - COUCHBASE_CAPELLA_COLLECTION
    """
    username = os.environ.get("COUCHBASE_CAPELLA_USERNAME")
    password = os.environ.get("COUCHBASE_CAPELLA_PASSWORD")
    host = os.environ.get(
        "COUCHBASE_CAPELLA_HOST", "cb.8vm1qjx5nowztp08.cloud.couchbase.com"
    )
    bucket = os.environ.get("COUCHBASE_CAPELLA_BUCKET", "travel-sample")
    scope = os.environ.get("COUCHBASE_CAPELLA_SCOPE", "inventory")
    collection = os.environ.get("COUCHBASE_CAPELLA_COLLECTION", "airline")

    # Test with bucket in URI and ssl=true parameter
    source_uri = f"couchbase://{username}:{password}@{host}/{bucket}?ssl=true"
    source_table = f"{scope}.{collection}"

    dest_uri = dest.start()
    dest_table = "raw.couchbase_capella_test"

    try:
        result = invoke_ingest_command(
            source_uri,
            source_table,
            dest_uri,
            dest_table,
        )

        assert result.exit_code == 0, f"Command failed with: {result.output}"

        # Verify data was ingested
        with sqlalchemy.create_engine(dest_uri).connect() as conn:
            res = conn.exec_driver_sql(f"select * from {dest_table}").fetchall()
            assert len(res) > 0, "No data was ingested from Couchbase Capella"
            print(
                f"Successfully ingested {len(res)} documents from Couchbase Capella (bucket in URI)"
            )
    finally:
        dest.stop()


@pytest.mark.skipif(
    not os.environ.get("COUCHBASE_CAPELLA_USERNAME")
    or not os.environ.get("COUCHBASE_CAPELLA_PASSWORD"),
    reason="Couchbase Capella credentials not set",
)
@pytest.mark.parametrize(
    "dest", list(DESTINATIONS.values()), ids=list(DESTINATIONS.keys())
)
def test_couchbase_capella_source_without_bucket_in_uri(dest):
    """
    Test Couchbase Capella with bucket specified in table name instead of URI.
    Uses SSL connection with bucket as part of the table identifier.
    """
    username = os.environ.get("COUCHBASE_CAPELLA_USERNAME")
    password = os.environ.get("COUCHBASE_CAPELLA_PASSWORD")
    host = os.environ.get(
        "COUCHBASE_CAPELLA_HOST", "cb.8vm1qjx5nowztp08.cloud.couchbase.com"
    )
    bucket = os.environ.get("COUCHBASE_CAPELLA_BUCKET", "travel-sample")
    scope = os.environ.get("COUCHBASE_CAPELLA_SCOPE", "inventory")
    collection = os.environ.get("COUCHBASE_CAPELLA_COLLECTION", "airline")

    # Test without bucket in URI - bucket is part of table name
    source_uri = f"couchbase://{username}:{password}@{host}?ssl=true"
    source_table = f"{bucket}.{scope}.{collection}"

    dest_uri = dest.start()
    dest_table = "raw.couchbase_capella_test2"

    try:
        result = invoke_ingest_command(
            source_uri,
            source_table,
            dest_uri,
            dest_table,
        )

        assert result.exit_code == 0, f"Command failed with: {result.output}"

        # Verify data was ingested
        with sqlalchemy.create_engine(dest_uri).connect() as conn:
            res = conn.exec_driver_sql(f"select * from {dest_table}").fetchall()
            assert len(res) > 0, "No data was ingested from Couchbase Capella"
            print(
                f"Successfully ingested {len(res)} documents from Couchbase Capella (bucket in table name)"
            )
    finally:
        dest.stop()


@pytest.mark.skipif(
    not os.environ.get("COUCHBASE_SERVER_USERNAME")
    or not os.environ.get("COUCHBASE_SERVER_PASSWORD"),
    reason="Couchbase Server credentials not set",
)
@pytest.mark.parametrize(
    "dest", list(DESTINATIONS.values()), ids=list(DESTINATIONS.keys())
)
def test_couchbase_server_source(dest):
    """
    Test Couchbase Server (self-hosted) as a source.

    Required environment variables:
    - COUCHBASE_SERVER_USERNAME
    - COUCHBASE_SERVER_PASSWORD
    - COUCHBASE_SERVER_HOST (optional, defaults to localhost)
    - COUCHBASE_SERVER_BUCKET (optional, defaults to default)
    - COUCHBASE_SERVER_SCOPE (optional, defaults to _default)
    - COUCHBASE_SERVER_COLLECTION (optional, defaults to _default)
    """
    username = os.environ.get("COUCHBASE_SERVER_USERNAME")
    password = os.environ.get("COUCHBASE_SERVER_PASSWORD")
    host = os.environ.get("COUCHBASE_SERVER_HOST", "localhost")
    bucket = os.environ.get("COUCHBASE_SERVER_BUCKET", "default")
    scope = os.environ.get("COUCHBASE_SERVER_SCOPE", "_default")
    collection = os.environ.get("COUCHBASE_SERVER_COLLECTION", "_default")

    # Couchbase Server typically doesn't require SSL for local connections
    source_uri = f"couchbase://{username}:{password}@{host}/{bucket}"
    source_table = f"{scope}.{collection}"

    dest_uri = dest.start()
    dest_table = "raw.couchbase_server_test"

    try:
        result = invoke_ingest_command(
            source_uri,
            source_table,
            dest_uri,
            dest_table,
        )

        assert result.exit_code == 0, f"Command failed with: {result.output}"

        # Verify data was ingested
        with sqlalchemy.create_engine(dest_uri).connect() as conn:
            res = conn.exec_driver_sql(f"select * from {dest_table}").fetchall()
            assert len(res) > 0, "No data was ingested from Couchbase Server"
            print(f"Successfully ingested {len(res)} documents from Couchbase Server")
    finally:
        dest.stop()
