import os
import unittest
from unittest.mock import MagicMock

import pytest
import sqlalchemy

from omniload.src.couchbase_source.helpers import fetch_documents
from tests.util import invoke_ingest_command
from tests.warehouse.container import COUCHBASE_IMAGE, DESTINATIONS, CouchbaseContainer


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


class TestFetchDocuments(unittest.TestCase):
    """
    Test fetch_documents function.
    Test helpers.py - verifies fetch_documents returns all fields, not just id.
    """

    def test_returns_all_fields_not_just_id(self):
        """Critical test: verify all fields are returned, not just id."""
        # Mock cluster
        mock_cluster = MagicMock()

        # Mock query result with multiple fields
        mock_result = [
            {
                "id": "airport_1",
                "name": "San Francisco International",
                "code": "SFO",
                "city": "San Francisco",
                "country": "USA",
            },
            {
                "id": "airport_2",
                "name": "Los Angeles International",
                "code": "LAX",
                "city": "Los Angeles",
                "country": "USA",
            },
        ]
        mock_cluster.query.return_value = iter(mock_result)

        # Fetch documents
        docs = list(
            fetch_documents(
                cluster=mock_cluster,
                bucket_name="test",
                scope_name="scope",
                collection_name="collection",
                incremental=None,
                limit=None,
            )
        )

        # CRITICAL: Verify we got ALL fields, not just id
        self.assertEqual(len(docs), 2)

        # Check first document has all fields
        first = docs[0]
        self.assertEqual(first["id"], "airport_1")
        self.assertEqual(first["name"], "San Francisco International")
        self.assertEqual(first["code"], "SFO")
        self.assertEqual(first["city"], "San Francisco")
        self.assertEqual(first["country"], "USA")

        # Check second document
        second = docs[1]
        self.assertEqual(second["id"], "airport_2")
        self.assertEqual(second["code"], "LAX")

    def test_query_uses_alias_format(self):
        """Test query uses 'c.*' format (not full path)."""
        mock_cluster = MagicMock()
        mock_cluster.query.return_value = iter([])

        list(
            fetch_documents(
                cluster=mock_cluster,
                bucket_name="bucket",
                scope_name="scope",
                collection_name="collection",
                incremental=None,
                limit=None,
            )
        )

        # Get the query that was called
        query = mock_cluster.query.call_args[0][0]

        # CRITICAL: Should use "c.*" not "bucket.scope.collection.*"
        self.assertIn("c.*", query)
        self.assertIn("FROM `bucket`.`scope`.`collection` c", query)

        # Should NOT have the full path in SELECT
        self.assertNotIn("`bucket`.`scope`.`collection`.*", query)

    def test_limit_parameter(self):
        """Test limit is applied to query."""
        mock_cluster = MagicMock()
        mock_cluster.query.return_value = iter([])

        list(
            fetch_documents(
                cluster=mock_cluster,
                bucket_name="bucket",
                scope_name="scope",
                collection_name="collection",
                incremental=None,
                limit=10,
            )
        )

        query = mock_cluster.query.call_args[0][0]
        self.assertIn("LIMIT 10", query)

    def test_no_limit_by_default(self):
        """Test no limit when limit=None."""
        mock_cluster = MagicMock()
        mock_cluster.query.return_value = iter([])

        list(
            fetch_documents(
                cluster=mock_cluster,
                bucket_name="bucket",
                scope_name="scope",
                collection_name="collection",
                incremental=None,
                limit=None,
            )
        )

        query = mock_cluster.query.call_args[0][0]
        self.assertNotIn("LIMIT", query)

    def test_meta_id_selected(self):
        """Test META().id is selected as id field."""
        mock_cluster = MagicMock()
        mock_cluster.query.return_value = iter([])

        list(
            fetch_documents(
                cluster=mock_cluster,
                bucket_name="bucket",
                scope_name="scope",
                collection_name="collection",
                incremental=None,
                limit=None,
            )
        )

        query = mock_cluster.query.call_args[0][0]
        self.assertIn("META().id as id", query)

    def test_empty_result(self):
        """Test handles empty result gracefully."""
        mock_cluster = MagicMock()
        mock_cluster.query.return_value = iter([])

        docs = list(
            fetch_documents(
                cluster=mock_cluster,
                bucket_name="bucket",
                scope_name="scope",
                collection_name="collection",
                incremental=None,
                limit=None,
            )
        )

        self.assertEqual(len(docs), 0)
