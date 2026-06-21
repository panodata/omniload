import json
import tempfile
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
import pytest
import sqlalchemy
from pyarrow import ipc
from testcontainers.mongodb import MongoDbContainer

from tests.util import get_random_string, invoke_ingest_command
from tests.warehouse.container import DESTINATIONS, MONGODB_IMAGE


def mongodb_test_cases():
    def smoke_test(mongo):
        collection = f"smoke_test_{get_random_string(5)}"
        result = invoke_ingest_command(
            "csv://omniload/testdata/create_replace.csv",
            "raw.input",
            mongo.get_connection_url(),
            collection,
        )
        assert result.exit_code == 0

        client = mongo.get_connection_client()
        assert client["omniload_db"][collection].count_documents({}) == 20

    def large_insert(mongo):
        """
        Insert more than batch_size items.
        """
        DOC_COUNT = 5000
        table = pa.Table.from_pandas(
            pd.DataFrame(
                {
                    "id": range(DOC_COUNT),
                }
            )
        )
        with tempfile.NamedTemporaryFile(suffix=".arrow") as fd:
            with pa.OSFile(fd.name, "wb") as f:
                writer = ipc.new_file(f, table.schema)
                writer.write_table(table)
                writer.close()

            collection = f"large_insert_{get_random_string(5)}"
            result = invoke_ingest_command(
                f"mmap://{fd.name}",
                "raw.input",
                mongo.get_connection_url(),
                collection,
            )
            assert result.exit_code == 0

            client = mongo.get_connection_client()
            assert client["omniload_db"][collection].count_documents({}) == DOC_COUNT

    def merge_with_primary_key(mongo):
        """
        Test merge disposition with primary key.
        """
        collection = f"merge_test_{get_random_string(5)}"

        initial_data = pa.Table.from_pandas(
            pd.DataFrame(
                {
                    "user_id": [1, 2, 3],
                    "name": ["Alice", "Bob", "Charlie"],
                    "age": [25, 30, 35],
                }
            )
        )

        with tempfile.NamedTemporaryFile(suffix=".arrow") as fd:
            with pa.OSFile(fd.name, "wb") as f:
                writer = ipc.new_file(f, initial_data.schema)
                writer.write_table(initial_data)
                writer.close()

            result = invoke_ingest_command(
                f"mmap://{fd.name}",
                "raw.input",
                mongo.get_connection_url(),
                collection,
                primary_key="user_id",
                inc_strategy="merge",
            )
            assert result.exit_code == 0

        client = mongo.get_connection_client()
        assert client["omniload_db"][collection].count_documents({}) == 3

        updated_data = pa.Table.from_pandas(
            pd.DataFrame(
                {
                    "user_id": [2, 3, 4],
                    "name": ["Bob Updated", "Charlie Updated", "Diana"],
                    "age": [31, 36, 28],
                }
            )
        )

        with tempfile.NamedTemporaryFile(suffix=".arrow") as fd:
            with pa.OSFile(fd.name, "wb") as f:
                writer = ipc.new_file(f, updated_data.schema)
                writer.write_table(updated_data)
                writer.close()

            result = invoke_ingest_command(
                f"mmap://{fd.name}",
                "raw.input",
                mongo.get_connection_url(),
                collection,
                primary_key="user_id",
                inc_strategy="merge",
            )
            assert result.exit_code == 0

        assert client["omniload_db"][collection].count_documents({}) == 4

        alice = client["omniload_db"][collection].find_one({"user_id": 1})
        assert alice is not None
        assert alice["name"] == "Alice"
        assert alice["age"] == 25

        bob = client["omniload_db"][collection].find_one({"user_id": 2})
        assert bob["name"] == "Bob Updated"
        assert bob["age"] == 31

        charlie = client["omniload_db"][collection].find_one({"user_id": 3})
        assert charlie["name"] == "Charlie Updated"
        assert charlie["age"] == 36

        diana = client["omniload_db"][collection].find_one({"user_id": 4})
        assert diana is not None
        assert diana["name"] == "Diana"
        assert diana["age"] == 28

    def merge_without_primary_key(mongo):
        """
        Test that merge disposition fails when no primary key is specified.
        """
        collection = f"merge_no_pk_{get_random_string(5)}"

        initial_data = pa.Table.from_pandas(
            pd.DataFrame(
                {
                    "user_id": [1, 2, 3],
                    "name": ["Alice", "Bob", "Charlie"],
                }
            )
        )

        with tempfile.NamedTemporaryFile(suffix=".arrow") as fd:
            with pa.OSFile(fd.name, "wb") as f:
                writer = ipc.new_file(f, initial_data.schema)
                writer.write_table(initial_data)
                writer.close()

            result = invoke_ingest_command(
                f"mmap://{fd.name}",
                "raw.input",
                mongo.get_connection_url(),
                collection,
                inc_strategy="merge",
            )
            # Should fail because no primary key is specified
            assert result.exit_code != 0
            assert (
                "merge operation requires primary keys" in result.output.lower()
                or "primary key" in result.output.lower()
            )

    def merge_with_multiple_primary_keys(mongo):
        """
        Test merge disposition with multiple primary keys.
        Uses user_id and category as composite primary key.
        """
        collection = f"merge_multi_pk_{get_random_string(5)}"

        initial_data = pa.Table.from_pandas(
            pd.DataFrame(
                {
                    "user_id": [1, 1, 2, 2],
                    "category": ["A", "B", "A", "B"],
                    "value": [100, 200, 300, 400],
                }
            )
        )

        with tempfile.NamedTemporaryFile(suffix=".arrow") as fd:
            with pa.OSFile(fd.name, "wb") as f:
                writer = ipc.new_file(f, initial_data.schema)
                writer.write_table(initial_data)
                writer.close()

            result = invoke_ingest_command(
                f"mmap://{fd.name}",
                "raw.input",
                mongo.get_connection_url(),
                collection,
                primary_key=["user_id", "category"],
                inc_strategy="merge",
            )
            assert result.exit_code == 0

        client = mongo.get_connection_client()
        assert client["omniload_db"][collection].count_documents({}) == 4

        # Now update some records and add a new one
        updated_data = pa.Table.from_pandas(
            pd.DataFrame(
                {
                    "user_id": [1, 2, 3],
                    "category": ["A", "B", "A"],
                    "value": [150, 450, 500],
                }
            )
        )

        with tempfile.NamedTemporaryFile(suffix=".arrow") as fd:
            with pa.OSFile(fd.name, "wb") as f:
                writer = ipc.new_file(f, updated_data.schema)
                writer.write_table(updated_data)
                writer.close()

            result = invoke_ingest_command(
                f"mmap://{fd.name}",
                "raw.input",
                mongo.get_connection_url(),
                collection,
                primary_key=["user_id", "category"],
                inc_strategy="merge",
            )
            assert result.exit_code == 0

        # Should have 5 documents now: (1,B), (2,A), (1,A updated), (2,B updated), (3,A new)
        assert client["omniload_db"][collection].count_documents({}) == 5

        # Check updated records
        user1_cat_a = client["omniload_db"][collection].find_one(
            {"user_id": 1, "category": "A"}
        )
        assert user1_cat_a["value"] == 150

        user2_cat_b = client["omniload_db"][collection].find_one(
            {"user_id": 2, "category": "B"}
        )
        assert user2_cat_b["value"] == 450

        # Check non-updated record
        user1_cat_b = client["omniload_db"][collection].find_one(
            {"user_id": 1, "category": "B"}
        )
        assert user1_cat_b["value"] == 200

        # Check new record
        user3_cat_a = client["omniload_db"][collection].find_one(
            {"user_id": 3, "category": "A"}
        )
        assert user3_cat_a is not None
        assert user3_cat_a["value"] == 500

    return [
        smoke_test,
        large_insert,
        merge_with_primary_key,
        merge_without_primary_key,
        merge_with_multiple_primary_keys,
    ]


@pytest.fixture(scope="session")
def mongodb_server():
    container = MongoDbContainer(MONGODB_IMAGE)
    container.start()
    yield container
    container.stop()


@pytest.mark.parametrize("testcase", mongodb_test_cases())
def test_mongodb_dest(testcase, mongodb_server):
    testcase(mongodb_server)


@pytest.mark.parametrize(
    "dest", list(DESTINATIONS.values()), ids=list(DESTINATIONS.keys())
)
def test_mongodb_source(dest):
    mongo = MongoDbContainer("mongo:7.0.7")
    mongo.start()

    db = mongo.get_connection_client()
    test_collection = db.test_db.test_collection
    test_collection.insert_many(
        [
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
            {
                "id": 4,
                "name": "Document 4",
                "nested_parent": {
                    "key1": "value4",
                    "key2": {"nested1": "value4"},
                    "key3": [{"nested3": "value4"}],
                },
                "key4": ["value1", "value2", "value3"],
                "value": 400,
            },
            {
                "id": 5,
                "name": "Document 5",
                "nested_parent": {
                    "key1": "value5",
                    "key2": {"nested1": "value5"},
                    "key3": [{"nested3": "value5"}],
                },
                "key4": ["value1", "value2", "value3"],
                "value": 500,
            },
        ]
    )

    dest_uri = dest.start()

    try:
        invoke_ingest_command(
            mongo.get_connection_url(),
            "test_db.test_collection",
            dest_uri,
            "raw.test_collection",
        )

        engine = sqlalchemy.create_engine(dest_uri)
        with engine.connect() as conn:
            res = conn.exec_driver_sql(
                "select id, name, nested_parent__key1, nested_parent__key2, nested_parent__key3, key4, value from raw.test_collection"
            ).fetchall()
        engine.dispose()

        assert len(res) == 5

        # convert string to json if needed. this is a particular case for Clickhouse which does not have json types by default.
        res = [
            (
                row[0],
                row[1],
                row[2],
                json.loads(row[3]) if isinstance(row[3], str) else row[3],
                json.loads(row[4]) if isinstance(row[4], str) else row[4],
                json.loads(row[5]) if isinstance(row[5], str) else row[5],
                row[6],
            )
            for row in res
        ]

        assert res[0] == (
            1,
            "Document 1",
            "value1",
            {"nested1": "value1"},
            [{"nested3": "value1"}],
            ["value1", "value2", "value3"],
            100,
        )
        assert res[1] == (
            2,
            "Document 2",
            "value2",
            {"nested1": "value2"},
            [{"nested3": "value2"}],
            ["value1", "value2", "value3"],
            200,
        )
        assert res[2] == (
            3,
            "Document 3",
            "value3",
            {"nested1": "value3"},
            [{"nested3": "value3"}],
            ["value1", "value2", "value3"],
            300,
        )
        assert res[3] == (
            4,
            "Document 4",
            "value4",
            {"nested1": "value4"},
            [{"nested3": "value4"}],
            ["value1", "value2", "value3"],
            400,
        )
        assert res[4] == (
            5,
            "Document 5",
            "value5",
            {"nested1": "value5"},
            [{"nested3": "value5"}],
            ["value1", "value2", "value3"],
            500,
        )
    finally:
        dest.stop()
        mongo.stop()


def mongodb_custom_query_test_cases():
    def simple_filtering_query(dest_uri: str):
        """Test simple aggregation query with filtering"""
        mongo = MongoDbContainer("mongo:7.0.7")
        mongo.start()

        try:
            db = mongo.get_connection_client()
            test_collection = db.test_db.events

            # Insert test data
            test_collection.insert_many(
                [
                    {
                        "event_id": 1,
                        "event_type": "login",
                        "user_id": "user1",
                        "status": "success",
                        "value": 100,
                    },
                    {
                        "event_id": 2,
                        "event_type": "purchase",
                        "user_id": "user1",
                        "status": "success",
                        "value": 250,
                    },
                    {
                        "event_id": 3,
                        "event_type": "login",
                        "user_id": "user2",
                        "status": "success",
                        "value": 150,
                    },
                    {
                        "event_id": 4,
                        "event_type": "purchase",
                        "user_id": "user2",
                        "status": "failed",
                        "value": 300,
                    },
                    {
                        "event_id": 5,
                        "event_type": "logout",
                        "user_id": "user1",
                        "status": "success",
                        "value": 50,
                    },
                ]
            )

            # Test simple filtering query
            custom_query = '[{"$match": {"status": "success"}}, {"$project": {"_id": 1, "event_id": 1, "event_type": 1, "user_id": 1, "value": 1}}]'
            schema_rand_prefix = f"testschema_mongo_filter_{get_random_string(5)}"

            result = invoke_ingest_command(
                mongo.get_connection_url(),
                f"test_db.events:{custom_query}",
                dest_uri,
                f"{schema_rand_prefix}.events_success",
            )

            assert result.exit_code == 0

            engine = sqlalchemy.create_engine(dest_uri)
            with engine.connect() as conn:
                res = conn.exec_driver_sql(
                    f"select event_id, event_type, user_id, value from {schema_rand_prefix}.events_success order by event_id"
                ).fetchall()
            engine.dispose()

            assert len(res) == 4  # Only successful events
            assert res[0] == (1, "login", "user1", 100)
            assert res[1] == (2, "purchase", "user1", 250)
            assert res[2] == (3, "login", "user2", 150)
            assert res[3] == (5, "logout", "user1", 50)

        finally:
            mongo.stop()

    def aggregation_with_grouping(dest_uri: str):
        """Test aggregation query with grouping operations"""
        mongo = MongoDbContainer("mongo:7.0.7")
        mongo.start()

        try:
            db = mongo.get_connection_client()
            test_collection = db.test_db.events

            # Insert test data
            test_collection.insert_many(
                [
                    {
                        "event_id": 1,
                        "event_type": "login",
                        "user_id": "user1",
                        "status": "success",
                        "value": 100,
                    },
                    {
                        "event_id": 2,
                        "event_type": "purchase",
                        "user_id": "user1",
                        "status": "success",
                        "value": 250,
                    },
                    {
                        "event_id": 3,
                        "event_type": "login",
                        "user_id": "user2",
                        "status": "success",
                        "value": 150,
                    },
                    {
                        "event_id": 4,
                        "event_type": "purchase",
                        "user_id": "user2",
                        "status": "failed",
                        "value": 300,
                    },
                    {
                        "event_id": 5,
                        "event_type": "logout",
                        "user_id": "user1",
                        "status": "success",
                        "value": 50,
                    },
                ]
            )

            # Test aggregation with grouping
            group_query = '[{"$match": {"status": "success"}}, {"$group": {"_id": "$user_id", "total_value": {"$sum": "$value"}, "event_count": {"$sum": 1}}}]'
            schema_rand_prefix = f"testschema_mongo_group_{get_random_string(5)}"

            result = invoke_ingest_command(
                mongo.get_connection_url(),
                f"test_db.events:{group_query}",
                dest_uri,
                f"{schema_rand_prefix}.user_stats",
            )

            assert result.exit_code == 0

            engine = sqlalchemy.create_engine(dest_uri)
            with engine.connect() as conn:
                res = conn.exec_driver_sql(
                    f"select _id, total_value, event_count from {schema_rand_prefix}.user_stats order by _id"
                ).fetchall()
            engine.dispose()

            assert len(res) == 2
            assert res[0] == (
                "user1",
                400,
                3,
            )  # user1: 100 + 250 + 50 = 400, 3 events
            assert res[1] == (
                "user2",
                150,
                1,
            )  # user2: only 150 from login, 1 event

        finally:
            mongo.stop()

    def incremental_with_interval_placeholders(dest_uri: str):
        """Test incremental load with interval placeholders"""
        mongo = MongoDbContainer("mongo:7.0.7")
        mongo.start()

        try:
            db = mongo.get_connection_client()
            test_collection = db.test_db.events

            # Insert test data with timestamps
            test_collection.insert_many(
                [
                    {
                        "event_id": 1,
                        "event_type": "login",
                        "user_id": "user1",
                        "timestamp": datetime(
                            2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc
                        ),
                        "status": "success",
                        "value": 100,
                    },
                    {
                        "event_id": 2,
                        "event_type": "purchase",
                        "user_id": "user1",
                        "timestamp": datetime(
                            2024, 1, 1, 11, 0, 0, tzinfo=timezone.utc
                        ),
                        "status": "success",
                        "value": 250,
                    },
                    {
                        "event_id": 3,
                        "event_type": "login",
                        "user_id": "user2",
                        "timestamp": datetime(2024, 1, 2, 9, 0, 0, tzinfo=timezone.utc),
                        "status": "success",
                        "value": 150,
                    },
                    {
                        "event_id": 4,
                        "event_type": "purchase",
                        "user_id": "user2",
                        "timestamp": datetime(
                            2024, 1, 2, 10, 0, 0, tzinfo=timezone.utc
                        ),
                        "status": "failed",
                        "value": 300,
                    },
                ]
            )

            # Test incremental load with interval placeholders
            incremental_query = '[{"$match": {"timestamp": {"$gte": ":interval_start", "$lt": ":interval_end"}, "status": "success"}}, {"$project": {"_id": 1, "event_id": 1, "event_type": 1, "user_id": 1, "timestamp": 1, "value": 1}}]'
            schema_rand_prefix = f"testschema_mongo_incr_{get_random_string(5)}"

            result = invoke_ingest_command(
                mongo.get_connection_url(),
                f"test_db.events:{incremental_query}",
                dest_uri,
                f"{schema_rand_prefix}.events_incremental",
                inc_strategy="append",
                inc_key="timestamp",
                interval_start="2024-01-01T00:00:00+00:00",
                interval_end="2024-01-02T00:00:00+00:00",
            )

            assert result.exit_code == 0

            engine = sqlalchemy.create_engine(dest_uri)
            with engine.connect() as conn:
                res = conn.exec_driver_sql(
                    f"select event_id, event_type, user_id, value from {schema_rand_prefix}.events_incremental order by event_id"
                ).fetchall()
            engine.dispose()

            # Should only get events from 2024-01-01 (events 1 and 2)
            assert len(res) == 2
            assert res[0] == (1, "login", "user1", 100)
            assert res[1] == (2, "purchase", "user1", 250)

        finally:
            mongo.stop()

    def incremental_multiple_days(dest_uri: str):
        """Test incremental load across multiple days"""
        mongo = MongoDbContainer("mongo:7.0.7")
        mongo.start()

        try:
            db = mongo.get_connection_client()
            test_collection = db.test_db.events

            # Insert test data with timestamps
            test_collection.insert_many(
                [
                    {
                        "event_id": 1,
                        "event_type": "login",
                        "user_id": "user1",
                        "timestamp": datetime(
                            2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc
                        ),
                        "status": "success",
                        "value": 100,
                    },
                    {
                        "event_id": 2,
                        "event_type": "purchase",
                        "user_id": "user1",
                        "timestamp": datetime(
                            2024, 1, 1, 11, 0, 0, tzinfo=timezone.utc
                        ),
                        "status": "success",
                        "value": 250,
                    },
                    {
                        "event_id": 3,
                        "event_type": "login",
                        "user_id": "user2",
                        "timestamp": datetime(2024, 1, 2, 9, 0, 0, tzinfo=timezone.utc),
                        "status": "success",
                        "value": 150,
                    },
                    {
                        "event_id": 4,
                        "event_type": "purchase",
                        "user_id": "user2",
                        "timestamp": datetime(
                            2024, 1, 2, 10, 0, 0, tzinfo=timezone.utc
                        ),
                        "status": "failed",
                        "value": 300,
                    },
                ]
            )

            incremental_query = '[{"$match": {"timestamp": {"$gte": ":interval_start", "$lt": ":interval_end"}, "status": "success"}}, {"$project": {"_id": 1, "event_id": 1, "event_type": 1, "user_id": 1, "timestamp": 1, "value": 1}}]'
            schema_rand_prefix = f"testschema_mongo_multi_{get_random_string(5)}"

            # First day
            result = invoke_ingest_command(
                mongo.get_connection_url(),
                f"test_db.events:{incremental_query}",
                dest_uri,
                f"{schema_rand_prefix}.events_multi",
                inc_strategy="append",
                inc_key="timestamp",
                interval_start="2024-01-01T00:00:00+00:00",
                interval_end="2024-01-02T00:00:00+00:00",
            )

            assert result.exit_code == 0

            # Second day
            result = invoke_ingest_command(
                mongo.get_connection_url(),
                f"test_db.events:{incremental_query}",
                dest_uri,
                f"{schema_rand_prefix}.events_multi",
                inc_strategy="append",
                inc_key="timestamp",
                interval_start="2024-01-02T00:00:00+00:00",
                interval_end="2024-01-03T00:00:00+00:00",
            )

            assert result.exit_code == 0

            engine = sqlalchemy.create_engine(dest_uri)
            with engine.connect() as conn:
                res = conn.exec_driver_sql(
                    f"select event_id, event_type, user_id, value from {schema_rand_prefix}.events_multi order by event_id"
                ).fetchall()
            engine.dispose()

            # Should have events from both days (events 1, 2, and 3)
            assert len(res) == 3
            assert res[0] == (1, "login", "user1", 100)
            assert res[1] == (2, "purchase", "user1", 250)
            assert res[2] == (3, "login", "user2", 150)

        finally:
            mongo.stop()

    return [
        simple_filtering_query,
        aggregation_with_grouping,
        incremental_with_interval_placeholders,
        incremental_multiple_days,
    ]


@pytest.mark.parametrize("testcase", mongodb_custom_query_test_cases())
@pytest.mark.parametrize(
    "dest", list(DESTINATIONS.values()), ids=list(DESTINATIONS.keys())
)
def test_mongodb_custom_query(testcase, dest):
    """Test MongoDB custom aggregation queries"""
    testcase(dest.start())
    dest.stop()
