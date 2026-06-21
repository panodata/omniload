import traceback
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, List
from urllib.parse import urlparse

import pendulum
import pytest

from tests.container.floci import FlociContainer
from tests.util import get_random_string, invoke_ingest_command
from tests.warehouse.container import DESTINATIONS
from tests.warehouse.operations import get_query_result


@dataclass
class DynamoDBTestConfig:
    db_name: str
    uri: str
    data: List[Dict]


@pytest.fixture(scope="session")
def dynamodb():
    db_name = f"dynamodb_test_{get_random_string(5)}"
    table_cfg = {
        "TableName": db_name,
        "KeySchema": [
            {
                "AttributeName": "id",
                "KeyType": "HASH",
            }
        ],
        "AttributeDefinitions": [
            {"AttributeName": "id", "AttributeType": "S"},
        ],
        "ProvisionedThroughput": {
            "ReadCapacityUnits": 35000,
            "WriteCapacityUnits": 35000,
        },
    }

    items = [
        {"id": {"S": "1"}, "updated_at": {"S": "2024-01-01T00:00:00"}},
        {"id": {"S": "2"}, "updated_at": {"S": "2024-02-01T00:00:00"}},
        {"id": {"S": "3"}, "updated_at": {"S": "2024-03-01T00:00:00"}},
    ]

    def load_test_data(ls):
        client = ls.get_client("dynamodb")
        client.create_table(**table_cfg)
        for item in items:
            client.put_item(TableName=db_name, Item=item)

    def items_to_list(items):
        """converts dynamodb item list to list of dics"""
        result = []
        for i in items:
            entry = {}
            for key, val in i.items():
                entry[key] = list(val.values())[0]
            result.append(entry)
        return result

    floci = FlociContainer(image="docker.io/floci/floci:1.5.25")
    floci.start()
    load_test_data(floci)

    dynamodb_url = urlparse(floci.get_url())
    src_uri = (
        f"dynamodb://{dynamodb_url.netloc}?"
        + f"region={floci.env['AWS_DEFAULT_REGION']}&"
        + f"access_key_id={floci.env['AWS_ACCESS_KEY_ID']}&"
        + f"secret_access_key={floci.env['AWS_SECRET_ACCESS_KEY']}"
    )
    yield DynamoDBTestConfig(
        db_name,
        src_uri,
        items_to_list(items),
    )

    floci.stop()


def dynamodb_tests() -> Iterable[Callable]:
    def assert_success(result):
        if result.exception is not None:
            traceback.print_exception(*result.exc_info)
            raise AssertionError(result.exception)

    def smoke_test(dest_uri, dynamodb):
        dest_table = f"public.dynamodb_{get_random_string(5)}"

        result = invoke_ingest_command(
            dynamodb.uri, dynamodb.db_name, dest_uri, dest_table, "append", "updated_at"
        )
        assert_success(result)

        result = get_query_result(
            dest_uri, f"select id, updated_at from {dest_table} ORDER BY id"
        )
        assert len(result) == 3
        for i in range(len(result)):
            assert result[i][0] == dynamodb.data[i]["id"]
            assert result[i][1] == pendulum.parse(dynamodb.data[i]["updated_at"])

    def append_test(dest_uri, dynamodb):
        dest_table = f"public.dynamodb_{get_random_string(5)}"

        # we run it twice to assert that the data in destination doesn't change
        for i in range(2):
            result = invoke_ingest_command(
                dynamodb.uri,
                dynamodb.db_name,
                dest_uri,
                dest_table,
                "append",
                "updated_at",
            )

            assert_success(result)
            result = get_query_result(
                dest_uri, f"select id, updated_at from {dest_table} ORDER BY id"
            )
            assert len(result) == 3
            for i in range(len(result)):
                assert result[i][0] == dynamodb.data[i]["id"]
                assert result[i][1] == pendulum.parse(dynamodb.data[i]["updated_at"])

    def incremental_test_factory(strategy):
        def incremental_test(dest_uri, dynamodb):
            dest_table = f"public.dynamodb_{get_random_string(5)}"

            result = invoke_ingest_command(
                dynamodb.uri,
                dynamodb.db_name,
                dest_uri,
                dest_table,
                inc_strategy=strategy,
                inc_key="updated_at",
                interval_start="2024-01-01T00:00:00",
                interval_end="2024-02-01T00:01:00",  # upto the second entry
            )
            assert_success(result)
            rows = get_query_result(
                dest_uri, f"select id, updated_at from {dest_table} ORDER BY id"
            )
            assert len(rows) == 2
            for i in range(len(rows)):
                assert rows[i][0] == dynamodb.data[i]["id"]
                assert rows[i][1] == pendulum.parse(dynamodb.data[i]["updated_at"])

            # ingest the rest
            # run it twice to test idempotency
            for _ in range(2):
                result = invoke_ingest_command(
                    dynamodb.uri,
                    dynamodb.db_name,
                    dest_uri,
                    dest_table,
                    inc_strategy=strategy,
                    inc_key="updated_at",
                    interval_start="2024-02-01T00:00:00",  # second entry onwards
                )
                assert_success(result)

                rows = get_query_result(
                    dest_uri, f"select id, updated_at from {dest_table} ORDER BY id"
                )
                rows_expected = 3
                if strategy == "replace":
                    # old rows are removed in replace
                    rows_expected = 2

                assert len(rows) == rows_expected
                for row in rows:
                    id = int(row[0]) - 1
                    assert row[0] == dynamodb.data[id]["id"]
                    assert row[1] == pendulum.parse(dynamodb.data[id]["updated_at"])

        # for easier debugging
        incremental_test.__name__ += f"_{strategy}"
        return incremental_test

    strategies = [
        "replace",
        "delete+insert",
        "merge",
    ]
    incremental_tests = [incremental_test_factory(strat) for strat in strategies]

    return [
        smoke_test,
        append_test,
        *incremental_tests,
    ]


@pytest.mark.parametrize(
    "dest", list(DESTINATIONS.values()), ids=list(DESTINATIONS.keys())
)
@pytest.mark.parametrize("testcase", dynamodb_tests())
def test_dynamodb(dest, dynamodb, testcase):
    testcase(dest.start(), dynamodb)
    dest.stop()
