import json
from concurrent.futures import ThreadPoolExecutor

import pytest
import sqlalchemy
from confluent_kafka import Producer

from tests.util import invoke_ingest_command
from tests.warehouse.settings import DESTINATIONS

# Marked explicitly (not auto-marked by path) because this module lives outside tests/warehouse.
pytestmark = pytest.mark.integration


@pytest.mark.parametrize(
    "dest", list(DESTINATIONS.values()), ids=list(DESTINATIONS.keys())
)
def test_kafka_to_db_incremental(kafka, dest, topic):
    """
    Validate standard Kafka event decoding, focusing on both metadata and data payload.
    """
    with ThreadPoolExecutor() as executor:
        dest_future = executor.submit(dest.start)
        dest_uri = dest_future.result()

    # Create Kafka producer
    producer = Producer({"bootstrap.servers": kafka})

    # Create topic and send messages
    messages = ["message1", "message2", "message3"]

    for message in messages:
        producer.produce(topic, message.encode("utf-8"))
    producer.flush()

    def run():
        res = invoke_ingest_command(
            f"kafka://?bootstrap_servers={kafka}&group_id=test_group",
            topic,
            dest_uri,
            f"{topic}.output",
        )
        assert res.exit_code == 0

    def get_output_table():
        dest_engine = sqlalchemy.create_engine(dest_uri)
        with dest_engine.connect() as conn:
            res = conn.exec_driver_sql(
                f"select _kafka__data from {topic}.output order by _kafka_msg_id asc"
            ).fetchall()
        dest_engine.dispose()
        return res

    run()

    res = get_output_table()
    assert len(res) == 3
    assert res[0] == ("message1",)
    assert res[1] == ("message2",)
    assert res[2] == ("message3",)

    # run again, nothing should be inserted into the output table
    run()

    res = get_output_table()
    assert len(res) == 3
    assert res[0] == ("message1",)
    assert res[1] == ("message2",)
    assert res[2] == ("message3",)

    # add a new message
    producer.produce(topic, "message4".encode("utf-8"))
    producer.flush()

    # run again, the new message should be inserted into the output table
    run()
    res = get_output_table()
    assert len(res) == 4
    assert res[0] == ("message1",)
    assert res[1] == ("message2",)
    assert res[2] == ("message3",)
    assert res[3] == ("message4",)


@pytest.mark.parametrize(
    "dest", list(DESTINATIONS.values()), ids=list(DESTINATIONS.keys())
)
def test_kafka_to_db_decode_json(kafka, dest, topic):
    """
    Validate slightly more advanced Kafka event decoding, focusing on the payload value this time.

    This exercise uses the `value_type=json` and `select=value` URL parameters.
    """
    with ThreadPoolExecutor() as executor:
        dest_future = executor.submit(dest.start)
        dest_uri = dest_future.result()

    # Create Kafka producer
    producer = Producer({"bootstrap.servers": kafka})

    # Create topic and send messages
    messages = [
        {"id": 1, "temperature": 42.42, "humidity": 82},
        {"id": 2, "temperature": 451.00, "humidity": 15},
    ]

    for message in messages:
        producer.produce(topic, json.dumps(message))
    producer.flush()

    def run():
        res = invoke_ingest_command(
            f"kafka://?bootstrap_servers={kafka}&group_id=test_group&value_type=json&select=value",
            topic,
            dest_uri,
            f"{topic}.output",
        )
        assert res.exit_code == 0

    def get_output_table():
        dest_engine = sqlalchemy.create_engine(dest_uri)
        with dest_engine.connect() as conn:
            res = (
                conn.exec_driver_sql(  # ty: ignore[no-matching-overload, unused-ignore-comment, unused-ignore-comment]
                    f"SELECT id, temperature, humidity FROM {topic}.output WHERE temperature >= 38.00 ORDER BY id ASC"
                )
                .mappings()
                .fetchall()
            )
        dest_engine.dispose()
        return res

    run()

    res = get_output_table()
    assert len(res) == 2
    assert res[0] == messages[0]
    assert res[1] == messages[1]


@pytest.mark.parametrize(
    "dest", list(DESTINATIONS.values()), ids=list(DESTINATIONS.keys())
)
def test_kafka_to_db_include_metadata(kafka, dest, topic):
    """
    Validate slightly more advanced Kafka event decoding, focusing on metadata this time.

    This exercise uses the `include=` URL parameter.
    """
    with ThreadPoolExecutor() as executor:
        dest_future = executor.submit(dest.start)
        dest_uri = dest_future.result()

    # Create Kafka producer
    producer = Producer({"bootstrap.servers": kafka})

    # Create topic and send messages
    messages = [
        {"id": 1, "temperature": 42.42, "humidity": 82},
        {"id": 2, "temperature": 451.00, "humidity": 15},
    ]

    for message in messages:
        producer.produce(topic=topic, value=json.dumps(message), key="test")
    producer.flush()

    def run():
        res = invoke_ingest_command(
            f"kafka://?bootstrap_servers={kafka}&group_id=test_group&include=partition,topic,key,offset,ts",
            topic,
            dest_uri,
            f"{topic}.output",
        )
        assert res.exit_code == 0

    def get_output_table():
        dest_engine = sqlalchemy.create_engine(dest_uri)
        with dest_engine.connect() as conn:
            res = (
                conn.exec_driver_sql(
                    f'SELECT "partition", "topic", "key", "offset" FROM {topic}.output ORDER BY "partition" ASC, "offset" ASC'
                )
                .mappings()
                .fetchall()
            )
        dest_engine.dispose()
        return res

    run()

    res = get_output_table()
    assert len(res) == 2
    assert res[0] == {"partition": 0, "topic": topic, "key": "test", "offset": 0}
    assert res[1] == {"partition": 0, "topic": topic, "key": "test", "offset": 1}
