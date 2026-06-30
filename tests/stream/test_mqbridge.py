"""Tests for the mq-bridge source connector.

The end-to-end test runs brokerless over mq-bridge's in-memory transport, so it needs no
Docker/services and stays out of the `integration` lane. The unit tests cover the
``<transport>+mqb://`` URI -> endpoint-config mapping and need no mq-bridge install.
"""

import duckdb
import pytest

from omniload.source.mqbridge.adapter import endpoint_from_uri


def test_endpoint_from_uri_kafka_strips_scheme_for_broker_list():
    transport, config = endpoint_from_uri(
        "kafka+mqb://localhost:9092?group_id=g", "events"
    )
    assert transport == "kafka"
    assert config == {
        "kafka": {"group_id": "g", "url": "localhost:9092", "topic": "events"}
    }


def test_endpoint_from_uri_nats_subject_and_type_coercion():
    transport, config = endpoint_from_uri(
        "nats+mqb://localhost:4222?prefetch_count=10&no_jetstream=true", "orders"
    )
    assert transport == "nats"
    nats = config["nats"]
    assert nats["url"] == "nats://localhost:4222"
    assert nats["subject"] == "orders"
    assert nats["prefetch_count"] == 10  # int field coerced from string
    assert nats["no_jetstream"] is True  # bool field coerced from string


def test_endpoint_from_uri_amqp_keeps_scheme_and_vhost():
    _, config = endpoint_from_uri("amqp+mqb://localhost:5672/vhost", "jobs")
    assert config["amqp"]["url"] == "amqp://localhost:5672/vhost"
    assert config["amqp"]["queue"] == "jobs"


def test_endpoint_from_uri_memory_is_topic_only():
    transport, config = endpoint_from_uri(
        "memory+mqb://?topic=orders&capacity=4096", ""
    )
    assert transport == "memory"
    assert config == {"memory": {"topic": "orders", "capacity": 4096}}


def test_endpoint_from_uri_memory_name_in_authority():
    # A bare channel name in the authority becomes the identifier (memory's canonical topic field).
    _, config = endpoint_from_uri("memory+mqb://orders?capacity=4096", "")
    assert config == {"memory": {"capacity": 4096, "topic": "orders"}}


def test_endpoint_from_uri_memory_authority_and_table_share_one_slot():
    # url and topic are aliases for one slot: the authority fills topic, and an explicit ?topic=
    # suppresses it (no redundant second identifier).
    _, config = endpoint_from_uri("memory+mqb://orders?topic=orders&capacity=4096", "")
    assert config == {"memory": {"topic": "orders", "capacity": 4096}}


def test_endpoint_from_uri_memory_ipc_via_source_table():
    # Scheme-carrying forms (ipc/unix/pipe) come through --source-table, not the authority.
    _, config = endpoint_from_uri("memory+mqb://?capacity=10", "ipc:///tmp/sock")
    assert config == {"memory": {"capacity": 10, "topic": "ipc:///tmp/sock"}}


def test_endpoint_from_uri_explicit_topic_param_wins_over_table():
    _, config = endpoint_from_uri("kafka+mqb://localhost:9092?topic=explicit", "table")
    assert config["kafka"]["topic"] == "explicit"


def test_endpoint_from_uri_kafka_multiple_brokers_kept_verbatim():
    _, config = endpoint_from_uri(
        "kafka+mqb://b1:9092,b2:9092,b3:9092?group_id=g", "events"
    )
    # Kafka's url is a comma-separated broker list; the authority is passed through as-is.
    assert config["kafka"]["url"] == "b1:9092,b2:9092,b3:9092"


def test_endpoint_from_uri_dotted_keys_expand_into_nested_tls_block():
    _, config = endpoint_from_uri(
        "kafka+mqb://b:9092?group_id=g"
        "&tls.required=true&tls.ca_file=/p/ca.pem&tls.accept_invalid_certs=false",
        "evt",
    )
    assert config["kafka"]["tls"] == {
        "required": True,  # leaf coerced to bool
        "ca_file": "/p/ca.pem",
        "accept_invalid_certs": False,
    }


def test_endpoint_from_uri_dotted_key_conflict_raises():
    with pytest.raises(ValueError):
        endpoint_from_uri("kafka+mqb://b:9092?tls=x&tls.required=true", "t")


def test_endpoint_from_uri_zeromq_builds_tcp_url():
    transport, config = endpoint_from_uri(
        "zeromq+mqb://127.0.0.1:5555?socket_type=pull&bind=true", "feed"
    )
    assert transport == "zeromq"
    assert config["zeromq"] == {
        "socket_type": "pull",
        "bind": True,
        "url": "tcp://127.0.0.1:5555",
        "topic": "feed",
    }


def test_endpoint_from_uri_aws_maps_table_to_queue_url_without_connection_url():
    transport, config = endpoint_from_uri(
        "aws+mqb://?region=us-east-1&wait_time_seconds=20",
        "https://sqs.us-east-1.amazonaws.com/123/orders",
    )
    assert transport == "aws"
    assert "url" not in config["aws"]  # SQS has no separate connection url
    assert (
        config["aws"]["queue_url"] == "https://sqs.us-east-1.amazonaws.com/123/orders"
    )
    assert config["aws"]["wait_time_seconds"] == 20


def test_endpoint_from_uri_rejects_plain_scheme():
    with pytest.raises(ValueError):
        endpoint_from_uri("kafka://localhost:9092", "t")


def test_endpoint_from_uri_rejects_unknown_transport():
    with pytest.raises(ValueError):
        endpoint_from_uri("frobnicate+mqb://host", "t")


def test_memory_transport_end_to_end(tmp_path):
    """Publish to an in-memory topic, then ingest it into DuckDB via run_ingest."""
    pytest.importorskip("mq_bridge")
    from mq_bridge import Publisher

    from omniload import run_ingest

    topic = "omniload.mqbridge.e2e"
    endpoint = {"memory": {"topic": topic, "capacity": 4096}}
    publisher = Publisher.from_config(endpoint)
    for order_id in range(5):
        publisher.send_json({"order_id": order_id, "amount": order_id * 10})

    dest = tmp_path / "warehouse.duckdb"
    info = run_ingest(
        source_uri=(
            f"memory+mqb://?topic={topic}&capacity=4096"
            "&idle_timeout_ms=1000&max_messages=100"
        ),
        dest_uri=f"duckdb:///{dest}",
        dest_table="out.orders",
        progress="log",
    )
    assert info is not None

    con = duckdb.connect(str(dest))
    rows = con.sql(
        "select order_id, amount from out.orders order by order_id"
    ).fetchall()
    distinct_row = con.sql("select count(distinct _mqb_id) from out.orders").fetchone()
    assert distinct_row is not None
    distinct_ids = distinct_row[0]
    con.close()

    assert rows == [(0, 0), (1, 10), (2, 20), (3, 30), (4, 40)]
    assert distinct_ids == 5  # _mqb_id carried through as the merge key
