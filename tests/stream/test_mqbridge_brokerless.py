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


def test_endpoint_from_uri_ibmmq_translates_conn_name_and_requires_qm_channel():
    transport, config = endpoint_from_uri(
        "ibmmq+mqb://mqhost:1414"
        "?queue_manager=QM1&channel=DEV.APP.SVRCONN&wait_timeout_ms=5000",
        "DEV.QUEUE.1",
    )
    assert transport == "ibmmq"
    ibmmq = config["ibmmq"]
    assert ibmmq["url"] == "mqhost(1414)"  # host:port -> host(port)
    assert ibmmq["queue_manager"] == "QM1"
    assert ibmmq["channel"] == "DEV.APP.SVRCONN"
    assert ibmmq["queue"] == "DEV.QUEUE.1"  # --source-table fills the queue
    assert ibmmq["wait_timeout_ms"] == 5000  # int field coerced


def test_endpoint_from_uri_ibmmq_failover_list_and_topic_mode():
    _, config = endpoint_from_uri(
        "ibmmq+mqb://h1:1414,h2:1414?queue_manager=QM1&channel=C&topic=news",
        "",
    )
    ibmmq = config["ibmmq"]
    assert (
        ibmmq["url"] == "h1(1414),h2(1414)"
    )  # comma-separated failover list translated
    assert (
        ibmmq["topic"] == "news"
    )  # explicit ?topic= switches to pub/sub subscriber mode
    assert "queue" not in ibmmq  # no --source-table, so no queue slot


def test_endpoint_from_uri_ibmmq_bare_host_and_empty_segment():
    # A host with no port is kept verbatim; an empty segment (trailing comma) is skipped.
    _, config = endpoint_from_uri(
        "ibmmq+mqb://mqhost,?queue_manager=QM1&channel=C", "Q"
    )
    assert config["ibmmq"]["url"] == "mqhost"


def test_endpoint_from_uri_rejects_non_integer_query_param():
    # An int-typed field that can't be parsed fails loudly rather than being forwarded as junk.
    with pytest.raises(ValueError, match="integer"):
        endpoint_from_uri("kafka+mqb://b:9092?prefetch_count=notanumber", "t")


def test_endpoint_from_uri_rejects_plain_scheme():
    with pytest.raises(ValueError):
        endpoint_from_uri("kafka://localhost:9092", "t")


def test_endpoint_from_uri_rejects_unknown_transport():
    with pytest.raises(ValueError):
        endpoint_from_uri("frobnicate+mqb://host", "t")


def test_dlt_source_rejects_incremental_key():
    # mq-bridge manages incrementality itself; run_ingest forwards the user's original request as
    # ``requested_incremental_key`` (it nulls ``incremental_key`` for handles_incrementality
    # sources before calling dlt_source), so the rejection must key off that.
    from omniload.source.mqbridge.api import MqBridgeSource

    with pytest.raises(ValueError, match="incremental"):
        MqBridgeSource().dlt_source(
            "memory+mqb://?topic=t", "t", requested_incremental_key="ts"
        )


def test_dlt_source_rejects_primary_key():
    # A user-supplied --primary-key would override the _mqb_id merge key and break dedup.
    from omniload.source.mqbridge.api import MqBridgeSource

    with pytest.raises(ValueError, match="primary-key"):
        MqBridgeSource().dlt_source(
            "memory+mqb://?topic=t", "t", requested_primary_key=["order_id"]
        )


def test_dlt_source_rejects_unknown_format():
    from omniload.source.mqbridge.api import MqBridgeSource

    with pytest.raises(ValueError, match="format"):
        MqBridgeSource().dlt_source("memory+mqb://?topic=t&format=xml", "t")


class _FakeMessage:
    def __init__(self, id_: str, value: int) -> None:
        self._id = id_
        self._value = value

    @property
    def id(self) -> str:
        return self._id

    @property
    def metadata(self) -> dict:
        return {}

    def json(self) -> dict:
        return {"value": self._value}

    def text(self) -> str:
        return str(self._value)


class _ScalarMessage(_FakeMessage):
    """A message whose JSON payload is a scalar (not a dict), to exercise the value-wrap path."""

    def json(self):
        return self._value


class _FakeConsumer:
    """Hands out preset batches via poll_batch and records which tokens get acked."""

    instance = None  # returned by from_config, so dlt_source picks it up

    def __init__(self, batches):
        self._batches = batches
        self._i = 0
        self.acked: list = []
        self.closed = False

    @classmethod
    def from_config(cls, _config):
        return cls.instance

    def poll_batch(self, max, timeout_ms):
        if self._i >= len(self._batches):
            return [], None
        batch = self._batches[self._i]
        token = self._i
        self._i += 1
        return list(batch), token

    def ack(self, token) -> None:
        self.acked.append(token)

    def close(self) -> None:
        self.closed = True


def _wire_fake_consumer(monkeypatch, batches):
    import mq_bridge

    from omniload.source.mqbridge.api import MqBridgeSource

    fake = _FakeConsumer(batches)
    _FakeConsumer.instance = fake
    monkeypatch.setattr(mq_bridge, "Consumer", _FakeConsumer)
    return MqBridgeSource(), fake


def test_post_load_acks_every_fully_drained_batch(monkeypatch):
    # Full drain: both batches make it into the load package, so post_load acks both tokens.
    src, fake = _wire_fake_consumer(
        monkeypatch,
        [[_FakeMessage("a", 1), _FakeMessage("b", 2)], [_FakeMessage("c", 3)]],
    )
    resource = src.dlt_source("memory+mqb://?topic=t&batch_size=10", "t")

    items = list(resource)
    assert [item["_mqb_id"] for item in items] == ["a", "b", "c"]

    src.post_load()
    assert fake.acked == [0, 1]  # both batch tokens acked after the load
    assert fake.closed


def test_yield_limit_truncation_leaves_partial_batch_unacked(monkeypatch):
    # A --yield-limit stops iteration mid-batch: the first batch is never fully yielded, so its
    # token is not recorded and post_load acks nothing — the batch redelivers and dedups instead
    # of silently losing the un-yielded remainder (the old bare commit() footgun).
    src, fake = _wire_fake_consumer(
        monkeypatch,
        [[_FakeMessage("a", 1), _FakeMessage("b", 2)], [_FakeMessage("c", 3)]],
    )
    resource = src.dlt_source("memory+mqb://?topic=t&batch_size=10", "t")

    first = next(iter(resource))  # pull just one of the batch's two messages
    assert first["_mqb_id"] == "a"

    src.post_load()
    assert fake.acked == []  # nothing acked: whole batch redelivers next run
    assert fake.closed


def test_release_acks_nothing_even_after_a_full_batch(monkeypatch):
    # A failure after batches were drained must not ack them: release closes without acking so the
    # broker redelivers everything.
    src, fake = _wire_fake_consumer(monkeypatch, [[_FakeMessage("a", 1)]])
    resource = src.dlt_source("memory+mqb://?topic=t&batch_size=10", "t")

    list(resource)
    assert list(src._pending_batches) == [0]  # recorded, but not yet acked

    src.release()
    assert fake.acked == []
    assert fake.closed


def test_reader_text_format_wraps_payload_under_value(monkeypatch):
    # format=text stores the raw text under a `value` column instead of decoding JSON.
    src, _ = _wire_fake_consumer(monkeypatch, [[_FakeMessage("a", 7)]])
    resource = src.dlt_source("memory+mqb://?topic=t&format=text&batch_size=10", "t")

    items = list(resource)
    assert items[0]["value"] == "7"  # text() rendering, not a decoded dict


def test_reader_non_dict_json_wrapped_under_value(monkeypatch):
    # A JSON payload that isn't an object (scalar/array) is wrapped so it still forms a row.
    src, _ = _wire_fake_consumer(monkeypatch, [[_ScalarMessage("a", 42)]])
    resource = src.dlt_source("memory+mqb://?topic=t&batch_size=10", "t")

    items = list(resource)
    assert items[0]["value"] == 42


def test_run_ingest_releases_consumer_when_the_load_fails(monkeypatch, tmp_path):
    # End-to-end guard for the item-7 fix: if consumption/load raises after the Consumer is opened
    # in dlt_source, run_ingest must release it (close, ack nothing) so the batch redelivers.
    pytest.importorskip("mq_bridge")
    import mq_bridge

    from omniload import run_ingest

    class _BoomConsumer(_FakeConsumer):
        def poll_batch(self, max, timeout_ms):
            raise RuntimeError("broker exploded mid-drain")

    fake = _BoomConsumer([])
    _BoomConsumer.instance = fake
    monkeypatch.setattr(mq_bridge, "Consumer", _BoomConsumer)

    dest = tmp_path / "warehouse.duckdb"
    with pytest.raises(Exception):
        run_ingest(
            source_uri="memory+mqb://?topic=t&idle_timeout_ms=100&max_messages=10",
            dest_uri=f"duckdb:///{dest}",
            dest_table="out.orders",
        )

    assert fake.closed  # release() ran on the failure path
    assert fake.acked == []  # nothing acked, so the broker redelivers


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
