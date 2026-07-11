"""Integration tests for the mq-bridge source over a real Kafka broker.

These live outside the ``tests/warehouse`` subtree, so the module is marked ``integration``
explicitly (Docker/testcontainers). They complement the brokerless unit/e2e tests in
``tests/stream/test_mqbridge_brokerless.py``: here we exercise the real transport, the ``_mqb_id`` derived
from a Kafka ``partition:offset``, and — most importantly — the deferred-ack guarantee, none of
which the in-memory transport covers.

Kafka is the cheapest real broker to test because the container fixtures already exist (see
``tests/warehouse/db/test_kafka.py``); the same ``DockerService(id="kafka", lock_dir=...)`` is
shared, so this file does not spin up a second container.
"""

import json
from concurrent.futures import ThreadPoolExecutor

import duckdb
import pytest
from confluent_kafka import Producer

from tests.util import invoke_ingest_command
from tests.util.db import get_query_result
from tests.warehouse.settings import DESTINATIONS

# Marked explicitly (not auto-marked by path) because this module lives outside tests/warehouse.
pytestmark = pytest.mark.integration

# mq-bridge is a core dependency, but guard against a broken/partial install of the native wheel.
pytest.importorskip("mq_bridge")

ROWS = [{"order_id": i, "amount": i * 10} for i in range(5)]
EXPECTED = [(r["order_id"], r["amount"]) for r in ROWS]


def _produce(address: str, topic: str, rows) -> None:
    producer = Producer({"bootstrap.servers": address})
    for row in rows:
        producer.produce(topic, json.dumps(row))
    producer.flush()


def _source_uri(address: str, topic: str) -> str:
    # group_id == topic makes the consumer durable and isolated per test; the short
    # idle_timeout keeps the bounded drain snappy once the backlog is consumed.
    return (
        f"kafka+mqb://{address}?group_id={topic}&idle_timeout_ms=2000&max_messages=100"
    )


@pytest.mark.parametrize(
    "dest", list(DESTINATIONS.values()), ids=list(DESTINATIONS.keys())
)
def test_mqbridge_kafka_to_db(kafka, dest, topic):
    """Consume real Kafka messages through mq-bridge into every destination, exactly once."""
    with ThreadPoolExecutor() as executor:
        dest_uri = executor.submit(dest.start).result()

    if dest_uri.startswith("cratedb://"):
        pytest.skip(
            "Fails on CrateDB with `DestinationSchemaTampered`, see "
            "https://github.com/crate/dlt-cratedb/issues/14"
        )

    _produce(kafka, topic, ROWS)

    def run():
        res = invoke_ingest_command(
            _source_uri(kafka, topic), topic, dest_uri, f"{topic}.output"
        )
        assert res.exit_code == 0

    def rows():
        res = get_query_result(
            dest_uri,
            f"select order_id, amount from {topic}.output order by order_id asc",
        )
        return [tuple(r) for r in res]

    run()
    assert rows() == EXPECTED

    # Re-running commits no new offsets to consume and merges on _mqb_id: still exactly five.
    run()
    assert rows() == EXPECTED

    # A newly produced message is picked up on the next run.
    _produce(kafka, topic, [{"order_id": 5, "amount": 50}])
    run()
    assert rows() == EXPECTED + [(5, 50)]


def test_mqbridge_failed_load_redelivers_without_loss_or_duplication(
    kafka, topic, tmp_path
):
    """The offset is committed only after a successful load.

    Run 1 consumes the backlog but its load fails (duckdb path is a directory), so the source is
    released *without* committing. Run 2 must therefore see the same messages redelivered and land
    them exactly once — proving the deferred ack, not just that earlier data persisted.
    """
    _produce(kafka, topic, ROWS)
    source_uri = _source_uri(kafka, topic)

    # Run 1: a directory where a database file is expected makes the load step fail *after* the
    # broker batch has already been polled.
    broken = tmp_path / "broken.duckdb"
    broken.mkdir()
    failed = invoke_ingest_command(
        source_uri, topic, f"duckdb:///{broken}", f"{topic}.output", print_output=False
    )
    assert failed.exit_code != 0

    # Run 2: a healthy destination. If run 1 had wrongly committed the offset, this would load
    # nothing; redelivery means it loads all five.
    good = tmp_path / "good.duckdb"
    ok = invoke_ingest_command(
        source_uri, topic, f"duckdb:///{good}", f"{topic}.output"
    )
    assert ok.exit_code == 0

    con = duckdb.connect(str(good))
    loaded = con.sql(
        f"select order_id, amount from {topic}.output order by order_id asc"
    ).fetchall()
    con.close()
    # All five present, none duplicated (merge on _mqb_id) — exactly once after redelivery.
    assert [tuple(r) for r in loaded] == EXPECTED
