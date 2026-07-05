"""Integration tests for the mq-bridge source over a real MQTT broker (Eclipse Mosquitto).

Like ``test_mqbridge_kafka.py`` these live outside ``tests/warehouse``, so the module is marked
``integration`` explicitly. They complement the brokerless unit/e2e tests in
``test_mqbridge_brokerless.py`` by exercising the real MQTT transport and — most importantly — the
deferred-ack guarantee (offset/PUBACK withheld until the load durably commits).

MQTT has an ordering constraint Kafka does not: a broker only queues messages for a subscriber
that already has a *durable session* (``clean_session=false`` + a stable ``client_id``). Messages
published before that session's first subscribe are lost. So every test here first opens the
consumer once to register the session, then publishes, then ingests — mirroring how a real
deployment leaves a long-lived consumer subscribed. Mosquitto is configured for persistence (see
``mosquitto.conf``) so those messages survive while the consumer is offline between runs.

Only DuckDB is exercised as a destination: the source→every-destination matrix is already covered
by the Kafka test, and the value added here is the MQTT transport and its delivery semantics.
"""

import time
from collections.abc import Iterator
from pathlib import Path

import duckdb
import pytest

from tests.util import invoke_ingest_command

# Marked explicitly (not auto-marked by path) because this module lives outside tests/warehouse.
pytestmark = pytest.mark.integration

# mq-bridge is a core dependency, but guard against a broken/partial install of the native wheel.
pytest.importorskip("mq_bridge")

MOSQUITTO_IMAGE = "eclipse-mosquitto:2.0.22"
MOSQUITTO_CONF = Path(__file__).parent / "mosquitto.conf"

ROWS = [{"order_id": i, "amount": i * 10} for i in range(5)]
EXPECTED = [(r["order_id"], r["amount"]) for r in ROWS]


@pytest.fixture(scope="session")
def mosquitto() -> Iterator[str]:
    """Session-wide Mosquitto broker; yields its ``host:port`` address.

    Self-contained (a plain testcontainers container on a Docker-assigned port) rather than a
    shared ``DockerService``: nothing else needs an MQTT broker, and a random host port keeps it
    parallel-safe across xdist workers without lock-file coordination.
    """
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.waiting_utils import wait_for_logs

    container = (
        DockerContainer(MOSQUITTO_IMAGE)
        .with_exposed_ports(1883)
        .with_volume_mapping(
            str(MOSQUITTO_CONF), "/mosquitto/config/mosquitto.conf", "ro"
        )
    )
    container.start()
    try:
        wait_for_logs(container, r"mosquitto version .* running", timeout=60)
        host = container.get_container_host_ip()
        port = container.get_exposed_port(1883)
        yield f"{host}:{port}"
    finally:
        container.stop()


def _source_uri(address: str, topic: str) -> str:
    # A stable client_id + clean_session=false makes the subscription durable, so messages
    # published while the consumer is offline between runs are queued by the broker and drained
    # on the next run. The short idle_timeout keeps the bounded drain snappy once caught up.
    return (
        f"mqtt+mqb://{address}?client_id={topic}&clean_session=false&qos=1"
        f"&idle_timeout_ms=2000&max_messages=100"
    )


def _establish_session(source_uri: str, topic: str) -> None:
    """Subscribe once so the broker starts queuing for this durable session, then disconnect.

    Nothing is published yet, so this drains nothing; its only job is to register the
    ``clean_session=false`` session before the first publish (the MQTT ordering constraint).
    """
    from mq_bridge import Consumer

    from omniload.source.mqbridge.adapter import endpoint_from_uri

    _, config = endpoint_from_uri(source_uri, topic)
    consumer = Consumer.from_config(config)
    try:
        consumer.poll_batch(max=1, timeout_ms=1500)
    finally:
        consumer.close()


def _publisher(address: str, topic: str):
    """A warmed-up QoS-1 publisher bound to ``topic``.

    A freshly connected publisher's first QoS-1 PUBLISH often races the CONNACK and the broker
    never confirms it, so we warm the connection before handing it back and retry each send in
    ``_publish``.
    """
    from mq_bridge import Publisher

    publisher = Publisher.from_config(
        {
            "mqtt": {
                "url": f"tcp://{address}",
                "topic": topic,
                "qos": 1,
                "client_id": f"pub-{topic}",
            }
        }
    )
    time.sleep(1.0)  # let the connection settle so the first PUBACK isn't dropped
    return publisher


def _publish(publisher, rows) -> None:
    for row in rows:
        for _attempt in range(8):
            try:
                publisher.send_json(row)
                break
            except RuntimeError:
                # PUBACK not yet confirmed (connection still settling); back off and retry.
                time.sleep(0.4)
        else:
            raise AssertionError(f"failed to publish {row!r} after retries")
    # Give the broker a moment to persist before the consumer polls.
    time.sleep(1.0)


def _rows(db_path: Path, topic: str):
    con = duckdb.connect(str(db_path))
    try:
        out = con.sql(
            f'select order_id, amount from "{topic}".output order by order_id asc'
        ).fetchall()
    finally:
        con.close()
    return [tuple(r) for r in out]


def test_mqbridge_mqtt_to_duckdb(mosquitto, topic, tmp_path):
    """Consume real MQTT messages through mq-bridge into DuckDB, exactly once."""
    source_uri = _source_uri(mosquitto, topic)
    # DuckDB catalog == db-file stem, dataset == topic; keep them distinct or the binder errors.
    db_path = tmp_path / "out.duckdb"
    dest_uri = f"duckdb:///{db_path}"

    _establish_session(source_uri, topic)
    publisher = _publisher(mosquitto, topic)
    _publish(publisher, ROWS)

    def run():
        res = invoke_ingest_command(
            source_uri, topic, dest_uri, f"{topic}.output", print_output=False
        )
        assert res.exit_code == 0

    run()
    assert _rows(db_path, topic) == EXPECTED

    # Re-running acks nothing new to consume and merges on _mqb_id: still exactly five.
    run()
    assert _rows(db_path, topic) == EXPECTED

    # A newly published message (while the durable session was offline) is picked up next run.
    _publish(publisher, [{"order_id": 5, "amount": 50}])
    run()
    assert _rows(db_path, topic) == EXPECTED + [(5, 50)]


def test_mqbridge_mqtt_failed_load_redelivers_without_loss_or_duplication(
    mosquitto, topic, tmp_path
):
    """The offset (PUBACK) is committed only after a successful load.

    Run 1 consumes the backlog but its load fails (duckdb path is a directory), so the source is
    released *without* acking. Run 2 must therefore see the same messages redelivered and land
    them exactly once — proving the deferred ack, not just that earlier data persisted.
    """
    source_uri = _source_uri(mosquitto, topic)

    _establish_session(source_uri, topic)
    publisher = _publisher(mosquitto, topic)
    _publish(publisher, ROWS)

    # Run 1: a directory where a database file is expected makes the load step fail *after* the
    # broker batch has already been polled.
    broken = tmp_path / "broken.duckdb"
    broken.mkdir()
    failed = invoke_ingest_command(
        source_uri, topic, f"duckdb:///{broken}", f"{topic}.output", print_output=False
    )
    assert failed.exit_code != 0

    # Run 2: a healthy destination. If run 1 had wrongly acked, this would load nothing;
    # redelivery means it loads all five.
    good = tmp_path / "good.duckdb"
    ok = invoke_ingest_command(
        source_uri, topic, f"duckdb:///{good}", f"{topic}.output", print_output=False
    )
    assert ok.exit_code == 0

    # All five present, none duplicated (merge on _mqb_id) — exactly once after redelivery.
    assert _rows(good, topic) == EXPECTED
