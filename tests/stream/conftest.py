import pytest
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient
from testcontainers.kafka import KafkaContainer

from tests.util.common import get_random_string
from tests.util.container.model import DockerService
from tests.warehouse.manager import KAFKA_IMAGE


@pytest.fixture(scope="session")
def kafka_service(request, shared_directory) -> DockerService:
    """
    Session-wide Kafka container, shared with the native kafka tests via the lock dir.
    Returns the `host:port` address of the Kafka container.
    """
    return DockerService(
        id="kafka",
        container_creator=lambda: KafkaContainer(KAFKA_IMAGE),
        lock_dir=shared_directory,
        shutdown=True,
    ).start_background()


@pytest.fixture(scope="function")
def topic() -> str:
    """
    Kafka: A unique topic per test (doubles as the consumer group, so offsets never collide).
    MQTT: A unique topic per test; doubles as the durable consumer's ``client_id`` seed.
    DB: Provide random unique identifier for Kafka topics and RDBMS schemas.
    """
    return "test_" + get_random_string(5)


@pytest.fixture(scope="function")
def kafka(kafka_service, topic) -> str:
    """
    Provide a Kafka service container using a clean canvas.
    Returns the `host:port` address of the Kafka container.
    Before invoking the test case, delete all relevant topics completely.
    """
    kafka_address = kafka_service.start()
    admin = AdminClient({"bootstrap.servers": kafka_address})
    for _name, fut in admin.delete_topics([topic]).items():
        try:
            fut.result(10)
        except KafkaException as exc:
            # Topic may not exist.
            if exc.args[0].code() != KafkaError.UNKNOWN_TOPIC_OR_PART:
                raise
    return kafka_address
