from testcontainers.clickhouse import ClickHouseContainer
from testcontainers.kafka import KafkaContainer
from testcontainers.mongodb import MongoDbContainer
from testcontainers.mysql import MySqlContainer
from testcontainers.postgres import PostgresContainer

from tests.container.impl.clickhouse import ClickhouseService
from tests.container.impl.couchbase import CouchbaseContainer
from tests.container.impl.duckdb import EphemeralDuckDb
from tests.container.impl.floci import FlociContainer
from tests.container.impl.mssql import get_mssql_service
from tests.container.model import (
    DockerService,
    ServiceRegistry,
)
from tests.container.settings import (
    CLICKHOUSE_IMAGE,
    COUCHBASE_IMAGE,
    FLOCI_IMAGE,
    KAFKA_IMAGE,
    MONGODB_IMAGE,
    MYSQL_IMAGE,
    POSTGRESQL_IMAGE,
)

# TODO: MongoDB, Couchbase, Kafka, Floci
registry = ServiceRegistry(
    clickhouse=ClickhouseService(
        "clickhouse", ClickHouseContainer(CLICKHOUSE_IMAGE)
    ),
    couchbase=DockerService(
        "couchbase", CouchbaseContainer(COUCHBASE_IMAGE)
    ),
    duckdb_source=EphemeralDuckDb(),
    duckdb_destination=EphemeralDuckDb(),
    floci=DockerService("floci", FlociContainer(FLOCI_IMAGE)),
    kafka=None,
    mongodb=DockerService("mongodb", MongoDbContainer(MONGODB_IMAGE)),
    mssql=get_mssql_service(),
    mysql=DockerService(
        "mysql",
        MySqlContainer(
            image=MYSQL_IMAGE, dialect="pymysql", username="root"
        ),
    ),
    postgresql=DockerService(
        "postgres", PostgresContainer(POSTGRESQL_IMAGE, driver=None)
    ),
)
