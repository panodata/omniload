from testcontainers.community.clickhouse import ClickHouseContainer
from testcontainers.community.cratedb import CrateDBContainer
from testcontainers.community.mongodb import MongoDbContainer
from testcontainers.community.mysql import MySqlContainer
from testcontainers.community.postgres import PostgresContainer

from tests.util.container.impl.clickhouse import ClickhouseService
from tests.util.container.impl.duckdb import EphemeralDuckDb
from tests.util.container.impl.mssql import get_mssql_service
from tests.util.container.model import DockerService
from tests.warehouse.model import ServiceRegistry

CLICKHOUSE_IMAGE = "docker.io/clickhouse/clickhouse-server:26.5"
COUCHBASE_IMAGE = "docker.io/couchbase:7.6.9"
CRATEDB_IMAGE = "docker.io/crate/crate:nightly"
FLOCI_IMAGE = "docker.io/floci/floci:1.5.25"
KAFKA_IMAGE = "docker.io/confluentinc/cp-kafka:7.6.0"
MONGODB_IMAGE = "docker.io/mongo:8.3"
MYSQL_IMAGE = "docker.io/mariadb:12"
MSSQL_IMAGE = "mcr.microsoft.com/mssql/server:2025-CU6-ubuntu-24.04"
POSTGRESQL_IMAGE = "docker.io/postgres:18-alpine"

registry = ServiceRegistry(
    clickhouse=ClickhouseService(
        "clickhouse", lambda: ClickHouseContainer(CLICKHOUSE_IMAGE)
    ),
    cratedb=DockerService(
        "cratedb",
        lambda: CrateDBContainer(
            CRATEDB_IMAGE,
            cmd_opts=[
                # The test suite creates lots of tables which are currently not purged.
                # This leads to the following errors on CrateDB when running the whole suite:
                # > [...] this action would add [4] total shards, but this
                # > cluster currently has [1000]/[1000] maximum shards open
                ("cluster.max_shards_per_node", "2000"),
            ],
        ),
    ),
    duckdb_source=EphemeralDuckDb(),
    duckdb_destination=EphemeralDuckDb(),
    mongodb=DockerService("mongodb", lambda: MongoDbContainer(MONGODB_IMAGE)),
    mssql=get_mssql_service(MSSQL_IMAGE),
    mysql=DockerService(
        "mysql",
        lambda: MySqlContainer(image=MYSQL_IMAGE, dialect="pymysql", username="root"),
    ),
    postgresql=DockerService(
        "postgres", lambda: PostgresContainer(POSTGRESQL_IMAGE, driver=None)
    ),
)
