from typing import Any

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

AZURITE_IMAGE = "mcr.microsoft.com/azure-storage/azurite:3.36.0"
#: Azurite rejects any `x-ms-version` newer than the release it ships with, and
#: three clients reach it with their own: the Python SDK arranging fixtures can be
#: pinned to a version Azurite knows (`AZURE_API_VERSION` in the tests), while
#: Arrow's bundled C++ SDK and adlfs (which still serves remote-database staging)
#: cannot, so the emulator is told to serve the request anyway. Repeats the image
#: default, which naming a command replaces.
#: Reachable from the host only when the in-container service binds every
#: interface, which is what the image default does.
AZURITE_HOST = "0.0.0.0"  # noqa: S104
AZURITE_COMMAND = [
    "azurite",
    "-l",
    "/data",
    "--blobHost",
    AZURITE_HOST,
    "--queueHost",
    AZURITE_HOST,
    "--tableHost",
    AZURITE_HOST,
    "--skipApiVersionCheck",
]
CLICKHOUSE_IMAGE = "docker.io/clickhouse/clickhouse-server:26.5"
COUCHBASE_IMAGE = "docker.io/couchbase:7.6.9"
CRATEDB_IMAGE = "docker.io/crate/crate:nightly"
FLOCI_IMAGE = "docker.io/floci/floci:1.5.25"
GCS_FAKE_SERVER_IMAGE = "docker.io/fsouza/fake-gcs-server:1.55.1"
KAFKA_IMAGE = "docker.io/confluentinc/cp-kafka:7.6.0"
MONGODB_IMAGE = "docker.io/mongo:8.3"
MYSQL_IMAGE = "docker.io/mariadb:12"
MSSQL_IMAGE = "mcr.microsoft.com/mssql/server:2025-CU6-ubuntu-24.04"
POSTGRESQL_IMAGE = "docker.io/postgres:18-alpine"


def _floci_endpoint(container: Any) -> str:
    return container.get_url()


def _azurite_connection_string(container: Any) -> str:
    return container.get_connection_string()


def _gcs_endpoint(container: Any) -> str:
    return container.get_endpoint_url()


_REMOTE_FILESYSTEM_SERVICES: dict[str, DockerService] | None = None


def get_remote_filesystem_services() -> dict[str, DockerService]:
    """Build the emulator registry only for tests that can use it."""
    global _REMOTE_FILESYSTEM_SERVICES
    if _REMOTE_FILESYSTEM_SERVICES is None:
        from testcontainers.azurite import AzuriteContainer

        from tests.dlt_filesystem.gcs import GCSFakeServerContainer
        from tests.util.container.impl.floci import FlociContainer

        _REMOTE_FILESYSTEM_SERVICES = {
            "s3": DockerService(
                "remote-filesystem-s3",
                lambda: FlociContainer(image=FLOCI_IMAGE),
                connection_getter=_floci_endpoint,
            ),
            "azure": DockerService(
                "remote-filesystem-azure",
                lambda: AzuriteContainer(image=AZURITE_IMAGE).with_command(
                    AZURITE_COMMAND
                ),
                connection_getter=_azurite_connection_string,
            ),
            "gcs": DockerService(
                "remote-filesystem-gcs",
                lambda: GCSFakeServerContainer(image=GCS_FAKE_SERVER_IMAGE),
                connection_getter=_gcs_endpoint,
            ),
        }
    return _REMOTE_FILESYSTEM_SERVICES


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
