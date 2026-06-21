import os
import shutil
import sys
import tempfile
import time
from pathlib import Path

from testcontainers.clickhouse import ClickHouseContainer
from testcontainers.mysql import MySqlContainer
from testcontainers.postgres import PostgresContainer

from tests.settings import CLICKHOUSE_IMAGE, MSSQL_IMAGE, MYSQL_IMAGE, POSTGRESQL_IMAGE


class DockerService:
    def __init__(self, id: str, container_creator, connection_suffix: str = "") -> None:
        self.id = id
        self.container_creator = container_creator
        self.connection_suffix = connection_suffix
        self.container_lock_dir = None
        self.container = None

    def start(self) -> str:
        file_path = f"{self.container_lock_dir}/{self.id}"
        attempts = 0
        while self.container_lock_dir is None or not os.path.exists(file_path):
            time.sleep(1)
            attempts += 1
            if attempts > 20:
                raise Exception("Failed to start container after bunch of attempts")

        with open(file_path, "r") as f:
            res = f.read()
            return res

    def start_fully(self) -> str:
        self.container = self.container_creator()
        if self.container is None:
            raise ValueError("Container is not initialized.")

        conn_url = self.container.get_connection_url() + self.connection_suffix

        with open(f"{self.container_lock_dir}/{self.id}", "w") as f:
            f.write(conn_url)

        return conn_url

    def stop(self):
        pass

    def stop_fully(self):
        if self.container is not None:
            self.container.stop()


class ClickhouseService(DockerService):
    def start_fully(self) -> str:
        self.container = self.container_creator()
        if self.container is None:
            raise ValueError("Container is not initialized.")

        port = self.container.get_exposed_port(8123)
        conn_url = (
            self.container.get_connection_url().replace(
                "clickhouse://", "clickhouse+native://"
            )
            + f"?http_port={port}&secure=0"
        )
        # raise ValueError(conn_url)
        with open(f"{self.container_lock_dir}/{self.id}", "w") as f:
            f.write(conn_url)

        return conn_url


class EphemeralDuckDb:
    def __init__(self):
        self.tmpdir = Path(tempfile.mkdtemp())

    def start(self) -> str:
        abs_path = self.tmpdir / "duckdb.db"
        return f"duckdb:///{abs_path}"

    def start_fully(self) -> str:  # type: ignore
        pass

    def stop(self):
        pass

    def stop_fully(self):
        shutil.rmtree(self.tmpdir)


pgDocker = DockerService(
    "postgres", lambda: PostgresContainer(POSTGRESQL_IMAGE, driver=None).start()
)
clickHouseDocker = ClickhouseService(
    "clickhouse", lambda: ClickHouseContainer(CLICKHOUSE_IMAGE).start()
)
mysqlDocker = DockerService(
    "mysql",
    lambda: MySqlContainer(
        image=MYSQL_IMAGE, dialect="pymysql", username="root"
    ).start(),
)


SOURCES = {
    "postgres": pgDocker,
    "duckdb": EphemeralDuckDb(),
    "mysql8": mysqlDocker,
}
DESTINATIONS = {
    "postgres": pgDocker,
    "duckdb": EphemeralDuckDb(),
    "clickhouse+native": clickHouseDocker,
}

if sys.platform == "linux":
    # [unixODBC][Driver Manager] Can't open lib 'ODBC Driver 18 for SQL Server' : file not found (0) (SQLDriverConnect)
    from testcontainers.mssql import SqlServerContainer

    SOURCES.update(
        {
            "sqlserver": DockerService(
                "sqlserver",
                lambda: SqlServerContainer(MSSQL_IMAGE, dialect="mssql").start(),
                "?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=Yes",
            )
        }
    )
