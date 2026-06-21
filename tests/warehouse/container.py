import os
import shutil
import sys
import tempfile
import time
from pathlib import Path

from testcontainers.clickhouse import ClickHouseContainer
from testcontainers.core.container import DockerContainer
from testcontainers.mysql import MySqlContainer
from testcontainers.postgres import PostgresContainer


class DockerImage:
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


class ClickhouseDockerImage(DockerImage):
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


class CouchbaseContainer(DockerContainer):
    """Custom Couchbase container for testing."""

    def __init__(self, image: str = "couchbase:community", **kwargs):
        super().__init__(image, **kwargs)
        # Use 1:1 port mapping (requires local Couchbase to be stopped)
        # This allows SDK to connect without alternate addresses
        self.with_bind_ports(8091, 8091)
        self.with_bind_ports(8092, 8092)
        self.with_bind_ports(8093, 8093)
        self.with_bind_ports(8094, 8094)
        self.with_bind_ports(8095, 8095)
        self.with_bind_ports(8096, 8096)
        self.with_bind_ports(11210, 11210)
        self.username = "Administrator"
        self.password = "password"
        self.bucket_name = "test_bucket"
        self.scope_name = "_default"
        self.collection_name = "_default"

    def start(self):
        """Start container and initialize Couchbase."""
        super().start()

        # Wait for Couchbase web console to be ready
        self._wait_for_couchbase()

        # Initialize cluster
        self._initialize_cluster()

        # Create bucket
        self._create_bucket()

        # Wait for bucket to be ready
        time.sleep(10)

        # Create primary index for N1QL queries
        self._create_primary_index()

        return self

    def _wait_for_couchbase(self):
        """Wait for Couchbase to be ready."""
        import requests

        port = self.get_exposed_port(8091)
        base_url = f"http://{self.get_container_host_ip()}:{port}"

        max_attempts = 30
        for i in range(max_attempts):
            try:
                response = requests.get(f"{base_url}/pools", timeout=2)
                if response.status_code == 200:
                    return
            except Exception:
                pass
            time.sleep(2)

        raise Exception(f"Couchbase did not become ready after {max_attempts} attempts")

    def _initialize_cluster(self):
        """Initialize Couchbase cluster using couchbase-cli."""
        # Use couchbase-cli inside the container for proper setup
        self.exec(
            f"couchbase-cli cluster-init -c 127.0.0.1 "
            f"--cluster-username {self.username} "
            f"--cluster-password {self.password} "
            f"--services data,index,query "
            f"--cluster-ramsize 256 "
            f"--cluster-index-ramsize 256"
        )

        # Wait for cluster to be initialized
        time.sleep(5)

    def _setup_alternate_addresses(self):
        """Setup alternate addresses for SDK bootstrap."""
        import requests

        host = self.get_container_host_ip()
        port = self.get_exposed_port(8091)

        # Configure alternate addresses so SDK can connect from outside container
        requests.post(
            f"http://{host}:{port}/node/controller/setupAlternateAddresses/external",
            auth=(self.username, self.password),
            json={
                "hostname": host,
                "mgmt": int(self.get_exposed_port(8091)),
                "kv": int(self.get_exposed_port(11210)),
                "n1ql": int(self.get_exposed_port(8093)),
                "capi": int(self.get_exposed_port(8092)),
                "fts": int(self.get_exposed_port(8094)),
                "cbas": int(self.get_exposed_port(8095)),
                "eventingAdminPort": int(self.get_exposed_port(8096)),
            },
        )
        time.sleep(2)

    def _create_bucket(self):
        """Create a test bucket using couchbase-cli."""
        self.exec(
            f"couchbase-cli bucket-create -c 127.0.0.1 "
            f"-u {self.username} -p {self.password} "
            f"--bucket {self.bucket_name} "
            f"--bucket-type couchbase "
            f"--bucket-ramsize 100 "
            f"--storage-backend couchstore "  # Use couchstore for community edition
            f"--bucket-replica 0"  # No replicas for single node
        )

        # Wait for bucket to be ready and healthy
        self._wait_for_bucket_ready()

    def _wait_for_bucket_ready(self):
        """Wait for bucket to be healthy and ready."""
        import requests

        host = self.get_container_host_ip()
        port = self.get_exposed_port(8091)

        for i in range(30):
            try:
                response = requests.get(
                    f"http://{host}:{port}/pools/default/buckets/{self.bucket_name}",
                    auth=(self.username, self.password),
                    timeout=2,
                )
                if response.status_code == 200:
                    bucket_info = response.json()
                    # Check if bucket is healthy and all nodes are ready
                    if bucket_info.get("nodes") and all(
                        node.get("status") == "healthy" for node in bucket_info["nodes"]
                    ):
                        time.sleep(5)  # Extra wait for full readiness
                        return
            except Exception:
                pass
            time.sleep(2)

        raise Exception(
            f"Bucket '{self.bucket_name}' did not become ready after waiting"
        )

    def _create_primary_index(self):
        """Create primary index for N1QL queries using cbq CLI."""
        # Use cbq command-line tool to create the primary index
        # Note: We ignore errors if the index already exists
        query = f"CREATE PRIMARY INDEX ON `{self.bucket_name}`.`{self.scope_name}`.`{self.collection_name}`"
        try:
            self.exec(
                f"cbq -u {self.username} -p {self.password} -engine=http://127.0.0.1:8091/ "
                f'-script="{query}"'
            )
            time.sleep(2)
        except Exception:
            # Index may already exist, ignore error
            pass

    def get_connection_string(self) -> str:
        """Get Couchbase connection string."""
        # With 1:1 port mapping, use localhost
        return "couchbase://localhost"

    def get_connection_url(self) -> str:
        """Get connection URL with credentials."""
        # With 1:1 port mapping, use localhost
        return f"couchbase://{self.username}:{self.password}@localhost"

    def insert_documents(self, documents: list):
        """Insert documents using Couchbase Python SDK from test machine."""
        from datetime import timedelta

        from couchbase.auth import PasswordAuthenticator
        from couchbase.cluster import Cluster
        from couchbase.options import ClusterOptions

        # Connect using SDK (from test machine to container)
        auth = PasswordAuthenticator(self.username, self.password)
        cluster = Cluster(self.get_connection_string(), ClusterOptions(auth))
        cluster.wait_until_ready(timedelta(seconds=30))

        # Get bucket and collection
        bucket = cluster.bucket(self.bucket_name)
        collection = bucket.scope(self.scope_name).collection(self.collection_name)

        # Insert documents
        for doc in documents:
            doc_id = str(doc.get("id", doc.get("_id", f"doc_{hash(str(doc))}")))
            collection.upsert(doc_id, doc)

        time.sleep(2)


POSTGRESQL_IMAGE = "docker.io/postgres:18-alpine"
MYSQL_IMAGE = "docker.io/mariadb:12"
MSSQL_IMAGE = "mcr.microsoft.com/mssql/server:2025-CU6-ubuntu-24.04"
CLICKHOUSE_IMAGE = "docker.io/clickhouse/clickhouse-server:26.5"
MONGODB_IMAGE = "docker.io/mongo:8.3"
COUCHBASE_IMAGE = "docker.io/couchbase:7.6.9"
pgDocker = DockerImage(
    "postgres", lambda: PostgresContainer(POSTGRESQL_IMAGE, driver=None).start()
)
clickHouseDocker = ClickhouseDockerImage(
    "clickhouse", lambda: ClickHouseContainer(CLICKHOUSE_IMAGE).start()
)
mysqlDocker = DockerImage(
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
            "sqlserver": DockerImage(
                "sqlserver",
                lambda: SqlServerContainer(MSSQL_IMAGE, dialect="mssql").start(),
                "?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=Yes",
            )
        }
    )
