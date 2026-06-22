import time
from abc import ABCMeta, abstractmethod
from pathlib import Path
from threading import Thread
from typing import Optional

from testcontainers.core.container import DockerContainer
from testcontainers.core.generic import DbContainer
from testcontainers.kafka import KafkaContainer

from tests.container.impl.couchbase import CouchbaseContainer


class AbstractService(metaclass=ABCMeta):
    """The contract for a service."""

    def __init__(self) -> None:
        self.container = None

    @abstractmethod
    def start(self) -> str:
        raise NotImplementedError("Need to implement abstract method")

    @abstractmethod
    def start_fully(self) -> str:
        raise NotImplementedError("Need to implement abstract method")

    @abstractmethod
    def stop(self):
        raise NotImplementedError("Need to implement abstract method")

    @abstractmethod
    def stop_fully(self):
        raise NotImplementedError("Need to implement abstract method")


class DockerService(AbstractService):
    """
    Wrap Docker container with locking, for parallel execution.

    Making session-scoped fixtures execute only once by using a filesystem lock.
    https://pytest-xdist.readthedocs.io/en/stable/how-to.html#making-session-scoped-fixtures-execute-only-once
    """

    def __init__(
        self,
        id: str,
        container: DockerContainer,
        connection_suffix: str = "",
        lock_dir: Optional[Path] = None,
        shutdown: Optional[bool] = False,
    ) -> None:
        super().__init__()
        self.id = id
        self.container = container
        self.connection_suffix = connection_suffix
        self.lock_dir = lock_dir
        self.register_for_shutdown = shutdown

    def start(self) -> str:
        """Wait for the controller to spin up the container."""
        attempts = 0
        while self.lock_dir is None or not self._conn_url_file.exists():
            time.sleep(0.5)
            attempts += 1
            if attempts > 40:
                raise Exception("Failed to start container after bunch of attempts")
        return self._conn_url_file.read_text()

    def stop(self):
        """Container lifecycle is managed by the controller."""
        pass

    def start_fully(self):
        if self._is_starting():
            return
        self._signal_starting()
        if self.container is None:
            raise RuntimeError("Container is not initialized")
        self.container.start()
        conn_url = self.get_connection_url()
        self.write_conn_url(conn_url)
        self._signal_started()

    def stop_fully(self):
        if self.container is not None:
            self.container.stop()

    def start_background(self):
        Thread(target=self.start_fully, daemon=True).start()
        return self

    def get_connection_url(self):
        if self.container is None:
            raise RuntimeError("Container is not initialized")
        if isinstance(self.container, KafkaContainer):
            return self.container.get_bootstrap_server()
        elif isinstance(self.container, CouchbaseContainer):
            return self.container.get_connection_url() + self.connection_suffix
        elif isinstance(self.container, DbContainer):
            return self.container.get_connection_url() + self.connection_suffix
        raise ValueError("Unable to get connection url")

    def write_conn_url(self, url: str):
        self._conn_url_file.write_text(url)

    def add_shutdown_signal(self):
        self._shutdown_file.touch()
        return self

    def _signal_starting(self):
        self._starter_file.touch()

    def _signal_started(self):
        self._starter_file.unlink(missing_ok=True)
        if self.register_for_shutdown:
            self.add_shutdown_signal()

    def _is_starting(self):
        return self._starter_file.exists()

    @property
    def _conn_url_file(self) -> Path:
        return Path(f"{self.lock_dir}/{self.id}")

    @property
    def _starter_file(self) -> Path:
        return Path(f"{self.lock_dir}/{self.id}.starting")

    @property
    def _shutdown_file(self) -> Path:
        return Path(f"{self.lock_dir}/{self.container_id}.shutdown")

    @property
    def container_id(self) -> str:
        if self.container is None:
            raise RuntimeError("Container is not initialized")
        if self.container._container is None:
            raise RuntimeError("Container was not started")
        return self.container._container.id


class StartSemaphore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def starting(self):
        self.path.touch()

    def started(self):
        self.path.unlink(missing_ok=True)

    @property
    def is_starting(self):
        return self.path.exists()
