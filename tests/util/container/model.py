import os
import sys
import time
from abc import ABCMeta, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Callable, Optional

from testcontainers.core.container import DockerContainer
from testcontainers.core.generic import DbContainer
from testcontainers.kafka import KafkaContainer

from tests.util.container.impl.couchbase import CouchbaseContainer


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
        container_creator: Callable[[], DockerContainer],
        connection_suffix: str = "",
        lock_dir: Optional[Path] = None,
        shutdown: Optional[bool] = False,
    ) -> None:
        super().__init__()
        self.id = id
        self.container = None
        self.container_creator = container_creator
        self.connection_suffix = connection_suffix
        self.lock_dir = lock_dir
        self.register_for_shutdown = shutdown

    def start(self) -> str:
        """Wait for the controller to spin up the container."""
        attempts = 0
        while self.lock_dir is None or not self._conn_url_file.exists():
            time.sleep(0.5)
            attempts += 1
            if attempts > 80:
                raise RuntimeError(f"Failed to start container: {self.id}")
        return self._conn_url_file.read_text()

    def stop(self):
        """Container lifecycle is managed by the controller."""
        pass

    def start_fully(self):
        if self.container is not None:
            return
        if not self._try_signal_starting():
            return
        try:
            self.container = self._start_container()
            conn_url = self.get_connection_url()
            self.write_conn_url(conn_url)
            self._signal_started()
        finally:
            self._signal_not_started()

    def _start_container(self, attempts: int = 3):
        """Build and start the container, retrying a fresh one on transient boot
        failures. Heavy images (SQL Server especially) intermittently exit between
        `docker create` and the readiness probe; because conftest boots every
        container up front, one such death would otherwise abort the whole test
        session. On every failed attempt we dump the dead container's own logs
        (the docker-API error alone never says *why* it exited) and stop it
        before retrying.
        """
        last_exc: Exception | None = None
        for attempt in range(1, attempts + 1):
            container = self.container_creator()
            if container is None:
                raise ValueError("Container is not initialized.")
            try:
                return container.start()
            except Exception as exc:
                last_exc = exc
                self._report_failed_start(container, attempt, attempts, exc)
                self._safe_stop(container)
                if attempt < attempts:
                    time.sleep(2 * attempt)
        assert last_exc is not None  # loop ran at least once
        raise last_exc

    def _report_failed_start(
        self, container, attempt: int, attempts: int, exc: Exception
    ) -> None:
        print(
            f"[testcontainers] {self.id!r} failed to start "
            f"(attempt {attempt}/{attempts}): {exc!r}",
            file=sys.stderr,
        )
        try:
            logs = container.get_logs()
        except Exception as log_exc:
            print(
                f"[testcontainers] {self.id!r} get_logs() unavailable: {log_exc!r}",
                file=sys.stderr,
            )
            return
        for stream_name, stream in zip(("stdout", "stderr"), logs):
            if not stream:
                continue
            text = (
                stream.decode("utf-8", "replace")
                if isinstance(stream, bytes)
                else str(stream)
            )
            print(
                f"[testcontainers] {self.id!r} container {stream_name}:\n{text}",
                file=sys.stderr,
            )

    def _safe_stop(self, container) -> None:
        try:
            container.stop()
        except Exception as exc:
            # Best-effort cleanup: log why the dead container wouldn't stop, but
            # don't let it mask the original startup failure being re-raised.
            print(
                f"[testcontainers] {self.id!r} failed to stop dead container "
                f"during retry cleanup: {exc!r}",
                file=sys.stderr,
            )

    def stop_fully(self):
        if self.container is not None:
            self.container.stop()

    def start_background(self):
        executor = ThreadPoolExecutor(max_workers=1)
        executor.submit(self.start_fully)
        executor.shutdown(wait=False, cancel_futures=False)
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

    def _try_signal_starting(self):
        try:
            fd = os.open(self._starter_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.close(fd)
            return True
        except FileExistsError:
            return False

    def _signal_started(self):
        if self.register_for_shutdown:
            self.add_shutdown_signal()

    def _signal_not_started(self):
        self._starter_file.unlink(missing_ok=True)

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
