import logging
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

import docker
import docker.errors
import pytest
import sqlalchemy

from omniload.target.clickhouse import ClickhouseDestination
from tests.util.common import get_testdata_path
from tests.warehouse.settings import DESTINATIONS, SOURCES

logger = logging.getLogger(__name__)


def pytest_configure(config):
    logging.getLogger("urllib3.connectionpool").setLevel(logging.INFO)
    logging.getLogger("testcontainers.core.waiting_utils").setLevel(logging.WARNING)
    logging.getLogger("testcontainers.core.container").setLevel(logging.WARNING)
    if is_master(config):
        config.shared_directory = tempfile.mkdtemp()


def pytest_configure_node(node):
    """xdist hook"""
    node.workerinput["shared_directory"] = node.config.shared_directory


def pytest_sessionstart(session):
    start_containers(session.config)


def pytest_sessionfinish(session, exitstatus):
    stop_containers(session.config)
    stop_containers_more(session.config)


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(config, items):
    """Auto-mark the Docker/credential-backed warehouse subtree as `integration`.

    Path-based so it is independent of import mode, and `tryfirst` so the marker
    is present before pytest's own `-m` deselection runs.
    """
    for item in items:
        if "warehouse" in item.path.parts:
            item.add_marker(pytest.mark.integration)


def _docker_available():
    try:
        docker.from_env().ping()
        return True
    except Exception:
        return False


def _integration_deselected(config):
    # Skip the DB-matrix boot only for the canonical `-m "not integration"`
    # (what `poe test-fast` runs). Match exactly rather than by substring: a
    # compound expression like `not integration or smoke` *can* still select
    # integration-marked tests, and a wrong skip would fail them, while a missed
    # skip merely wastes boot time. So anything fancier errs toward booting.
    return (config.option.markexpr or "").strip() == "not integration"


def _skip_containers(config):
    return _integration_deselected(config) or not _docker_available()


@pytest.fixture(scope="session")
def shared_directory(request):
    """
    Returns a unique and temporary directory which can be shared by
    master or worker nodes in xdist runs.
    https://hackebrot.github.io/pytest-tricks/shared_directory_xdist/
    """
    if is_master(request.config):
        return request.config.shared_directory
    else:
        return request.config.workerinput["shared_directory"]


def is_master(config):
    """True if the code running the given pytest.config object is running in a xdist master
    node or not running xdist at all.
    """
    return not hasattr(config, "workerinput")


@pytest.fixture(scope="session")
def testdata_path() -> Path:
    """Path to the test data directory, as pytest fixture."""
    return get_testdata_path()


@pytest.fixture(scope="session", autouse=True)
def manage_containers(request, shared_directory):
    unique_containers = set(SOURCES.values()) | set(DESTINATIONS.values())
    for container in unique_containers:
        container.lock_dir = shared_directory  # ty: ignore[invalid-assignment, unresolved-attribute, unused-ignore-comment]


def start_containers(config):
    if _skip_containers(config):
        return
    if is_worker(config):
        return

    unique_containers = set(SOURCES.values()) | set(DESTINATIONS.values())
    unique_containers = [x for x in unique_containers if x is not None]

    for container in unique_containers:
        container.lock_dir = config.shared_directory  # ty: ignore[invalid-assignment, unresolved-attribute, unused-ignore-comment]

    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(container.start_fully) for container in unique_containers
        ]

    # ThreadPoolExecutor.__exit__ has joined every start_fully() call by now. Surface
    # the first startup failure with its real traceback, instead of letting it be
    # swallowed and resurface as a misleading "Failed to start container after bunch
    # of attempts" timeout in every test that depends on the container.
    for future in futures:
        if future.exception() is None:
            continue
        # pytest skips pytest_sessionfinish (so stop_containers never runs) when
        # pytest_sessionstart raises, and Ryuk is disabled in the default test run.
        # Stop any containers that did come up so they don't leak, then re-raise.
        for container in unique_containers:
            try:
                container.stop_fully()
            except Exception:
                logger.exception(
                    f"Failed to stop container: {getattr(container, 'id')}"
                )
        future.result()  # re-raises the captured exception with its traceback


def stop_containers(config):
    if _skip_containers(config):
        return
    if is_worker(config):
        return

    should_manage_containers = os.environ.get("PYTEST_XDIST_WORKER", "gw0") == "gw0"
    if not should_manage_containers:
        return

    unique_containers = set(SOURCES.values()) | set(DESTINATIONS.values())
    unique_containers = [x for x in unique_containers if x is not None]

    for container in unique_containers:
        try:
            container.stop_fully()
        except Exception:
            logger.exception(f"Failed to stop container: {getattr(container, 'id')}")


def stop_containers_more(config):
    """
    More containers clean-up.

    On the controller, at the end of the session, also stop all other
    containers not tracked by `SOURCES` or `DESTINATIONS`.
    """
    if _skip_containers(config):
        return
    if is_worker(config):
        return
    shared_directory = config.shared_directory
    shutdown_files = Path(shared_directory).glob("*.shutdown")
    docker_client = docker.DockerClient.from_env()
    for shutdown_file in shutdown_files:
        container_id = shutdown_file.stem
        try:
            container = docker_client.containers.get(container_id)
        except docker.errors.NotFound:
            continue
        container.remove(force=True)
    docker_client.close()


def is_worker(config):
    """True if the code running the given pytest.config object is running in a xdist master
    node or not running xdist at all.
    """
    return hasattr(config, "workerinput")


@pytest.fixture(scope="session", autouse=True)
def autocreate_db_for_clickhouse():
    """
    patches ClickhouseDestination to autocreate dest tables if they don't exist
    """
    dlt_dest = ClickhouseDestination().dlt_dest

    def patched_dlt_dest(uri, **kwargs):
        db, _ = kwargs["dest_table"].split(".")
        dest_engine = sqlalchemy.create_engine(uri)
        with dest_engine.connect() as dest_conn:
            dest_conn.exec_driver_sql(f"CREATE DATABASE IF NOT EXISTS {db}")
        dest_engine.dispose()
        return dlt_dest(uri, **kwargs)

    patcher = patch("omniload.target.clickhouse.ClickhouseDestination.dlt_dest")
    mock = patcher.start()
    mock.side_effect = patched_dlt_dest
    yield
    patcher.stop()
