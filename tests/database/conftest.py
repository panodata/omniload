import os
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import pytest
import sqlalchemy

from omniload.src.destinations import ClickhouseDestination
from tests.database.container import DESTINATIONS, SOURCES


def pytest_sessionstart(session):
    start_containers(session.config)


def pytest_sessionfinish(session, exitstatus):
    stop_containers(session.config)


@pytest.fixture(scope="session", autouse=True)
def manage_containers(request, shared_directory):
    unique_containers = set(SOURCES.values()) | set(DESTINATIONS.values())
    for container in unique_containers:
        container.container_lock_dir = shared_directory  # ty: ignore[invalid-assignment]


def start_containers(config):
    if hasattr(config, "workerinput"):
        return

    unique_containers = set(SOURCES.values()) | set(DESTINATIONS.values())
    for container in unique_containers:
        container.container_lock_dir = config.shared_directory  # ty: ignore[invalid-assignment]

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
                pass
        future.result()  # re-raises the captured exception with its traceback


def stop_containers(config):
    if hasattr(config, "workerinput"):
        return

    should_manage_containers = os.environ.get("PYTEST_XDIST_WORKER", "gw0") == "gw0"
    if not should_manage_containers:
        return

    unique_containers = set(SOURCES.values()) | set(DESTINATIONS.values())

    for container in unique_containers:
        container.stop_fully()


@pytest.fixture(scope="session", autouse=True)
def autocreate_db_for_clickhouse():
    """
    patches ClickhouseDestination to autocreate dest tables if they don't exist
    """
    dlt_dest = ClickhouseDestination().dlt_dest

    def patched_dlt_dest(uri, **kwargs):
        db, _ = kwargs["dest_table"].split(".")
        dest_engine = sqlalchemy.create_engine(uri)
        dest_conn = dest_engine.connect()
        dest_conn.exec_driver_sql(f"CREATE DATABASE IF NOT EXISTS {db}")
        return dlt_dest(uri, **kwargs)

    patcher = patch("omniload.src.factory.ClickhouseDestination.dlt_dest")
    mock = patcher.start()
    mock.side_effect = patched_dlt_dest
    yield
    patcher.stop()
