import os
import tempfile
from concurrent.futures import ThreadPoolExecutor

import pytest

from .main_test import DESTINATIONS, SOURCES


def pytest_configure(config):
    if is_master(config):
        config.shared_directory = tempfile.mkdtemp()


def pytest_configure_node(node):
    """xdist hook"""
    node.workerinput["shared_directory"] = node.config.shared_directory


@pytest.fixture(scope="session")
def shared_directory(request):
    if is_master(request.config):
        return request.config.shared_directory
    else:
        return request.config.workerinput["shared_directory"]


def is_master(config):
    """True if the code running the given pytest.config object is running in a xdist master
    node or not running xdist at all.
    """
    return not hasattr(config, "workerinput")


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


def pytest_sessionstart(session):
    start_containers(session.config)


def pytest_sessionfinish(session, exitstatus):
    stop_containers(session.config)
