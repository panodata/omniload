import logging
import tempfile
from pathlib import Path

import pytest

from tests.util import get_testdata_path


def pytest_configure(config):
    logging.getLogger("urllib3.connectionpool").setLevel(logging.INFO)
    logging.getLogger("testcontainers.core.waiting_utils").setLevel(logging.WARNING)
    logging.getLogger("testcontainers.core.container").setLevel(logging.WARNING)
    if is_master(config):
        config.shared_directory = tempfile.mkdtemp()


def pytest_configure_node(node):
    """xdist hook"""
    node.workerinput["shared_directory"] = node.config.shared_directory


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
