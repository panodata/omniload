import logging
import tempfile

import pytest

logging.getLogger("testcontainers.core.waiting_utils").setLevel(logging.WARNING)
logging.getLogger("testcontainers.core.container").setLevel(logging.WARNING)


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
