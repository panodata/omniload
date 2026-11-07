"""Which shared containers a pytest invocation boots, decided by its targets."""

from pathlib import Path
from types import SimpleNamespace

from tests import conftest
from tests.warehouse.settings import DESTINATIONS, SOURCES

ROOT = Path(conftest.__file__).resolve().parent.parent
EXASOL = "tests/warehouse/db/test_exasol.py"
MATRIX = set(SOURCES.values()) | set(DESTINATIONS.values())


MANAGED = "tests/warehouse/managed"
LAMBDA = f"{MANAGED}/test_aws_lambda.py"


def config(*args):
    return SimpleNamespace(
        args=list(args), rootpath=ROOT, option=SimpleNamespace(markexpr="")
    )


def test_exasol_module_alone_boots_no_shared_containers():
    for args in (
        [EXASOL],
        [f"{EXASOL}::test_read_only_user"],
        [f"./{EXASOL}"],
        [str(ROOT / EXASOL)],
    ):
        assert conftest._managed_containers(config(*args)) == set(), args


def test_exasol_module_among_other_targets_boots_the_matrix():
    for args in (
        [EXASOL, "tests/warehouse/db/test_mysql.py"],
        [EXASOL, "."],
        ["tests/warehouse/db"],
        ["tests"],
        [],
    ):
        assert MATRIX <= conftest._managed_containers(config(*args)), args


def test_managed_runtime_tests_alone_boot_no_shared_containers():
    for args in (
        [MANAGED],
        [f"{MANAGED}/"],
        [LAMBDA],
        [f"{LAMBDA}::test_stack"],
        [f"./{LAMBDA}"],
        [str(ROOT / LAMBDA)],
        [str(ROOT / MANAGED)],
        [LAMBDA, MANAGED],
    ):
        assert conftest._managed_containers(config(*args)) == set(), args


def test_managed_runtime_tests_among_other_targets_boot_the_matrix():
    for args in (
        [LAMBDA, "tests/warehouse/db/test_mysql.py"],
        [MANAGED, "tests/warehouse/lakehouse"],
        [LAMBDA, EXASOL],
        [LAMBDA, "tests/main/filesystem"],
        [LAMBDA, "."],
        ["tests/warehouse"],
        ["tests"],
        [],
    ):
        assert MATRIX <= conftest._managed_containers(config(*args)), args
