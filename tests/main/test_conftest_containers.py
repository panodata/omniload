"""Which shared containers a pytest invocation boots, decided by its targets."""

from pathlib import Path
from types import SimpleNamespace

from tests import conftest
from tests.warehouse.settings import DESTINATIONS, SOURCES

ROOT = Path(conftest.__file__).resolve().parent.parent
EXASOL = "tests/warehouse/db/test_exasol.py"
MATRIX = set(SOURCES.values()) | set(DESTINATIONS.values())


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
