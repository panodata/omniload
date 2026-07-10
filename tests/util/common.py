import importlib.resources
import os
import random
import string
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Union

import pytest


def as_datetime(date_str: str) -> date:
    return datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc).date()


def as_datetime_notz(date_str: str) -> datetime:
    return datetime.strptime(date_str, "%Y-%m-%d")


def get_random_string(length) -> str:
    letters = string.ascii_lowercase
    result_str = "".join(random.choice(letters) for i in range(length))  # noqa: S311
    return result_str


def has_exception(exception, exc_type):
    if isinstance(exception, pytest.ExceptionInfo):
        exception = exception.value

    while exception:
        if isinstance(exception, exc_type):
            return True
        exception = exception.__cause__
    return False


def get_abs_path(relative_path: Union[str, Path]) -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), relative_path))


def get_etc_path() -> Path:
    """Path to the tests/etc directory."""
    with importlib.resources.path("tests", "etc") as path:
        return path


def get_testdata_path() -> Path:
    """Path to the omniload/testdata directory."""
    with importlib.resources.path("omniload", "testdata") as path:
        return path


def pp(x):
    print(x, file=sys.stderr)
