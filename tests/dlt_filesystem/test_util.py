import pytest

from dlt_filesystem.source.impl.util import strip_protocol_suffix
from dlt_filesystem.util.python import (
    apply_alias,
    asbool,
    cast_to_bool,
    cast_to_float,
    cast_to_list,
)


def test_asbool_success():
    """Validate the `asbool` utility function."""
    assert asbool("true") is True
    assert asbool("yes") is True
    assert asbool("false") is False
    assert asbool("no") is False


def test_asbool_failure():
    """Validate failing the `asbool` utility function."""
    with pytest.raises(ValueError) as exc_info:
        asbool("unknown")
    assert exc_info.match("Cannot cast value to bool: 'unknown'")

    with pytest.raises(ValueError) as exc_info:
        asbool("2")
    assert exc_info.match("Cannot cast value to bool: '2'")

    with pytest.raises(ValueError) as exc_info:
        asbool("")
    assert exc_info.match("Cannot cast value to bool: ''")


def test_cast_to_bool():
    """Validate the `cast_to_bool` utility function."""
    data = {"foo": "1"}
    cast_to_bool(data, ["foo"])
    assert data["foo"] is True


def test_cast_to_float():
    """Validate the `cast_to_float` utility function."""
    data = {"foo": "42.42"}
    cast_to_float(data, ["foo"])
    assert data["foo"] == 42.42


def test_cast_to_list():
    """Validate the `cast_to_list` utility function."""
    data = {"foo": '["bar"]'}
    cast_to_list(data, ["foo"])
    assert data["foo"] == ["bar"]


def test_apply_alias_success():
    """Validate the `apply_alias` utility function."""
    data = {"foo": "bar"}
    apply_alias(data, "foo", "effective")
    assert data["effective"] == "bar"


def test_apply_alias_collision():
    """Validate failing the `apply_alias` utility function."""
    data = {"foo": "bar"}
    with pytest.raises(ValueError) as exc_info:
        apply_alias(data, "foo", "foo")
    assert exc_info.match("use only one")


def test_strip_protocol_suffix():
    """Validate the `strip_protocol_suffix` utility function."""
    assert (
        strip_protocol_suffix("http+dav://foo:1234/bar/?baz=+dav", "webdav", "dav")
        == "http://foo:1234/bar/?baz=+dav"
    )
    assert (
        strip_protocol_suffix("http+dav://foo:1234/bar/?baz=+webdav", "webdav", "dav")
        == "http://foo:1234/bar/?baz=+webdav"
    )
