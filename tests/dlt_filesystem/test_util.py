from dlt_filesystem.util.python import (
    apply_alias,
    asbool,
    cast_to_bool,
    cast_to_float,
    cast_to_list,
)


def test_asbool():
    """Validate the `asbool` utility function."""
    assert asbool("true") is True
    assert asbool("yes") is True
    assert asbool("false") is False
    assert asbool("no") is False


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


def test_apply_alias():
    """Validate the `apply_alias` utility function."""
    data = {"foo": "bar"}
    apply_alias(data, "foo", "effective")
    assert data["effective"] == "bar"
