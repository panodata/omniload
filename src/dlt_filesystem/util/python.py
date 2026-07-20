import inspect
import json
import logging
import types
import typing
from typing import Any, Callable, Dict, List, Optional

_TRUE_VALUES = {"true", "1", "yes", "on"}
_FALSE_VALUES = {"false", "0", "no", "off"}


logger = logging.getLogger(__name__)


def cast_kwargs_to_signature(
    func: Callable, hints: Dict[str, str], symbols: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Cast string hint values to the types declared by `func`'s signature.

    https://github.com/panodata/omniload/issues/214
    """
    symbols = symbols or {}
    sig = inspect.signature(func)
    try:
        type_hints = typing.get_type_hints(func, localns=symbols)
    except (NameError, TypeError) as exc:
        # `get_type_hints()` can fail for signatures that reference names only
        # available under `TYPE_CHECKING` and not supplied via `symbols`
        # (`NameError`), or for types that are not subscriptable during runtime,
        # depending on the Python version (`TypeError`). Fall back to no casting
        # (values pass through as strings) rather than crashing the reader.
        # https://github.com/python/typing/issues/819
        # https://github.com/tfranzel/drf-spectacular/issues/795
        # https://github.com/OpenBMB/ProAgent/issues/17
        # https://sqlpad.io/tutorial/solving-type-object-is-not-subscriptable-in-python/
        # https://www.devzery.com/post/typeerror-type-object-is-not-subscriptable
        if isinstance(exc, TypeError) and "is not subscriptable" not in str(exc):
            raise
        logger.warning("Unrecognized type hints: %s", exc)
        type_hints = {}
    casted: Dict[str, Any] = {}
    for key, value in hints.items():
        if key not in sig.parameters:
            casted[key] = value  # unknown kwarg, pass through
            continue
        casted[key] = _cast_value(value, type_hints.get(key, str))
    return casted


def _cast_value(value: str, target_type: Any) -> Any:
    """Helper function to cast value to target type."""
    origin = typing.get_origin(target_type)

    # Unwrap Optional[...] / Union[...] as well as PEP 604 `X | Y` unions.
    if origin is typing.Union or origin is types.UnionType:
        args = [a for a in typing.get_args(target_type) if a is not type(None)]
        if len(args) == 1:
            return _cast_value(value, args[0])
        for arg in args:  # ambiguous union, try each candidate
            try:
                return _cast_value(value, arg)
            except (ValueError, TypeError, json.JSONDecodeError):
                continue
        return value

    if target_type is bool:
        normalized = value.strip().lower()
        if normalized in _TRUE_VALUES:
            return True
        if normalized in _FALSE_VALUES:
            return False
        raise ValueError(f"Cannot cast {value!r} to bool")
    if target_type is int:
        return int(value)
    if target_type is float:
        return float(value)
    if target_type is str:
        return value
    if target_type in (list, dict) or origin in (list, dict):
        return json.loads(value)

    # Fallback for anything else (e.g. Sequence[...], Literal[...]): try JSON,
    # else leave as string.
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return value


# from paste.deploy.converters
def asbool(obj: Any) -> bool:
    """From `sqlalchemy.util.langhelpers`"""
    if isinstance(obj, str):
        obj = obj.strip().lower()
        if obj in ["true", "yes", "on", "y", "t", "1"]:
            return True
        elif obj in ["false", "no", "off", "n", "f", "0"]:
            return False
        else:
            raise ValueError("String is not true/false: %r" % obj)
    return bool(obj)


def cast_to_int(data: Dict[str, Any], names: List[str]) -> Dict[str, Any]:
    """Cast dictionary values to integers."""
    for field_name in names:
        if field_name in data:
            data[field_name] = int(data[field_name])
    return data


def cast_to_bool(data: Dict[str, Any], names: List[str]) -> Dict[str, Any]:
    """Cast dictionary values to booleans."""
    for field_name in names:
        if field_name in data:
            data[field_name] = asbool(data[field_name])
    return data


def cast_to_dict(data: Dict[str, Any], names: List[str]) -> Dict[str, Any]:
    """Cast dictionary values from JSON."""
    for field_name in names:
        if field_name in data:
            data[field_name] = json.loads(data[field_name])
    return data


def apply_alias(data: Dict[str, Any], name: str, effective_name: str) -> Dict[str, Any]:
    """Apply aliasing to dictionary keys."""
    if name in data:
        data[effective_name] = data.pop(name)
    return data
