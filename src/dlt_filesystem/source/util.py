import json
from typing import Any, Dict, List

from sqlalchemy.util import asbool


def shrink_qs_dict(data: Dict[str, Any]) -> Dict[str, Any]:
    """URL query strings yield multiple values for the same key. Let's only use the first element."""
    return {key: value[0] for key, value in data.items()}


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
    """Cast dictionary values to integers."""
    for field_name in names:
        if field_name in data:
            data[field_name] = json.loads(data[field_name])
    return data


def apply_alias(data: Dict[str, Any], name: str, effective_name: str) -> Dict[str, Any]:
    """Apply aliasing to dictionary keys."""
    if name in data:
        data[effective_name] = data.pop(name)
    return data
