from typing import Any, Dict


def shrink_qs_dict(data: Dict[str, Any]) -> Dict[str, Any]:
    """Let's only use the first element when decoding URL query parameters."""
    return {key: value[0] for key, value in data.items()}
