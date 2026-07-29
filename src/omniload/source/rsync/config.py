"""Tuning options for an oc-rsync transfer.

Parsed from the source-URI query string; connection-specific settings live
in the transport layer instead.
"""

from __future__ import annotations

import os
import shlex
from dataclasses import dataclass, field
from typing import Dict, List, Mapping, Optional
from urllib.parse import parse_qsl, urlparse

DEFAULT_BINARY = "oc-rsync"
ALLOWED_BINARIES = frozenset({"oc-rsync", "rsync"})


def query_params(uri: str) -> Dict[str, str]:
    """Return the URI query string as a ``{key: value}`` map.

    Duplicate keys keep the last occurrence; blank values are preserved so
    ``?no_motd=`` is distinguishable from an absent key.
    """
    query = urlparse(uri).query
    return {key: value for key, value in parse_qsl(query, keep_blank_values=True)}


def _as_bool(value: Optional[str], default: bool) -> bool:
    """Interpret a query-string value as a boolean.

    A bare flag (``?compress``) is ``True``; accepts ``true/false``, ``1/0``,
    ``yes/no``, ``on/off``.
    """
    if value is None:
        return default
    token = value.strip().lower()
    if token == "":
        return True
    if token in {"1", "true", "yes", "on"}:
        return True
    if token in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"Expected a boolean value, got {value!r}")


def _as_int(value: Optional[str]) -> Optional[int]:
    """Parse an optional integer query value, raising on non-numeric input."""
    if value is None or value.strip() == "":
        return None
    return int(value)


@dataclass(frozen=True)
class RsyncConfig:
    """Immutable tuning options for a single oc-rsync transfer."""

    binary: str = DEFAULT_BINARY
    compress: bool = True
    timeout: Optional[int] = None
    contimeout: Optional[int] = None
    bwlimit: Optional[str] = None
    rsync_path: Optional[str] = None
    staging_dir: Optional[str] = None
    extra_args: List[str] = field(default_factory=list)

    @classmethod
    def from_params(cls, params: Mapping[str, str]) -> "RsyncConfig":
        """Build a config from a URI query-parameter map.

        ``extra_args`` is shell-split so multiple tokens can be passed in a
        single query value (e.g. ``?extra_args=--chmod%3DD755%20--numeric-ids``).
        """
        extra = params.get("extra_args")
        binary = params.get("binary") or DEFAULT_BINARY
        _validate_binary(binary)
        return cls(
            binary=binary,
            compress=_as_bool(params.get("compress"), default=True),
            timeout=_as_int(params.get("timeout")),
            contimeout=_as_int(params.get("contimeout")),
            bwlimit=params.get("bwlimit") or None,
            rsync_path=params.get("rsync_path") or None,
            staging_dir=params.get("staging_dir") or None,
            extra_args=shlex.split(extra) if extra else [],
        )


def _validate_binary(binary: str) -> None:
    """Raise ``ValueError`` if ``binary`` is not an allowed rsync executable.

    Absolute paths are accepted when their basename is allowed (e.g.
    ``/usr/local/bin/rsync``), bare names must match directly.
    """
    name = os.path.basename(binary) if os.sep in binary or "/" in binary else binary
    if name not in ALLOWED_BINARIES:
        allowed = ", ".join(sorted(ALLOWED_BINARIES))
        raise ValueError(
            f"Binary {binary!r} is not allowed; permitted names: {allowed}"
        )
