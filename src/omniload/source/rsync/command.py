"""Building and running the ``oc-rsync`` command line."""

from __future__ import annotations

import os
import subprocess
from typing import List, Mapping, Optional, Protocol, Sequence

from omniload.source.rsync.error import RsyncTransferError


def _is_recursive(pattern: str) -> bool:
    """Return whether a glob may match files below the top level."""
    return "**" in pattern or "/" in pattern


def filter_rules(pattern: str) -> List[str]:
    """Translate a file glob into oc-rsync ``--filter`` rules.

    Recursive globs use ``+ */`` / ``+ <basename>`` / ``- *``; non-recursive
    patterns include only the top level.  Basename extraction may over-select
    (e.g. ``logs/**/*.csv`` pulls ``*.csv`` from every directory) — the reader
    re-applies the exact glob on the staged tree, so over-selection costs
    bandwidth but never drops a file.
    """
    if not pattern:
        raise ValueError("filter pattern must not be empty")
    if not _is_recursive(pattern):
        return [f"+ {pattern}", "- *"]
    basename = pattern.rsplit("/", 1)[-1]
    return ["+ */", f"+ {basename}", "- *"]


def is_recursive_glob(pattern: str) -> bool:
    """Return whether ``pattern`` requires recursive (``-r``) transfer."""
    return _is_recursive(pattern)


class RsyncCommandBuilder:
    """Fluent builder for an ``oc-rsync`` argument vector.

    Argv order: binary, flags/options, ``--filter`` rules, source, destination.
    """

    def __init__(self, binary: str) -> None:
        self._binary = binary
        self._options: List[str] = []
        self._filters: List[str] = []
        self._source: Optional[str] = None
        self._destination: Optional[str] = None

    def flag(self, name: str) -> "RsyncCommandBuilder":
        """Append a bare flag such as ``-r`` or ``--no-motd``."""
        self._options.append(name)
        return self

    def option(self, name: str, value: str) -> "RsyncCommandBuilder":
        """Append a ``--name value`` option pair."""
        self._options += [name, value]
        return self

    def extend(self, tokens: Sequence[str]) -> "RsyncCommandBuilder":
        """Append already-tokenised options (transport args, user extras)."""
        self._options.extend(tokens)
        return self

    def filters(self, rules: Sequence[str]) -> "RsyncCommandBuilder":
        """Append ``--filter`` rules, applied in order (first match wins)."""
        for rule in rules:
            self._filters += ["--filter", rule]
        return self

    def source(self, spec: str) -> "RsyncCommandBuilder":
        """Set the remote source spec (produced by the transport)."""
        self._source = spec
        return self

    def destination(self, path: str) -> "RsyncCommandBuilder":
        """Set the local staging destination (trailing slash added if absent)."""
        self._destination = path if path.endswith("/") else f"{path}/"
        return self

    def build(self) -> List[str]:
        """Materialise the argv, validating that source and destination are set."""
        if self._source is None:
            raise ValueError("rsync command requires a source spec")
        if self._destination is None:
            raise ValueError("rsync command requires a destination")
        return [
            self._binary,
            *self._options,
            *self._filters,
            self._source,
            self._destination,
        ]


class CommandRunner(Protocol):
    """Protocol for executing an oc-rsync process."""

    def run(self, argv: Sequence[str], env: Optional[Mapping[str, str]] = None) -> None:
        """Execute ``argv``, raising :class:`RsyncTransferError` on failure."""
        ...


class SubprocessRunner:
    """Run oc-rsync via :mod:`subprocess`.

    ``env`` is overlaid on the current process environment so transport secrets
    are added without discarding ``PATH``.
    """

    def run(self, argv: Sequence[str], env: Optional[Mapping[str, str]] = None) -> None:
        process_env = {**os.environ, **(env or {})}
        completed = subprocess.run(  # noqa: S603 - argv is fully constructed, no shell
            list(argv),
            env=process_env,
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.returncode != 0:
            raise RsyncTransferError(argv, completed.returncode, completed.stderr)
