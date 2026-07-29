"""Exceptions raised by the oc-rsync connector."""

from __future__ import annotations

from typing import Sequence


class RsyncError(Exception):
    """Base class for every error raised by the oc-rsync connector."""


class UnsupportedTransportError(RsyncError):
    """The URI scheme does not map to a known oc-rsync transport."""

    def __init__(self, scheme: str, supported: Sequence[str]) -> None:
        joined = ", ".join(sorted(supported))
        super().__init__(
            f"Unsupported rsync transport scheme {scheme!r}; use one of: {joined}"
        )
        self.scheme = scheme


class InvalidRsyncUriError(RsyncError):
    """The URI or source table did not carry the parts a transport requires."""


class RsyncTransferError(RsyncError):
    """The ``oc-rsync`` process exited with a non-zero status.

    Carries the invoked argv, the exit code, and captured stderr so callers get
    an actionable message instead of a bare ``CalledProcessError``. The remote
    source spec is the last-but-one argv element; secrets never appear on the
    command line (daemon passwords travel via ``--password-file`` or the
    ``RSYNC_PASSWORD`` environment variable), so echoing argv is safe.
    """

    def __init__(self, argv: Sequence[str], returncode: int, stderr: str) -> None:
        self.argv = list(argv)
        self.returncode = returncode
        self.stderr = stderr.strip()
        detail = f": {self.stderr}" if self.stderr else ""
        super().__init__(
            f"oc-rsync exited with status {returncode} "
            f"(command: {' '.join(self.argv)}){detail}"
        )
