"""Transport strategies for reaching an oc-rsync source.

SSH (``rsync+ssh://``) uses ``host:path`` through a remote shell; the daemon
transport (``rsync://``) uses a URL on TCP 873.  Connection-specific behaviour
is isolated behind :class:`RsyncTransport`; new transports subclass it and
register in ``_FACTORIES``.
"""

from __future__ import annotations

import shlex
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Mapping, Optional
from urllib.parse import ParseResult, urlparse

from omniload.source.rsync.error import (
    InvalidRsyncUriError,
    UnsupportedTransportError,
)

SSH_SCHEME = "rsync+ssh"
DAEMON_SCHEME = "rsync"
DEFAULT_DAEMON_PORT = 873
DEFAULT_SSH_PORT = 22


def _authority(user: Optional[str], host: str) -> str:
    """Render the ``[user@]host`` half of a remote spec."""
    return f"{user}@{host}" if user else host


class RsyncTransport(ABC):
    """Strategy interface for an oc-rsync connection form."""

    host: str
    user: Optional[str]

    @abstractmethod
    def remote_spec(self, remote_root: str) -> str:
        """Return the oc-rsync source argument for ``remote_root``.

        Trailing slashes on ``remote_root`` are normalised; the returned spec
        always ends in ``/`` so oc-rsync copies the *contents* of the root,
        keeping the staged layout aligned with the reader's glob.
        """

    def command_args(self) -> List[str]:
        """Return transport-specific CLI options (empty by default)."""
        return []

    def environment(self) -> Dict[str, str]:
        """Return transport-specific environment overlays (empty by default)."""
        return {}

    @abstractmethod
    def storage_namespace(self) -> str:
        """Return a stable, secret-free identity for this connection."""


@dataclass(frozen=True)
class SshTransport(RsyncTransport):
    """SSH transport: ``[user@]host:path``, reached through a remote shell.

    ``rsh`` overrides the entire remote-shell command; otherwise ``ssh_key``,
    ``port``, and ``ssh_options`` compose a ``ssh`` invocation (``-e`` is
    emitted only when at least one of these is present).
    """

    host: str
    user: Optional[str] = None
    port: Optional[int] = None
    rsh: Optional[str] = None
    ssh_key: Optional[str] = None
    ssh_options: Optional[str] = None

    def remote_spec(self, remote_root: str) -> str:
        return f"{_authority(self.user, self.host)}:{remote_root.rstrip('/')}/"

    def command_args(self) -> List[str]:
        shell = self._remote_shell()
        return ["-e", shell] if shell else []

    def storage_namespace(self) -> str:
        host = self.host.lower()
        port = self.port or DEFAULT_SSH_PORT
        return f"rsync+ssh:{host}:{port}:{self.user or ''}"

    def _remote_shell(self) -> Optional[str]:
        """Compose the ``-e`` remote-shell command, or ``None`` for the default."""
        if self.rsh:
            return self.rsh

        parts: List[str] = []
        if self.ssh_key or self.port or self.ssh_options:
            parts.append("ssh")
            if self.port:
                parts += ["-p", str(self.port)]
            if self.ssh_key:
                parts += ["-i", self.ssh_key]
            if self.ssh_options:
                parts += shlex.split(self.ssh_options)
        return shlex.join(parts) if parts else None

    @classmethod
    def from_uri(cls, parsed: ParseResult, params: Mapping[str, str]) -> "SshTransport":
        if not parsed.hostname:
            raise InvalidRsyncUriError("SSH rsync URI requires a host")
        return cls(
            host=parsed.hostname,
            user=parsed.username or params.get("user") or None,
            port=parsed.port or _int_param(params.get("ssh_port")),
            rsh=params.get("rsh") or params.get("ssh_command") or None,
            ssh_key=params.get("ssh_key") or None,
            ssh_options=params.get("ssh_options") or None,
        )


@dataclass(frozen=True)
class DaemonTransport(RsyncTransport):
    """Daemon transport: ``rsync://[user@]host[:port]/module/path``.

    ``--password-file`` is preferred for authentication; otherwise an inline
    password is promoted to ``RSYNC_PASSWORD`` so it never appears on the
    command line.
    """

    host: str
    user: Optional[str] = None
    port: int = DEFAULT_DAEMON_PORT
    password: Optional[str] = None
    password_file: Optional[str] = None
    no_motd: bool = True

    def remote_spec(self, remote_root: str) -> str:
        root = remote_root.strip("/")
        return f"rsync://{_authority(self.user, self.host)}:{self.port}/{root}/"

    def command_args(self) -> List[str]:
        args: List[str] = ["--port", str(self.port)]
        if self.no_motd:
            args.append("--no-motd")
        if self.password_file:
            args += ["--password-file", self.password_file]
        return args

    def environment(self) -> Dict[str, str]:
        # File-based password wins; fall back to env only when no file is set.
        if self.password and not self.password_file:
            return {"RSYNC_PASSWORD": self.password}
        return {}

    def storage_namespace(self) -> str:
        return f"rsync:{self.host.lower()}:{self.port}:{self.user or ''}"

    @classmethod
    def from_uri(
        cls, parsed: ParseResult, params: Mapping[str, str]
    ) -> "DaemonTransport":
        if not parsed.hostname:
            raise InvalidRsyncUriError("rsync daemon URI requires a host")
        return cls(
            host=parsed.hostname,
            user=parsed.username or params.get("user") or None,
            port=parsed.port or _int_param(params.get("port")) or DEFAULT_DAEMON_PORT,
            password=parsed.password or params.get("password") or None,
            password_file=params.get("password_file") or None,
            no_motd=_bool_param(params.get("no_motd"), default=True),
        )


# Scheme -> factory. New transports register here.
_FACTORIES = {
    SSH_SCHEME: SshTransport.from_uri,
    DAEMON_SCHEME: DaemonTransport.from_uri,
}


def transport_for(uri: str, params: Mapping[str, str]) -> RsyncTransport:
    """Resolve the transport strategy for a source URI's scheme."""
    parsed = urlparse(uri)
    try:
        factory = _FACTORIES[parsed.scheme]
    except KeyError as exc:
        raise UnsupportedTransportError(parsed.scheme, list(_FACTORIES.keys())) from exc
    return factory(parsed, params)


def _int_param(value: Optional[str]) -> Optional[int]:
    """Parse an optional integer parameter, raising on non-numeric input."""
    if value is None or value.strip() == "":
        return None
    return int(value)


def _bool_param(value: Optional[str], default: bool) -> bool:
    """Parse an optional boolean parameter (bare key means true)."""
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
