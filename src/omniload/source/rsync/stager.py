"""Stage a remote file selection into a local directory via oc-rsync."""

from __future__ import annotations

import hashlib
import os
import tempfile
from dataclasses import dataclass
from typing import Tuple

from dlt_filesystem.source.impl.util import _split_dir_glob
from omniload.source.rsync.command import (
    CommandRunner,
    RsyncCommandBuilder,
    filter_rules,
    is_recursive_glob,
)
from omniload.source.rsync.config import RsyncConfig
from omniload.source.rsync.transport import RsyncTransport

STAGING_PREFIX = "omniload-rsync"


@dataclass(frozen=True)
class FileSelection:
    """The remote directory to read and the glob that selects files within it."""

    root: str
    pattern: str

    @classmethod
    def from_remote_path(cls, remote_path: str) -> "FileSelection":
        """Split a remote path into ``(root, pattern)`` for staging and reading.

        Examples: ``/data/**/*.csv`` -> ``("/data", "**/*.csv")``;
        ``/srv/one.jsonl`` -> ``("/srv", "one.jsonl")``.
        """
        root, pattern = _split_dir_glob(remote_path)
        return cls(root=root, pattern=pattern)


class RsyncStager:
    """Run the oc-rsync transfer for one file selection.

    The staging directory is keyed by transport namespace and file selection,
    so repeated runs reuse the same local cache and only transfer the delta.
    """

    def __init__(
        self,
        transport: RsyncTransport,
        config: RsyncConfig,
        runner: CommandRunner,
    ) -> None:
        self._transport = transport
        self._config = config
        self._runner = runner

    def staging_dir(self, selection: FileSelection) -> str:
        """Return (creating if needed) the deterministic staging directory."""
        base = self._config.staging_dir or os.path.join(
            tempfile.gettempdir(), STAGING_PREFIX
        )
        identity = "\0".join(
            [self._transport.storage_namespace(), selection.root, selection.pattern]
        )
        digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:32]
        path = os.path.join(base, digest)
        os.makedirs(path, mode=0o700, exist_ok=True)
        return path

    def stage(self, selection: FileSelection) -> str:
        """Transfer the selected files into staging and return its path."""
        destination = self.staging_dir(selection)
        argv = self._build(selection, destination)
        self._runner.run(argv, env=self._transport.environment())
        return destination

    def _build(self, selection: FileSelection, destination: str) -> list[str]:
        """Assemble the full argv for this transfer."""
        builder = RsyncCommandBuilder(self._config.binary)
        for flag in self._base_flags(selection.pattern):
            builder.flag(flag)
        self._apply_tuning(builder)
        builder.extend(self._transport.command_args())
        builder.extend(self._config.extra_args)
        builder.filters(filter_rules(selection.pattern))
        builder.source(self._transport.remote_spec(selection.root))
        builder.destination(destination)
        return builder.build()

    def _base_flags(self, pattern: str) -> Tuple[str, ...]:
        """Return policy flags: ``-q`` quiet, ``-t`` preserve mtime, ``-L``
        resolve symlinks, ``--partial`` resume, ``-r`` recurse (recursive globs
        only), ``-z`` compress (opt-out via config).
        """
        flags = ["-q", "-t", "-L", "--partial"]
        if is_recursive_glob(pattern):
            flags.append("-r")
        if self._config.compress:
            flags.append("-z")
        return tuple(flags)

    def _apply_tuning(self, builder: RsyncCommandBuilder) -> None:
        """Append the optional numeric/string tuning options that are set."""
        config = self._config
        if config.timeout is not None:
            builder.option("--timeout", str(config.timeout))
        if config.contimeout is not None:
            builder.option("--contimeout", str(config.contimeout))
        if config.bwlimit:
            builder.option("--bwlimit", config.bwlimit)
        if config.rsync_path:
            builder.option("--rsync-path", config.rsync_path)
