"""Test doubles for the rsync connector."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Dict, List, Mapping, Optional, Sequence

from omniload.source.rsync.transport import RsyncTransport


@dataclass
class RecordingRunner:
    """Records calls and optionally stages files under the destination.

    When ``files`` is provided, each ``run`` writes them into the destination
    directory (the final argv element), emulating a completed transfer.
    """

    files: Mapping[str, str] = field(default_factory=dict)
    argv: List[str] = field(default_factory=list)
    env: Mapping[str, str] = field(default_factory=dict)
    calls: int = 0

    def run(self, argv: Sequence[str], env: Optional[Mapping[str, str]] = None) -> None:
        self.argv = list(argv)
        self.env = dict(env) if env else {}
        self.calls += 1
        destination = self.argv[-1]
        for rel_path, content in self.files.items():
            target = os.path.join(destination, rel_path)
            os.makedirs(os.path.dirname(target) or destination, exist_ok=True)
            with open(target, "w", encoding="utf-8") as handle:
                handle.write(content)

    @property
    def options(self) -> List[str]:
        """Return argv without the binary, source, and destination."""
        assert self.argv is not None
        return self.argv[1:-2]


@dataclass(frozen=True)
class FakeTransport(RsyncTransport):
    """Minimal transport stub for exercising the stager in isolation."""

    spec_prefix: str = "fake:"
    args: tuple = ()
    env: Dict[str, str] = field(default_factory=dict)
    namespace: str = "fake:host"

    def remote_spec(self, remote_root: str) -> str:
        return f"{self.spec_prefix}{remote_root}/"

    def command_args(self) -> list:
        return list(self.args)

    def environment(self) -> Dict[str, str]:
        return dict(self.env)

    def storage_namespace(self) -> str:
        return self.namespace
