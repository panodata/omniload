import time

import pytest

from tests.container.model import DockerService


class FlakyFakeContainer:
    """Stand-in for a testcontainers container whose first ``fail_times`` starts
    raise, used to exercise DockerImage._start_container without Docker."""

    def __init__(
        self, fail_times: int, logs=(b"server exited code 1", b""), error="boom"
    ):
        self.fail_times = fail_times
        self.logs = logs
        self.error = error
        self.start_calls = 0
        self.stopped = False

    def start(self):
        self.start_calls += 1
        if self.start_calls <= self.fail_times:
            raise RuntimeError(self.error)
        return self

    def get_logs(self):
        return self.logs

    def stop(self):
        self.stopped = True


def test_start_container_retries_then_succeeds(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda *_: None)
    created = []

    def creator():
        # A fresh container per attempt: the first two die on start, the third boots.
        c = FlakyFakeContainer(fail_times=1 if len(created) < 2 else 0)
        created.append(c)
        return c

    image = DockerService("fake", creator)
    result = image._start_container(attempts=3)

    # Fresh container per attempt; the two dead ones get stopped, the live one stays.
    assert len(created) == 3
    assert result is created[-1]
    assert [c.stopped for c in created] == [True, True, False]


def test_start_container_exhausts_attempts_and_dumps_logs(monkeypatch, capsys):
    monkeypatch.setattr(time, "sleep", lambda *_: None)
    created = []

    def creator():
        # Distinct error per attempt so the assertion below proves the LAST
        # exception is re-raised, not the first.
        c = FlakyFakeContainer(fail_times=99, error=f"boom {len(created) + 1}")
        created.append(c)
        return c

    image = DockerService("fake", creator)
    with pytest.raises(RuntimeError, match="boom 3"):
        image._start_container(attempts=3)

    assert len(created) == 3
    assert all(c.stopped for c in created)
    err = capsys.readouterr().err
    assert "server exited code 1" in err  # the dead container's own reason, surfaced
    assert "attempt 3/3" in err
