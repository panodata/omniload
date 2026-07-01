import time

import pytest
from _pytest.outcomes import Skipped
from testcontainers.core.container import DockerContainer

from tests.util.container.model import DockerService


def _unused_creator() -> DockerContainer:
    """A container_creator for tests that never boot a container (they exercise the
    failure-marker path, which short-circuits before the creator is ever called)."""
    raise AssertionError("container_creator must not be called in this test")


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


def test_required_container_failure_fails_only_dependent_test(monkeypatch, tmp_path):
    """A recorded boot failure fails the dependent test fast with the real cause,
    instead of polling 40s for a connection URL that will never appear."""
    monkeypatch.setattr(time, "sleep", lambda *_: None)
    svc = DockerService("postgres", _unused_creator)
    svc.lock_dir = tmp_path
    svc.record_start_failure(RuntimeError("postgres exited code 1"))

    with pytest.raises(RuntimeError, match="postgres exited code 1") as excinfo:
        svc.start()
    # Failing, not skipping: a required container's death must surface loudly (#67).
    assert "failing dependent test" in str(excinfo.value)


def test_optional_container_failure_skips_only_dependent_test(monkeypatch, tmp_path):
    """An optional (flaky, non-product) container's boot failure skips its dependent
    tests rather than failing them, keeping the rest of the suite green."""
    monkeypatch.setattr(time, "sleep", lambda *_: None)
    svc = DockerService("sqlserver", _unused_creator, optional=True)
    svc.lock_dir = tmp_path
    svc.record_start_failure(RuntimeError("sql server exited code 1"))

    with pytest.raises(Skipped) as excinfo:
        svc.start()
    assert "sqlserver" in str(excinfo.value.msg)
    assert "sql server exited code 1" in str(excinfo.value.msg)


def test_healthy_container_unaffected_by_another_containers_failure(tmp_path):
    """Blast-radius isolation: a marker for one container does not disturb a healthy
    one. The healthy service returns its URL even though a sibling failed."""
    failed = DockerService("sqlserver", _unused_creator, optional=True)
    failed.lock_dir = tmp_path
    failed.record_start_failure(RuntimeError("boom"))

    healthy = DockerService("postgres", _unused_creator)
    healthy.lock_dir = tmp_path
    healthy.write_conn_url("postgresql://localhost:5432/test")

    assert healthy.start() == "postgresql://localhost:5432/test"
