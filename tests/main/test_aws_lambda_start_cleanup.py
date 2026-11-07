"""A failed container start in the AWS Lambda smoke leaves nothing of its own behind.

A Docker create can time out on the client while the daemon still creates the
container, so the smoke's fixture holds no handle to it. These tests inject that
fault with a fake Docker client, no daemon needed: the start's own error must
reach the caller, and only this run's container, matched by its run label and
exact name, may be removed.
"""

import pytest

import tests.warehouse.managed.test_aws_lambda as smoke

RUN = "a1b2c3d4"
LABELS = {smoke.RUN_LABEL: RUN}
FLOCI_NAME = f"omniload-lambda-{RUN}-floci"
EMULATOR_NAME = f"omniload-lambda-{RUN}-rie"


class FakeContainer:
    def __init__(self, name: str, labels: dict[str, str], status: str = "running"):
        self.id = f"id-{name}"
        self.name = name
        self.labels = labels
        self.status = status
        self.removed = False

    def remove(self, force: bool = False, v: bool = False) -> None:
        self.removed = True


class FakeContainers:
    """The Docker filters the smoke uses: `label` as `key=value`, exact; `name` as
    a substring of the name, as the Docker API matches it. As in Docker, a
    container that is not running is listed only with `all=True`."""

    def __init__(self, existing: list[FakeContainer]):
        self.all = list(existing)
        self.run_error: BaseException | None = None

    def list(self, all: bool = False, filters: dict | None = None) -> list:
        filters = filters or {}
        found = []
        for container in self.all:
            if container.removed:
                continue
            if not all and container.status != "running":
                continue
            label = filters.get("label")
            if label is not None:
                key, _, value = label.partition("=")
                if container.labels.get(key) != value:
                    continue
            name = filters.get("name")
            if name is not None and name not in container.name:
                continue
            found.append(container)
        return found

    def run(self, image, *, name: str, labels: dict[str, str], **kwargs):
        # The daemon completes the create; the client sees only the timeout, and
        # the container is left created, not running.
        self.all.append(FakeContainer(name, labels, status="created"))
        assert self.run_error is not None
        raise self.run_error


class FakeDockerClient:
    def __init__(self, existing: list[FakeContainer]):
        self.containers = FakeContainers(existing)


def bystanders(name: str) -> list[FakeContainer]:
    """Containers that must survive: a foreign one named like ours, another run's
    container of the same name pattern, and one of this run's with a longer name
    the name filter alone would also match."""
    return [
        FakeContainer(f"{name}-copy", {}),
        FakeContainer(name.replace(RUN, "ffffffff"), {smoke.RUN_LABEL: "ffffffff"}),
        FakeContainer(f"{name}-sidecar", dict(LABELS)),
    ]


def test_failed_floci_start_removes_only_this_runs_container(monkeypatch):
    others = bystanders(FLOCI_NAME)
    client = FakeDockerClient(others)
    error = TimeoutError("timed out")

    class FailingFloci:
        def __init__(self, *, name: str, labels: dict[str, str], **kwargs):
            self.name = name
            self.labels = labels

        def start(self):
            client.containers.all.append(
                FakeContainer(self.name, self.labels, status="created")
            )
            raise error

    monkeypatch.setattr(smoke, "FlociContainer", FailingFloci)

    with pytest.raises(TimeoutError) as raised:
        smoke._start_floci(client, "omniload-lambda-net", RUN, dict(LABELS))

    assert raised.value is error
    (created,) = [c for c in client.containers.all if c.name == FLOCI_NAME]
    assert created.removed
    assert [c.removed for c in others] == [False, False, False]


def test_failed_emulator_start_removes_only_this_runs_container():
    others = bystanders(EMULATOR_NAME)
    client = FakeDockerClient(others)
    error = TimeoutError("timed out")
    client.containers.run_error = error
    stack = smoke.Stack(
        name="stack",
        function="function",
        source_bucket="source",
        destination_bucket="destination",
        status="CREATE_COMPLETE",
    )

    with pytest.raises(TimeoutError) as raised:
        smoke._start_emulator(
            client,
            "omniload-lambda-net",
            RUN,
            dict(LABELS),
            "http://floci:4566",
            stack,
        )

    assert raised.value is error
    (created,) = [c for c in client.containers.all if c.name == EMULATOR_NAME]
    assert created.removed
    assert [c.removed for c in others] == [False, False, False]


def test_failed_start_with_nothing_created_still_raises(monkeypatch):
    """When the daemon created nothing, the error still reaches the caller and
    nothing is removed, a foreign container already holding the exact name (the
    create's name conflict) included: the run label is what makes a container
    this run's, never the name alone."""
    name_holder = FakeContainer(FLOCI_NAME, {})
    others = [*bystanders(FLOCI_NAME), name_holder]
    client = FakeDockerClient(others)

    class RefusedFloci:
        def __init__(self, **kwargs):
            pass

        def start(self):
            raise ConnectionError("name already in use")

    monkeypatch.setattr(smoke, "FlociContainer", RefusedFloci)

    with pytest.raises(ConnectionError, match="name already in use"):
        smoke._start_floci(client, "omniload-lambda-net", RUN, dict(LABELS))
    assert [c.removed for c in others] == [False, False, False, False]
