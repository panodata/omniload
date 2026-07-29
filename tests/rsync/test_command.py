import os
import stat
import sys

import pytest

from omniload.source.rsync.command import (
    RsyncCommandBuilder,
    SubprocessRunner,
    filter_rules,
    is_recursive_glob,
)
from omniload.source.rsync.config import RsyncConfig, query_params
from omniload.source.rsync.error import RsyncTransferError

# --- config: query parsing -------------------------------------------------


def test_config_defaults():
    cfg = RsyncConfig.from_params(query_params("rsync://host/mod/x.csv"))
    assert cfg.binary == "oc-rsync"
    assert cfg.compress is True
    assert cfg.timeout is None
    assert cfg.extra_args == []


def test_config_parses_all_tuning():
    uri = (
        "rsync://host/mod/x.csv?binary=/opt/oc-rsync&compress=false"
        "&timeout=30&contimeout=5&bwlimit=2m&rsync_path=/usr/bin/rsync"
        "&staging_dir=/tmp/stage"
    )
    cfg = RsyncConfig.from_params(query_params(uri))
    assert cfg.binary == "/opt/oc-rsync"
    assert cfg.compress is False
    assert cfg.timeout == 30
    assert cfg.contimeout == 5
    assert cfg.bwlimit == "2m"
    assert cfg.rsync_path == "/usr/bin/rsync"
    assert cfg.staging_dir == "/tmp/stage"


@pytest.mark.parametrize(
    ("token", "expected"),
    [("", True), ("true", True), ("1", True), ("no", False), ("off", False)],
)
def test_config_bool_tokens(token, expected):
    cfg = RsyncConfig.from_params(query_params(f"rsync://h/m/x.csv?compress={token}"))
    assert cfg.compress is expected


def test_config_bool_rejects_garbage():
    with pytest.raises(ValueError):
        RsyncConfig.from_params(query_params("rsync://h/m/x.csv?compress=maybe"))


def test_config_extra_args_are_shell_split():
    uri = "rsync://h/m/x.csv?extra_args=--chmod%3DD755%20--numeric-ids"
    cfg = RsyncConfig.from_params(query_params(uri))
    assert cfg.extra_args == ["--chmod=D755", "--numeric-ids"]


@pytest.mark.parametrize(
    "binary",
    ["oc-rsync", "rsync", "/usr/local/bin/rsync", "/opt/oc-rsync"],
)
def test_config_allows_known_binaries(binary):
    cfg = RsyncConfig.from_params({"binary": binary})
    assert cfg.binary == binary


@pytest.mark.parametrize(
    "binary",
    ["/usr/bin/env", "bash", "/bin/sh", "python3"],
)
def test_config_rejects_unknown_binaries(binary):
    with pytest.raises(ValueError, match="not allowed"):
        RsyncConfig.from_params({"binary": binary})


# --- filter_rules ----------------------------------------------------------


@pytest.mark.parametrize(
    ("pattern", "expected"),
    [
        ("*.csv", ["+ *.csv", "- *"]),
        ("report.csv", ["+ report.csv", "- *"]),
        ("**/*.csv", ["+ */", "+ *.csv", "- *"]),
        ("logs/**/*.jsonl", ["+ */", "+ *.jsonl", "- *"]),
    ],
)
def test_filter_rules(pattern, expected):
    assert filter_rules(pattern) == expected


def test_filter_rules_rejects_empty_pattern():
    with pytest.raises(ValueError, match="empty"):
        filter_rules("")


@pytest.mark.parametrize(
    ("pattern", "recursive"),
    [("*.csv", False), ("report.csv", False), ("**/*.csv", True), ("a/b.csv", True)],
)
def test_is_recursive_glob(pattern, recursive):
    assert is_recursive_glob(pattern) is recursive


# --- RsyncCommandBuilder ---------------------------------------------------


def test_builder_orders_argv_deterministically():
    argv = (
        RsyncCommandBuilder("oc-rsync")
        .flag("-q")
        .flag("-t")
        .option("--timeout", "30")
        .extend(["--port", "873"])
        .filters(["+ *.csv", "- *"])
        .source("rsync://host:873/mod/data/")
        .destination("/tmp/stage")
        .build()
    )
    assert argv == [
        "oc-rsync",
        "-q",
        "-t",
        "--timeout",
        "30",
        "--port",
        "873",
        "--filter",
        "+ *.csv",
        "--filter",
        "- *",
        "rsync://host:873/mod/data/",
        "/tmp/stage/",
    ]


def test_builder_adds_trailing_slash_to_destination():
    argv = (
        RsyncCommandBuilder("oc-rsync")
        .source("host:/data/")
        .destination("/tmp/stage/")
        .build()
    )
    assert argv[-1] == "/tmp/stage/"


def test_builder_requires_both_source_and_destination():
    with pytest.raises(ValueError):
        RsyncCommandBuilder("oc-rsync").destination("/tmp").build()
    with pytest.raises(ValueError):
        RsyncCommandBuilder("oc-rsync").source("host:/x/").build()


# --- SubprocessRunner (POSIX only — shell scripts cannot run on Windows) ---

_posix_only = pytest.mark.skipif(sys.platform == "win32", reason="requires POSIX shell")


def _write_script(path: str, body: str) -> str:
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(body)
    os.chmod(path, os.stat(path).st_mode | stat.S_IEXEC)
    return path


@_posix_only
def test_subprocess_runner_success(tmp_path):
    script = _write_script(str(tmp_path / "ok.sh"), "#!/bin/sh\nexit 0\n")
    SubprocessRunner().run([script])


@_posix_only
def test_subprocess_runner_raises_with_stderr(tmp_path):
    script = _write_script(
        str(tmp_path / "fail.sh"),
        "#!/bin/sh\necho 'boom' 1>&2\nexit 23\n",
    )
    with pytest.raises(RsyncTransferError) as excinfo:
        SubprocessRunner().run([script])
    assert excinfo.value.returncode == 23
    assert "boom" in excinfo.value.stderr
    assert "status 23" in str(excinfo.value)


@_posix_only
def test_subprocess_runner_overlays_env(tmp_path):
    out = tmp_path / "env.txt"
    script = _write_script(
        str(tmp_path / "env.sh"),
        f"#!/bin/sh\nprintf '%s' \"$RSYNC_PASSWORD\" > {out}\nexit 0\n",
    )
    SubprocessRunner().run([script], env={"RSYNC_PASSWORD": "s3cret"})
    assert out.read_text() == "s3cret"


@_posix_only
def test_subprocess_runner_does_not_use_shell(tmp_path):
    marker = tmp_path / "created_by_injection"
    script = _write_script(str(tmp_path / "noop.sh"), "#!/bin/sh\nexit 0\n")
    # If a shell evaluated this, `; touch` would create the marker file.
    SubprocessRunner().run([script, f"; touch {marker}"])
    assert not marker.exists()
