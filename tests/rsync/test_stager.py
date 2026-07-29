import os
import stat
import sys

import pytest

from omniload.source.rsync.config import RsyncConfig
from omniload.source.rsync.stager import STAGING_PREFIX, FileSelection, RsyncStager
from tests.rsync.support import FakeTransport, RecordingRunner

# --- FileSelection ---------------------------------------------------------


@pytest.mark.parametrize(
    ("remote_path", "root", "pattern"),
    [
        ("/data/exports/*.csv", "/data/exports", "*.csv"),
        ("/data/**/*.csv", "/data", "**/*.csv"),
        ("mod/reports/report.csv", "mod/reports", "report.csv"),
        ("/srv/one.jsonl", "/srv", "one.jsonl"),
    ],
)
def test_file_selection_splits_root_and_pattern(remote_path, root, pattern):
    sel = FileSelection.from_remote_path(remote_path)
    assert sel.root == root
    assert sel.pattern == pattern


# --- staging directory -----------------------------------------------------


def test_staging_dir_is_deterministic_and_created(tmp_path):
    config = RsyncConfig(staging_dir=str(tmp_path))
    stager = RsyncStager(FakeTransport(), config, RecordingRunner())
    sel = FileSelection.from_remote_path("/data/*.csv")

    first = stager.staging_dir(sel)
    second = stager.staging_dir(sel)

    assert first == second
    assert os.path.isdir(first)
    assert first.startswith(str(tmp_path))


def test_staging_dir_differs_per_selection(tmp_path):
    config = RsyncConfig(staging_dir=str(tmp_path))
    stager = RsyncStager(FakeTransport(), config, RecordingRunner())
    a = stager.staging_dir(FileSelection.from_remote_path("/data/*.csv"))
    b = stager.staging_dir(FileSelection.from_remote_path("/data/*.jsonl"))
    assert a != b


def test_staging_dir_defaults_under_tempdir_prefix():
    config = RsyncConfig()
    stager = RsyncStager(FakeTransport(), config, RecordingRunner())
    path = stager.staging_dir(FileSelection.from_remote_path("/data/*.csv"))
    assert STAGING_PREFIX in path


@pytest.mark.skipif(sys.platform == "win32", reason="NTFS has no POSIX mode bits")
def test_staging_dir_has_owner_only_permissions(tmp_path):
    config = RsyncConfig(staging_dir=str(tmp_path))
    stager = RsyncStager(FakeTransport(), config, RecordingRunner())
    path = stager.staging_dir(FileSelection.from_remote_path("/data/*.csv"))
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o700


# --- command composition ---------------------------------------------------


def test_stage_composes_base_flags_transport_filters_and_paths(tmp_path):
    runner = RecordingRunner()
    transport = FakeTransport(args=("--port", "873"), env={"RSYNC_PASSWORD": "x"})
    config = RsyncConfig(staging_dir=str(tmp_path))
    stager = RsyncStager(transport, config, runner)

    destination = stager.stage(FileSelection.from_remote_path("/data/*.csv"))

    assert runner.calls == 1
    argv = runner.argv
    assert argv[0] == "oc-rsync"
    assert "-r" not in argv
    for flag in ("-q", "-t", "-L", "--partial", "-z"):
        assert flag in argv
    assert argv[argv.index("--port") + 1] == "873"
    assert "--filter" in argv
    assert "+ *.csv" in argv
    assert "- *" in argv
    assert argv[-2] == "fake:/data/"
    assert argv[-1] == f"{destination}/"
    assert runner.env == {"RSYNC_PASSWORD": "x"}


def test_stage_adds_recursive_flag_for_recursive_glob(tmp_path):
    runner = RecordingRunner()
    stager = RsyncStager(
        FakeTransport(), RsyncConfig(staging_dir=str(tmp_path)), runner
    )
    stager.stage(FileSelection.from_remote_path("/data/**/*.csv"))
    assert "-r" in runner.argv
    assert "+ */" in runner.argv


def test_stage_omits_compress_when_disabled(tmp_path):
    runner = RecordingRunner()
    stager = RsyncStager(
        FakeTransport(), RsyncConfig(staging_dir=str(tmp_path), compress=False), runner
    )
    stager.stage(FileSelection.from_remote_path("/data/*.csv"))
    assert "-z" not in runner.argv


def test_stage_threads_tuning_options(tmp_path):
    runner = RecordingRunner()
    config = RsyncConfig(
        staging_dir=str(tmp_path),
        timeout=30,
        contimeout=5,
        bwlimit="2m",
        rsync_path="/usr/bin/rsync",
        extra_args=["--numeric-ids"],
    )
    stager = RsyncStager(FakeTransport(), config, runner)
    stager.stage(FileSelection.from_remote_path("/data/*.csv"))
    argv = runner.argv
    assert argv[argv.index("--timeout") + 1] == "30"
    assert argv[argv.index("--contimeout") + 1] == "5"
    assert argv[argv.index("--bwlimit") + 1] == "2m"
    assert argv[argv.index("--rsync-path") + 1] == "/usr/bin/rsync"
    assert "--numeric-ids" in argv
