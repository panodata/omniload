"""The writers themselves: what lands on disk, independent of the URI plumbing."""

import json
import os
import subprocess
import sys

import pytest

from dlt_filesystem.target.registry import WRITE_FORMATS, writer_for_format

ROWS = [{"id": 1, "name": "Zoë"}, {"id": 2, "name": "Ōtautahi", "note": "late column"}]

# Writes each format from a child interpreter and reports the encoding that child's
# text handles would default to, so the parent can tell "the locale was forced" from
# "the platform ignored the request".
_CHILD = """
import sys, tempfile
from dlt_filesystem.target.registry import writer_for_format

# What `open()` in text mode would actually use, asked of a real handle rather than
# of the locale module: `locale.getencoding()` is 3.11+, and this package supports
# 3.10, where the equivalent spelling is `getpreferredencoding(False)`.
with tempfile.TemporaryFile("w") as probe:
    print(probe.encoding)

out_dir, rows = sys.argv[1], __import__("json").loads(sys.argv[2])
for file_format in sys.argv[3].split(","):
    writer_for_format(file_format)(f"{out_dir}/out.{file_format}", rows)
"""


def test_write_json_emits_one_array_document(tmp_path):
    """One document, not a concatenated stream: `read_json` parses the whole file as a
    single value and only falls back to line-delimited parsing when that fails."""
    path = tmp_path / "out.json"
    writer_for_format("json")(str(path), ROWS)

    assert json.loads(path.read_text(encoding="utf-8")) == ROWS


def test_write_json_of_no_rows_is_an_empty_array(tmp_path):
    path = tmp_path / "out.json"
    writer_for_format("json")(str(path), [])

    assert path.read_text(encoding="utf-8") == "[]"


def test_writers_emit_utf8_whatever_the_locale(tmp_path):
    """The readers decode as UTF-8 unconditionally (`json.loadb` accepts nothing else,
    Polars defaults to it), so a locale-encoded export would not read back on the
    machine that wrote it. A text handle's encoding is fixed at interpreter startup, so
    forcing a non-UTF-8 default needs a child process rather than a monkeypatch.
    """
    text_formats = [f for f in WRITE_FORMATS if f not in {"orc", "parquet"}]
    child = subprocess.run(  # noqa: S603  # trusted: sys.executable + a fixed code string
        [
            sys.executable,
            "-c",
            _CHILD,
            str(tmp_path),
            json.dumps(ROWS),
            ",".join(text_formats),
        ],
        env={
            **os.environ,
            "LC_ALL": "C",
            "PYTHONUTF8": "0",
            "PYTHONCOERCECLOCALE": "0",
        },
        capture_output=True,
        text=True,
    )
    assert child.returncode == 0, child.stderr
    if child.stdout.strip().lower().replace("-", "") in {"utf8", "cp65001"}:
        pytest.skip(f"platform kept a UTF-8 default: {child.stdout.strip()}")

    for file_format in text_formats:
        written = (tmp_path / f"out.{file_format}").read_bytes().decode("utf-8")
        assert "Zoë" in written and "Ōtautahi" in written, file_format
