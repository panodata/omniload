"""The writers themselves: what lands on disk, independent of the URI plumbing."""

import datetime
import decimal
import json
import os
import subprocess
import sys

import pytest
import yaml

from dlt_filesystem.source.error import MissingDecoderError
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


def test_write_yaml_emits_one_sequence_document(tmp_path):
    """One document holding a list, not a `---`-separated document per row.

    Both shapes round-trip through `read_yaml`, which is why this asserts the document
    count rather than the rows: a stream would load the same records and still be a
    file no other writer here produces. Multi-row, because a single row cannot tell the
    two apart.
    """
    path = tmp_path / "out.yaml"
    writer_for_format("yaml")(str(path), ROWS)

    text = path.read_text(encoding="utf-8")
    documents = list(yaml.safe_load_all(text))
    assert len(documents) == 1
    assert documents[0] == ROWS
    assert "---" not in text


def test_write_yaml_keeps_the_column_order_of_the_row(tmp_path):
    """`sort_keys=False`, so an export reads in the order the load produced, the same
    promise `_column_union` makes for the writers that carry a header."""
    path = tmp_path / "out.yaml"
    writer_for_format("yaml")(str(path), [{"name": "Zoe", "id": 1, "age": 2}])

    assert path.read_text(encoding="utf-8").splitlines() == [
        "- name: Zoe",
        "  id: 1",
        "  age: 2",
    ]


def test_write_yaml_of_no_rows_is_an_empty_sequence(tmp_path):
    """An empty load writes an explicit empty document, not an empty file.

    Both load as zero rows here -- `_yaml_eager_decode` returns early on empty input --
    so this pins what the file *says* rather than what this reader does with it: `[]` is
    a document every YAML parser reads as an empty sequence, where an empty file is a
    parser-by-parser question.
    """
    path = tmp_path / "out.yaml"
    writer_for_format("yaml")(str(path), [])

    assert path.read_text(encoding="utf-8") == "[]\n"
    assert yaml.safe_load(path.read_text(encoding="utf-8")) == []


def test_write_yaml_spells_native_types_the_way_the_json_writers_do(tmp_path):
    """`--loader-file-format parquet` is the one path that hands a writer native values,
    and PyYAML's safe dumper raises on half of them. A `Decimal` writes as the string
    dlt's own serializer produces, keeping a scale a float would drop; the types YAML
    knows keep their native spelling, so a datetime reads back as a datetime.
    """
    path = tmp_path / "out.yaml"
    writer_for_format("yaml")(
        str(path),
        [
            {
                "price": decimal.Decimal("1.50"),
                "at": datetime.time(9, 30),
                "blob": b"hi",
                "ts": datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc),
            }
        ],
    )

    text = path.read_text(encoding="utf-8")
    assert "!!binary" in text, "bytes keep YAML's own binary tag rather than a repr"

    # Read with PyYAML, so these are claims about the file. Through omniload's own
    # reader the `!!binary` becomes the base64 string `'aGk='`, which is what `.json`
    # and `.jsonl` round-trip the same value to, by a different on-disk spelling.
    loaded = yaml.safe_load(text)[0]
    # dlt's spelling for the two types PyYAML would refuse, so a decimal keeps a scale
    # a float would drop and a time keeps its ISO form.
    assert loaded["price"] == "1.50"
    assert loaded["at"] == "09:30:00"
    # ... and the two it does know keep their native YAML types, so a datetime reads
    # back as a datetime rather than as text.
    assert loaded["ts"] == datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc)
    assert loaded["blob"] == b"hi"


def test_write_yaml_without_pyyaml_names_the_install(tmp_path, monkeypatch):
    """The same install hint the reader gives, rather than a bare ImportError.

    PyYAML is an unconditional `dlt` dependency, so this branch is unreachable through
    a real install; it exists so the writer keeps the contract `yaml.md` states for the
    format as a whole.
    """
    monkeypatch.setitem(sys.modules, "yaml", None)

    with pytest.raises(MissingDecoderError) as exc:
        writer_for_format("yaml")(str(tmp_path / "out.yaml"), ROWS)

    assert "omniload[iterable]" in str(exc.value)


def test_write_orc_preserves_sparse_rows_and_column_order(tmp_path):
    from pyarrow import orc

    path = tmp_path / "out.orc"
    writer_for_format("orc")(str(path), ROWS)

    table = orc.ORCFile(path).read()
    assert table.column_names == ["id", "name", "note"]
    assert table.to_pylist() == [
        {"id": 1, "name": "Zoë", "note": None},
        {"id": 2, "name": "Ōtautahi", "note": "late column"},
    ]


def test_write_orc_of_no_rows_is_valid(tmp_path):
    from pyarrow import orc

    path = tmp_path / "empty.orc"
    writer_for_format("orc")(str(path), [])

    assert orc.ORCFile(path).read().num_rows == 0


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
