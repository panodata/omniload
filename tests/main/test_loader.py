"""Read-back tests for the dlt loader files ``dlt_filesystem.util.loader`` consumes.

The fixtures are written by dlt itself rather than by hand, because the thing under test
is agreement with dlt's own naming: which extension it gives each loader file format, and
when it appends ``.gz``. Hand-rolled fixtures would agree with whatever this module
believes and could not catch a change in dlt.
"""

import os
import pathlib
import tempfile
from contextlib import contextmanager

import pytest
from dlt.common.data_writers.writers import TLoaderFileFormat
from dlt_filesystem.util.loader import (
    UnsupportedLoaderFileFormat,
    factory,
    load_dlt_file,
)

TESTDATA = [
    {"name": "Jhon", "email": "jhon@acme.com"},
    {"name": "Lisa", "email": "lisa@acme.com"},
]

#: label -> (loader_file_format, compression disabled, expected file name suffix).
#: Both compressed cases matter: dlt gzips CSV as well as JSONL by default, so the
#: compressed names are the pair a content sniffer cannot tell apart.
LOADER_FILES: dict[str, tuple[TLoaderFileFormat, bool, str]] = {
    "jsonl": ("jsonl", False, ".jsonl.gz"),
    "csv": ("csv", False, ".csv.gz"),
    "parquet": ("parquet", False, ".parquet"),
    "jsonl-uncompressed": ("jsonl", True, ".jsonl"),
    "csv-uncompressed": ("csv", True, ".csv"),
}


@contextmanager
def _compression(disable: bool):
    """Set dlt's compression switch for one load, then put the environment back.

    Leaving it set would follow the process out of the fixture and, under xdist, reach
    every later test on the same worker: an uncompressed CSV intermediate is exactly what
    some of them are written to prove does not happen by default.
    """
    key = "DATA_WRITER__DISABLE_COMPRESSION"
    previous = os.environ.get(key)
    if disable:
        os.environ[key] = "true"
    else:
        os.environ.pop(key, None)
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = previous


def _write_with_dlt(
    base: pathlib.Path,
    loader_file_format: TLoaderFileFormat,
    disable_compression: bool = False,
) -> str:
    """Run one dlt load into ``base`` and return its single data file."""
    import dlt

    bucket = tempfile.mkdtemp(dir=base)
    with _compression(disable_compression):
        pipeline = dlt.pipeline(
            pipeline_name=f"loader_fixture_{loader_file_format}_{disable_compression}",
            destination=dlt.destinations.filesystem(
                bucket_url=pathlib.Path(bucket).as_uri()
            ),
            dataset_name="public",
            pipelines_dir=tempfile.mkdtemp(dir=base),
        )
        pipeline.run(
            TESTDATA, table_name="people", loader_file_format=loader_file_format
        )

    table_dir = os.path.join(bucket, "public", "people")
    names = sorted(os.listdir(table_dir))
    assert len(names) == 1, f"expected one data file, got {names}"
    return os.path.join(table_dir, names[0])


@pytest.fixture(scope="session")
def loader_files(tmp_path_factory):
    """One dlt-written data file per format, keyed by the labels in ``LOADER_FILES``."""
    base = tmp_path_factory.mktemp("loader_files")
    return {
        label: _write_with_dlt(base, fmt, disable_compression)
        for label, (fmt, disable_compression, _) in LOADER_FILES.items()
    }


def _payload(rows):
    """Rows without dlt's bookkeeping columns, which the caller strips itself."""
    return [{k: v for k, v in row.items() if not k.startswith("_dlt_")} for row in rows]


@pytest.mark.parametrize("label", list(LOADER_FILES))
def test_loader_reads_every_format_dlt_writes(loader_files, label):
    """Both compressed formats included, which is what the previous ``file``-based
    routing could not do: it sent every gzip file to the JSONL reader, so a CSV load
    failed with a raw JSON decode error."""
    path = loader_files[label]
    assert path.endswith(LOADER_FILES[label][2]), path

    assert _payload(load_dlt_file(path)) == TESTDATA


def test_loader_needs_no_external_file_command(loader_files, monkeypatch):
    """The loader must not shell out: ``file`` is absent on Windows and its output
    varies by version. An emptied ``PATH`` makes a ``file`` subprocess raise
    ``FileNotFoundError``, which is how this asserts something. Emptied, not deleted:
    with ``PATH`` unset, the subprocess falls back to a default search path and finds
    the command anyway, so the test would pass against a loader that still shells out.
    """
    monkeypatch.setenv("PATH", "")

    for label in LOADER_FILES:
        assert _payload(load_dlt_file(loader_files[label])) == TESTDATA


def test_loader_refuses_a_format_it_cannot_read(loader_files):
    """``insert_values`` is a dlt loader file format with no row-shaped reading here.
    It used to be gzipped, matched the gzip branch and was misread as JSONL, so it
    failed with a JSON decode error naming a column position. Now it is named."""
    unreadable = str(
        pathlib.Path(loader_files["jsonl"]).with_name("x.insert_values.gz")
    )

    with pytest.raises(UnsupportedLoaderFileFormat, match="insert_values"):
        factory(unreadable)


def test_loader_refuses_a_file_with_no_extension(loader_files):
    """dlt names every loader file with its format, so a name without one is not a dlt
    loader file. Refusing beats guessing: the two compressed formats are identical
    bytes, so a guess would silently pick the wrong reader for one of them."""
    nameless = str(pathlib.Path(loader_files["jsonl"]).with_name("no-extension-here"))

    with pytest.raises(UnsupportedLoaderFileFormat, match="no-extension-here"):
        factory(nameless)


def test_loader_never_invokes_a_subprocess(loader_files, monkeypatch):
    """Stronger than the PATH check: an emptied PATH proves the loader does not depend
    on the command succeeding, not that it never runs one (an absolute path bypasses
    PATH, and a caught FileNotFoundError could hide a fallback). This proves it."""
    import subprocess

    calls = []
    monkeypatch.setattr(subprocess, "run", lambda *a, **kw: calls.append(a))
    monkeypatch.setattr(subprocess, "Popen", lambda *a, **kw: calls.append(a))

    for label in LOADER_FILES:
        assert _payload(load_dlt_file(loader_files[label])) == TESTDATA

    assert calls == []


@pytest.mark.parametrize(
    "setting",
    [
        "DATA_WRITER__DELIMITER",
        # The staged files are written in the normalize stage, so this spelling reaches
        # the writer as well. Resolving the dialect without that section reads only the
        # unscoped one and quietly parses the file with the wrong delimiter.
        "NORMALIZE__DATA_WRITER__DELIMITER",
    ],
)
def test_loader_reads_the_csv_delimiter_dlt_wrote(tmp_path, monkeypatch, setting):
    """dlt's CSV writer takes its delimiter from configuration. Reading with a hardcoded
    comma parses every row into one composite column named by the whole header line, so
    the load reports success and the rows are unusable."""
    monkeypatch.setenv(setting, "|")

    path = _write_with_dlt(tmp_path, "csv")

    assert _payload(load_dlt_file(path)) == TESTDATA


def test_loader_refuses_a_csv_line_terminator_it_cannot_read(tmp_path, monkeypatch):
    """A terminator csv does not recognise leaves no newline in the file, so
    ``csv.DictReader`` sees one unterminated record and yields nothing: a load of zero
    rows rather than an error, which is how an export silently loses everything.
    Splitting the text on the terminator instead would have to know where the quoted
    fields are, and a value containing the terminator would be truncated in silence,
    so this refuses rather than trading one silent wrong answer for another."""
    monkeypatch.setenv("DATA_WRITER__LINETERMINATOR", "|")

    path = _write_with_dlt(tmp_path, "csv")

    with pytest.raises(UnsupportedLoaderFileFormat, match="line terminator"):
        list(load_dlt_file(path))


@pytest.mark.parametrize("terminator", ["\r\n", "\r"])
def test_loader_reads_the_line_endings_csv_ends_a_record_on(
    tmp_path, monkeypatch, terminator
):
    """csv recognises all three spellings of a line ending when reading, whatever it was
    told to write, so these round-trip rather than being refused with the terminators it
    genuinely cannot read."""
    monkeypatch.setenv("DATA_WRITER__LINETERMINATOR", terminator)

    path = _write_with_dlt(tmp_path, "csv")

    assert _payload(load_dlt_file(path)) == TESTDATA


def test_loader_refuses_headerless_csv(tmp_path, monkeypatch):
    """The names live in the dlt schema, not the file. Refusing beats naming the columns
    after the first row of data."""
    monkeypatch.setenv("DATA_WRITER__INCLUDE_HEADER", "false")

    path = _write_with_dlt(tmp_path, "csv")

    with pytest.raises(UnsupportedLoaderFileFormat, match="without a header"):
        list(load_dlt_file(path))
