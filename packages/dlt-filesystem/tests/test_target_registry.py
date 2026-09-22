"""Write-side registry: derivation, ambiguity rejection, and drift against the docs.

`test_target_local.py` covers URI/format resolution; this file covers the registry the
resolver reads and the claims the documentation makes about it.
"""

import re
from pathlib import Path

import pytest

from dlt_filesystem.source.format.registry import FORMAT_TO_READER
from dlt_filesystem.target.registry import (
    ADVERTISED_WRITE_FORMATS,
    ADVERTISED_WRITE_FORMATS_TEXT,
    FORMAT_TO_WRITER,
    WRITE_FORMATS,
    WRITER_REGISTRATIONS,
    WriterRegistration,
    _build_writer_map,
    supported_write_format_message,
    writer_for_format,
)

# The pages stay in the consumer's repository until this package has its own docs
# project, so a standalone checkout has nothing to compare against and says so.
DOCS = Path(__file__).resolve().parents[3] / "docs" / "supported-sources"
needs_docs = pytest.mark.skipif(
    not DOCS.is_dir(), reason=f"documentation tree not present at {DOCS}"
)


def test_write_formats_is_derived_from_the_registrations():
    """The map and the tuple cannot disagree, because one is built from the other."""
    registered = tuple(
        key for registration in WRITER_REGISTRATIONS for key in registration.format_keys
    )
    assert WRITE_FORMATS == registered
    assert set(FORMAT_TO_WRITER) == set(registered)


def test_only_the_first_key_of_a_registration_is_advertised():
    """Aliases route but are not named. `yml` and `yaml` are one writer under two
    extensions, so an error that listed both would read as two formats."""
    assert ADVERTISED_WRITE_FORMATS == tuple(
        registration.format_keys[0] for registration in WRITER_REGISTRATIONS
    )
    assert ADVERTISED_WRITE_FORMATS_TEXT == ", ".join(ADVERTISED_WRITE_FORMATS)
    assert set(ADVERTISED_WRITE_FORMATS) <= set(WRITE_FORMATS)
    assert "yml" in WRITE_FORMATS and "yml" not in ADVERTISED_WRITE_FORMATS


def test_no_registered_write_alias_is_advertised():
    """The same property the read side asserts, so neither registry can drift alone.

    Non-vacuity first: with no alias registered this would pass against an
    implementation that advertised every routing key.
    """
    aliases = {
        key
        for registration in WRITER_REGISTRATIONS
        for key in registration.format_keys[1:]
    }
    assert aliases, "no registration carries an alias, so this guard proves nothing"
    assert aliases.isdisjoint(ADVERTISED_WRITE_FORMATS)
    assert aliases < set(WRITE_FORMATS), "an alias must still route"


def test_an_alias_routes_to_the_same_writer_as_its_canonical_format():
    assert writer_for_format("yml") is writer_for_format("yaml")


def test_duplicate_format_registration_is_rejected():
    """Two writers claiming one format is a bug, not a last-one-wins override."""
    with pytest.raises(ValueError, match="Duplicate file format registration: csv"):
        _build_writer_map(
            (
                WriterRegistration(lambda path, rows: None, ("csv",)),
                WriterRegistration(lambda path, rows: None, ("csv",)),
            )
        )


@pytest.mark.parametrize("file_format", WRITE_FORMATS)
def test_every_registered_format_resolves_to_a_callable(file_format):
    assert callable(writer_for_format(file_format))


def test_unregistered_format_raises():
    with pytest.raises(NotImplementedError, match="Unsupported file format: bson"):
        writer_for_format("bson")


def test_every_write_format_is_also_a_read_format():
    """A file this package writes and cannot read back is a dead end, so the write set stays
    a subset of the read set. The reverse does not hold: several formats are read-only
    on purpose (see the registration comments)."""
    assert set(WRITE_FORMATS) <= set(FORMAT_TO_READER)


def test_the_supported_format_message_names_the_advertised_set():
    message = supported_write_format_message("txt")
    assert ADVERTISED_WRITE_FORMATS_TEXT in message
    assert "(got 'txt')" in message


# --- documentation drift -------------------------------------------------------------
#
# Every place the docs state which formats `file://` writes, with the parser that reads
# the claim back out. A new format then needs no edit here; a new *claim site* does, and
# a stale claim at an existing site fails.


def _matrix_write_formats() -> set[str]:
    """Formats whose Write column is ticked in the filesystem format matrix.

    A row is identified by its Read and Write cells both holding a tick or a cross,
    which the header and separator rows do not, rather than by the format-hint cell
    starting with `#`. Keying off the hint would silently ignore a ticked row whose
    hint were spelled differently, and an ignored row is the one case where this test
    could pass while the matrix is wrong.
    """
    marks = {"✅", "❌"}
    formats = set()
    for line in (DOCS / "filesystem.md").read_text().splitlines():
        cells = [cell.strip() for cell in line.split("|")]
        # | Format | Description | Extensions | Format hint | Read | Write |
        if len(cells) != 8 or cells[5] not in marks or cells[6] not in marks:
            continue
        if cells[6] == "✅":
            formats.add(cells[4].lstrip("#"))
    return formats


def _prose_formats(text: str) -> set[str]:
    """Format names out of a comma/and-separated prose list, backticks optional."""
    return {
        token.strip("`. ").lower()
        for token in re.split(r",|\band\b", text)
        if token.strip()
    }


PROSE_CLAIMS = [
    (
        "filesystem.md",
        r"Supported formats for write operations are currently ([^.]+)\.",
    ),
    (
        "file.md",
        r"Supported output formats\s+are ([^;]+);",
    ),
    (
        "index.md",
        r"Local files \(([^;]+) written;",
    ),
]


@needs_docs
def test_the_matrix_write_column_matches_the_registry():
    assert _matrix_write_formats() == set(ADVERTISED_WRITE_FORMATS)


@pytest.mark.parametrize(
    ("page", "pattern"), PROSE_CLAIMS, ids=[c[0] for c in PROSE_CLAIMS]
)
@needs_docs
def test_prose_claims_match_the_registry(page, pattern):
    """A page that enumerates the write set in prose must enumerate the current one."""
    text = (DOCS / page).read_text()
    match = re.search(pattern, text, re.MULTILINE | re.DOTALL)
    assert match, (
        f"{page}: the write-format claim this test pins is gone; drop the entry"
    )
    assert _prose_formats(match.group(1)) == set(ADVERTISED_WRITE_FORMATS)


@needs_docs
def test_the_pages_that_stopped_naming_the_write_set_have_not_regrown_it():
    """`yaml.md` and `xml.md` used to spell the write set out in passing, which is how
    both came to name a stale one. They link the matrix instead now. This is a tripwire
    for that one phrasing, not a general parser: a genuinely new claim site needs an
    entry in PROSE_CLAIMS."""
    stale = re.compile(r"writes\s+`csv`", re.IGNORECASE)
    offenders = [
        page.name
        for page in sorted(DOCS.glob("*.md"))
        if stale.search(page.read_text())
    ]
    assert offenders == []


def test_vortex_writer_without_the_extra_names_the_install(monkeypatch, tmp_path):
    """`vortex` stays advertised on the write side, like `yaml`, so a missing package
    has to surface as an install hint rather than a bare `ModuleNotFoundError`."""
    import sys

    from dlt_filesystem.source.error import MissingDecoderError

    for name in [m for m in sys.modules if m == "vortex" or m.startswith("vortex.")]:
        monkeypatch.delitem(sys.modules, name)
    monkeypatch.setitem(sys.modules, "vortex", None)
    with pytest.raises(
        MissingDecoderError, match=r"pip install 'dlt-filesystem\[vortex\]'"
    ):
        writer_for_format("vortex")(str(tmp_path / "out.vortex"), [{"id": 1}])
