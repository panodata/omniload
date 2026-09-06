from dataclasses import dataclass
from typing import Callable

from dlt_filesystem.target.writer import (
    write_csv,
    write_json,
    write_jsonl,
    write_orc,
    write_parquet,
    write_yaml,
)

Writer = Callable[[str, list[dict]], None]


@dataclass(frozen=True)
class WriterRegistration:
    """Describe one writer and the format keys that route to it.

    The write-side twin of ``source.format.registry.ReaderRegistration``. It carries a
    callable rather than a name because a writer is called directly, where a reader name
    is resolved against a dlt source's attributes at build time.
    """

    writer: Writer
    format_keys: tuple[str, ...]


# Writers that ship with the base install. Declaration order is what the
# supported-format error message lists, so it is pinned here (lexical) rather than left
# to whatever a dict literal happens to iterate.
#
# The write side registers fewer formats than the read side, and the gap is deliberate
# rather than pending: `csv_headless` is a read-only concept (parsing a header-less CSV;
# writing always emits a header), `csv_duckdb` is a reader choice for the same bytes CSV
# already writes, and `bson` / `xml` are read-only for reasons the docs give per format.
WRITER_REGISTRATIONS: tuple[WriterRegistration, ...] = (
    WriterRegistration(write_csv, ("csv",)),
    # `json` writes one array document and `jsonl` one record per line, matching the
    # split the readers already make: a `.json` file is read as a single document.
    WriterRegistration(write_json, ("json",)),
    WriterRegistration(write_jsonl, ("jsonl",)),
    WriterRegistration(write_orc, ("orc",)),
    WriterRegistration(write_parquet, ("parquet",)),
    # `yaml` is registered unconditionally, where the *reader* lists it under the
    # optional `iterable` extra. PyYAML is not actually optional in this dependency
    # set: `dlt` and `google-ads` both require it outright, so it arrives with any
    # install and the reader's `find_spec` gate never fires for it. `write_yaml` still
    # raises the install hint the reader gives if the import ever does fail, so the
    # contract `docs/supported-sources/yaml.md` states holds in both directions.
    #
    # `yml` is the same format under the other common extension, and is an alias here
    # for the same reason it is one on the read side: the format is chosen from the
    # path, so a destination spelled `out.yml` must resolve or it is rejected as an
    # unsupported format while `out.yaml` writes.
    WriterRegistration(write_yaml, ("yaml", "yml")),
)


def _build_writer_map(
    registrations: tuple[WriterRegistration, ...],
) -> dict[str, Writer]:
    """Build a format-to-writer map, rejecting ambiguous format keys.

    Mirrors ``source.format.registry._build_format_map`` rather than reusing it: that
    one is typed ``tuple[ReaderRegistration, ...] -> dict[str, str]`` and maps a format
    to a reader *name*, where the write side routes straight to a callable.
    """
    writer_map: dict[str, Writer] = {}
    for registration in registrations:
        for format_key in registration.format_keys:
            if format_key in writer_map:
                raise ValueError(f"Duplicate file format registration: {format_key}")
            writer_map[format_key] = registration.writer
    return writer_map


FORMAT_TO_WRITER = _build_writer_map(WRITER_REGISTRATIONS)

#: Every key a destination path or ``#hint`` may name, aliases included.
WRITE_FORMATS = tuple(FORMAT_TO_WRITER)

#: What error messages and the documentation name: the first key of each registration,
#: one entry per writer. An alias routes but is not advertised, so ``yml`` does not read
#: as a second format alongside ``yaml`` -- the same split the read side makes, where
#: `FORMAT_TO_READER` carries `yml` and `advertised_file_formats()` does not.
ADVERTISED_WRITE_FORMATS = tuple(
    registration.format_keys[0] for registration in WRITER_REGISTRATIONS
)
ADVERTISED_WRITE_FORMATS_TEXT = ", ".join(ADVERTISED_WRITE_FORMATS)


def writer_for_format(file_format: str) -> Writer:
    try:
        return FORMAT_TO_WRITER[file_format]
    except KeyError as e:
        raise NotImplementedError(f"Unsupported file format: {file_format}") from e


def supported_write_format_message(file_format: str | None = None) -> str:
    got = f" (got '{file_format}')" if file_format else ""
    return (
        "Local file Destination only supports file formats: "
        f"{ADVERTISED_WRITE_FORMATS_TEXT}{got}"
    )


def pinned_write_format_message(pinned_format: str, file_format: str) -> str:
    """Build the error for a destination whose scheme already names its format."""
    return (
        f"A '{pinned_format}' destination only writes {pinned_format} files, and this "
        f"one names '{file_format}'. Use a 'file://' destination for other formats."
    )
