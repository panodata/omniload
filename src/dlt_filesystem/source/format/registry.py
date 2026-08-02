from dataclasses import dataclass

from dlt_filesystem.source.error import UnsupportedEndpointError


@dataclass(frozen=True)
class ReaderRegistration:
    """Describe one reader transformer and the format keys that route to it."""

    reader_name: str
    format_keys: tuple[str, ...]
    transformer_order: int
    max_table_nesting: int = 0


# Readers that ship with the base install (core dependencies).
BASE_READER_REGISTRATIONS = (
    ReaderRegistration("read_csv", ("csv",), transformer_order=0),
    ReaderRegistration("read_csv_headless", ("csv_headless",), transformer_order=1),
    # `json` reads a whole document (object or array); `jsonl` stays on the strict
    # line-delimited reader. A `.json` file carrying line-delimited records is handled by
    # `read_json`'s fallback, so the two keys never need to route to each other.
    ReaderRegistration("read_json", ("json",), transformer_order=4),
    ReaderRegistration("read_jsonl", ("jsonl",), transformer_order=5),
    ReaderRegistration("read_ods", ("ods",), transformer_order=3),
    ReaderRegistration("read_orc", ("orc",), transformer_order=13),
    ReaderRegistration("read_parquet", ("parquet",), transformer_order=11),
    # bson is read-only: the file:// destination's WRITE_FORMATS is a separate tuple.
    ReaderRegistration("read_bson", ("bson",), transformer_order=6),
    ReaderRegistration("read_excel", ("xlsx",), transformer_order=2),
    ReaderRegistration("read_csv_duckdb", ("csv_duckdb",), transformer_order=12),
)

# Readers backed by the optional `iterable` extra (msgpack via iterabledata; cbor, xml and yaml
# via their own libraries directly -- see `format.iterable_codec`). They are routable so
# `.msgpack` / `#msgpack` resolve and the reader can raise a precise install hint, but they are
# advertised as supported only when their decoder is importable (see
# `supported_file_format_message`), so a base install never claims a format it can't actually
# read. Lexically sorted by format name.
ITERABLE_READER_REGISTRATIONS = (
    ReaderRegistration("read_cbor", ("cbor",), transformer_order=8),
    ReaderRegistration("read_msgpack", ("msgpack",), transformer_order=7),
    ReaderRegistration("read_xml", ("xml",), transformer_order=9),
    # `.yml` is the same format as `.yaml`; it routes to the same reader (which decodes as
    # "yaml"). Only the canonical "yaml" is advertised (installed_iterable_formats reads the
    # codec registry), so this alias just lets a `.yml` extension / `#yml` hint resolve.
    ReaderRegistration("read_yaml", ("yaml", "yml"), transformer_order=10),
)

READER_REGISTRATIONS = tuple(
    sorted(
        BASE_READER_REGISTRATIONS + ITERABLE_READER_REGISTRATIONS,
        key=lambda registration: registration.transformer_order,
    )
)


def _build_format_map(
    registrations: tuple[ReaderRegistration, ...],
) -> dict[str, str]:
    """Build a format-to-reader map, rejecting ambiguous format keys."""
    format_map: dict[str, str] = {}
    for registration in registrations:
        for format_key in registration.format_keys:
            if format_key in format_map:
                raise ValueError(f"Duplicate file format registration: {format_key}")
            format_map[format_key] = registration.reader_name
    return format_map


BASE_FILE_FORMATS = _build_format_map(BASE_READER_REGISTRATIONS)
ITERABLE_FILE_FORMATS = _build_format_map(ITERABLE_READER_REGISTRATIONS)
FORMAT_TO_READER = _build_format_map(
    BASE_READER_REGISTRATIONS + ITERABLE_READER_REGISTRATIONS
)
SUPPORTED_FILE_FORMATS = tuple(FORMAT_TO_READER)


def reader_for_format(file_format: str) -> str:
    """Return the reader-function name for ``file_format``, or raise ``UnsupportedEndpointError``."""
    try:
        return FORMAT_TO_READER[file_format]
    except KeyError as e:
        raise UnsupportedEndpointError(f"Unsupported file format: {file_format}") from e


def advertised_file_formats() -> tuple[str, ...]:
    """Formats to name in user-facing "supported formats" errors.

    Base formats always ship. Iterable-extra formats are appended only when their decoder is
    importable, so a base install doesn't advertise a format that would fail with an install
    hint (the reader still routes such a format and raises that hint if it is used).
    """
    from dlt_filesystem.source.format.iterable_codec import (
        installed_iterable_formats,
    )

    return tuple(BASE_FILE_FORMATS) + installed_iterable_formats()


def supported_file_format_message(source_name: str) -> str:
    """Build the "only supports file formats: ..." error message for ``source_name``."""
    formats = ", ".join(advertised_file_formats())
    return f"{source_name} Source only supports file formats: {formats}"
