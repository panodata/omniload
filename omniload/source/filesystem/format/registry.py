from omniload.source.filesystem.error import UnsupportedEndpointError

# Formats whose reader ships with the base install (core dependencies).
BASE_FILE_FORMATS: dict[str, str] = {
    "csv": "read_csv",
    "csv_headless": "read_csv_headless",
    "jsonl": "read_jsonl",
    "ods": "read_ods",
    "parquet": "read_parquet",
    # bson is read-only: the file:// destination's WRITE_FORMATS is a separate tuple.
    "bson": "read_bson",
    "xlsx": "read_excel",
}

# Formats backed by the optional `iterable` extra (msgpack via iterabledata; cbor, xml and yaml
# via their own libraries directly -- see `format.iterable_codec`). They are routable so
# `.msgpack` / `#msgpack` resolve and the reader can raise a precise install hint, but they are
# advertised as supported only when their decoder is importable (see
# `supported_file_format_message`), so a base install never claims a format it can't actually
# read. Lexically sorted by format name.
ITERABLE_FILE_FORMATS: dict[str, str] = {
    "cbor": "read_cbor",
    "msgpack": "read_msgpack",
    "xml": "read_xml",
    "yaml": "read_yaml",
    # `.yml` is the same format as `.yaml`; it routes to the same reader (which decodes as
    # "yaml"). Only the canonical "yaml" is advertised (installed_iterable_formats reads the
    # codec registry), so this alias just lets a `.yml` extension / `#yml` hint resolve.
    "yml": "read_yaml",
}

FORMAT_TO_READER: dict[str, str] = {**BASE_FILE_FORMATS, **ITERABLE_FILE_FORMATS}
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
    from omniload.source.filesystem.format.iterable_codec import (
        installed_iterable_formats,
    )

    return tuple(BASE_FILE_FORMATS) + installed_iterable_formats()


def supported_file_format_message(source_name: str) -> str:
    """Build the "only supports file formats: ..." error message for ``source_name``."""
    formats = ", ".join(advertised_file_formats())
    return f"{source_name} Source only supports file formats: {formats}"
