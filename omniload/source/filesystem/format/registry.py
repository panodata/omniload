from omniload.source.filesystem.error import UnsupportedEndpointError

FORMAT_TO_READER: dict[str, str] = {
    "csv": "read_csv",
    "csv_headless": "read_csv_headless",
    "jsonl": "read_jsonl",
    "parquet": "read_parquet",
}
SUPPORTED_FILE_FORMATS = tuple(FORMAT_TO_READER)
SUPPORTED_FILE_FORMATS_TEXT = ", ".join(SUPPORTED_FILE_FORMATS)


def reader_for_format(file_format: str) -> str:
    try:
        return FORMAT_TO_READER[file_format]
    except KeyError as e:
        raise UnsupportedEndpointError(f"Unsupported file format: {file_format}") from e


def supported_file_format_message(source_name: str) -> str:
    return f"{source_name} Source only supports file formats: {SUPPORTED_FILE_FORMATS_TEXT}"
