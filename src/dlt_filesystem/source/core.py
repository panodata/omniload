from typing import Union

import dlt
from dlt.extract import DltResource, DltSource
from fsspec import AbstractFileSystem

from dlt_filesystem.source.adapter import filesystem, readers
from dlt_filesystem.source.error import UnsupportedEndpointError
from dlt_filesystem.source.format.registry import supported_file_format_message
from dlt_filesystem.source.model import FilesystemLocator, FilesystemReference
from dlt_filesystem.source.router import determine_endpoint


def resource_for_reader(ref: FilesystemReference) -> Union[DltSource, DltResource]:
    """Build the filesystem reader resource named by ``ref.reader_name``.

    Threads ``column_types`` into ``read_csv_headless`` and per-URI reader hints (e.g. XML's
    ``#tagname``) into a hint-consuming reader; every other reader is selected as-is.
    """

    # Establish filesystem and reader elements.
    filesystem_resource = filesystem(
        ref.bucket_url,
        ref.fs,
        file_glob=ref.file_glob,
        extract_content=False,
    )
    if ref.filesystem_incremental:
        filesystem_resource = filesystem_resource.with_name(
            ref.incremental_resource_name
        )
        filesystem_resource.apply_hints(
            incremental=dlt.sources.incremental("modification_date")
        )
    all_readers = readers(
        ref.bucket_url, ref.fs, file_glob=ref.file_glob
    ).with_resources(ref.reader_name)
    reader = all_readers.selected_resources[ref.reader_name]

    # Apply parameter bindings for certain readers.
    # TODO: Can this be generalized? Why not always loop in column_names into reader hints?
    if ref.reader_name == "read_csv_headless":
        column_names = list(ref.column_types.keys()) if ref.column_types else None
        reader = reader.bind(column_names=column_names, **ref.hints)
    else:
        reader = reader.bind(**ref.hints)

    # Connect and propagate elements.
    return filesystem_resource | reader


def infer_resource(
    fs: AbstractFileSystem, locator: FilesystemLocator
) -> Union[DltSource, DltResource]:
    """
    Infer dlt resource from fsspec filesystem, with reader.
    """

    # Decode into base url and url path / file glob, and apply sanity checks.
    locator.validate()

    # TODO: Naming things: Rename `determine_endpoint` to `infer_reader`.
    try:
        endpoint = determine_endpoint(locator.path, locator.file_glob)
    except UnsupportedEndpointError:
        raise ValueError(supported_file_format_message(locator.name)) from None

    # TODO: FilesystemLocator and FilesystemReference are somewhat redundant now. Refactor!
    #       => Bundle fs, locator and reader into another data class , then feed that to
    #       `resource_for_reader`.
    return resource_for_reader(
        FilesystemReference(
            fs=fs,
            bucket_url=locator.bucket_url,
            file_glob=locator.file_glob,
            reader_name=endpoint,
            hints=locator.hints,
            # TODO: Can `column_types` be looped into reader|writer hints instead?
            #       We believe it represents a special case handling for `csv_headless`.
            column_types=locator.options.params.get("column_types"),
        )
    )
