from typing import Any, Optional, Union

import dlt
from dlt.extract import DltResource, DltSource
from fsspec import AbstractFileSystem

from dlt_filesystem.source.adapter import filesystem, readers
from dlt_filesystem.source.error import UnsupportedEndpointError
from dlt_filesystem.source.format.registry import (
    reader_for_format,
    supported_file_format_message,
)
from dlt_filesystem.source.model import (
    FilesystemLocator,
    FilesystemReference,
    ResourceOptions,
)
from dlt_filesystem.source.router import determine_endpoint


def resource_for_reader(ref: FilesystemReference) -> Union[DltSource, DltResource]:
    """Build the filesystem reader resource named by ``ref.reader_name``.

    Threads ``column_types`` into ``read_csv_headless`` and per-URI reader hints (e.g. XML's
    ``#tagname``) into a hint-consuming reader; every other reader is selected as-is.
    """

    # Enforce concrete selections on this outer lister only. Piping it into the
    # selected reader replaces the reader source's inner parent, so discovery runs
    # exactly once and the check happens before any downstream incremental filter.
    filesystem_resource = filesystem(
        ref.bucket_url,
        ref.fs,
        file_glob=ref.file_glob,
        extract_content=False,
        require_file_match=ref.require_file_match,
        filesystem_incremental=ref.filesystem_incremental,
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
    reader_kwargs: dict[str, Any] = dict(ref.hints)
    if ref.reader_name in {"read_excel", "read_ods"}:
        # The filesystem lister yields pages of files. Keep one collision registry
        # bound to the reader so distinct worksheet names cannot normalize to the
        # same table even when the workbooks occur in different pages.
        reader_kwargs["worksheet_names"] = {}

    if ref.reader_name == "read_csv_headless":
        column_names = list(ref.column_types.keys()) if ref.column_types else None
        reader = reader.bind(column_names=column_names, **reader_kwargs)
    else:
        reader = reader.bind(**reader_kwargs)

    # Connect and propagate elements.
    return filesystem_resource | reader


def infer_resource(
    fs: AbstractFileSystem,
    locator: FilesystemLocator,
    options: Optional[ResourceOptions] = None,
) -> Union[DltSource, DltResource]:
    """
    Infer dlt resource from fsspec filesystem, with reader.

    Args:
        fs: The filesystem the connector built from its own connection arguments.
        locator: The parsed source URI.
        options: The resource options a `dlt_source` implementation declares by
            name in its own signature (`filesystem_incremental`, `column_types`,
            `reader_hints`). Omitted by callers that have none, which reads the
            same as a run that enabled nothing.
    """

    options = options or ResourceOptions()

    # Decode into base url and url path / file glob, and apply sanity checks.
    locator.validate()

    # TODO: Naming things: Rename `determine_endpoint` to `infer_reader`.
    try:
        # A `#format` the selection names wins over the file extension, whichever
        # carrier it rode in on. `determine_endpoint` reads it from the table form
        # itself; this also honours it on the URI, which for an HTTP URL is the
        # only place it can be written.
        endpoint = (
            reader_for_format(locator.format_hint)
            if locator.format_hint
            else determine_endpoint(locator.path, locator.file_glob)
        )
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
            # Require a match only when the locator's unparsed carrier names one
            # concrete file. This keeps wildcard discovery empty-safe.
            require_file_match=locator.require_file_match,
            # A URI fragment addresses one file, so it wins over a hint the source
            # derived for the whole run.
            hints={**(options.reader_hints or {}), **locator.hints},
            filesystem_incremental=options.filesystem_incremental,
            # TODO: Can `column_types` be looped into reader|writer hints instead?
            #       We believe it represents a special case handling for `csv_headless`.
            # The run value (`--columns`) wins; the URI query parameter remains the
            # fallback for callers that address it there.
            column_types=(
                options.column_types
                if options.column_types is not None
                else locator.options.params.get("column_types")
            ),
        )
    )
