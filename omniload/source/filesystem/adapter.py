# Copyright 2022-2025 ScaleVector
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Reads files in s3, gs or azure buckets using fsspec and provides convenience resources for chunked reading of various file formats"""

from typing import Any, Iterator, List, Optional, Tuple, Union

import dlt
from dlt.common.typing import TDataItems
from dlt.extract import DltSource
from dlt.sources import DltResource
from dlt.sources.credentials import FileSystemCredentials
from dlt.sources.filesystem import FileItem, FileItemDict, fsspec_filesystem, glob_files
from fsspec import AbstractFileSystem

from omniload.source.filesystem.format.readers import (
    ReadersSource,
    read_bson,
    read_cbor,
    read_csv,
    read_csv_duckdb,
    read_csv_headless,
    read_jsonl,
    read_msgpack,
    read_parquet,
)

from .model import FilesystemConfigurationResource, FilesystemReference


@dlt.source(_impl_cls=ReadersSource, spec=FilesystemConfigurationResource)
def readers(
    bucket_url: str,
    credentials: Union[FileSystemCredentials, AbstractFileSystem],
    file_glob: Optional[str] = "*",
) -> Tuple[DltResource, ...]:
    """This source provides a few resources that are chunked file readers. Readers can be further parametrized before use
       read_csv(chunksize, **pandas_kwargs)
       read_jsonl(chunksize)
       read_parquet(chunksize)

    Args:
        bucket_url (str): The url to the bucket.
        credentials (FileSystemCredentials | AbstractFilesystem): The credentials to the filesystem of fsspec `AbstractFilesystem` instance.
        file_glob (str, optional): The filter to apply to the files in glob format. by default lists all files in bucket_url non-recursively
    """
    filesystem_resource = filesystem(bucket_url, credentials, file_glob=file_glob)

    # NOTE: incremental support is disabled until we can figure out
    #       how to support incremental loads per matching file, rather
    #       than a blanket threshold.
    #
    # filesystem_resource.apply_hints(
    #     incremental=dlt.sources.incremental("modification_date"),
    # )
    return (
        filesystem_resource
        | dlt.transformer(name="read_csv", max_table_nesting=0)(read_csv),
        filesystem_resource
        | dlt.transformer(name="read_csv_headless", max_table_nesting=0)(
            read_csv_headless
        ),
        filesystem_resource
        | dlt.transformer(name="read_jsonl", max_table_nesting=0)(read_jsonl),
        filesystem_resource
        | dlt.transformer(name="read_bson", max_table_nesting=0)(read_bson),
        filesystem_resource
        | dlt.transformer(name="read_msgpack", max_table_nesting=0)(read_msgpack),
        filesystem_resource
        | dlt.transformer(name="read_cbor", max_table_nesting=0)(read_cbor),
        filesystem_resource
        | dlt.transformer(name="read_parquet", max_table_nesting=0)(read_parquet),
        filesystem_resource
        | dlt.transformer(name="read_csv_duckdb", max_table_nesting=0)(read_csv_duckdb),
    )


@dlt.resource(
    primary_key="file_url", spec=FilesystemConfigurationResource, standalone=True
)
def filesystem(
    bucket_url: str = dlt.secrets.value,
    credentials: Union[FileSystemCredentials, AbstractFileSystem] = dlt.secrets.value,
    file_glob: Optional[str] = "*",
    files_per_page: int = 100,
    extract_content: bool = True,
) -> Iterator[List[FileItem]]:
    """This resource lists files in `bucket_url` using `file_glob` pattern. The files are yielded as FileItem which also
    provide methods to open and read file data. It should be combined with transformers that further process (ie. load files)

    Args:
        bucket_url (str): The url to the bucket.
        credentials (FileSystemCredentials | AbstractFilesystem): The credentials to the filesystem of fsspec `AbstractFilesystem` instance.
        file_glob (str, optional): The filter to apply to the files in glob format. by default lists all files in bucket_url non-recursively
        files_per_page (int, optional): The number of files to process at once, defaults to 100.
        extract_content (bool, optional): If true, the content of the file will be extracted if
            false it will return a fsspec file, defaults to False.

    Returns:
        Iterator[List[FileItem]]: The list of files.
    """

    fs_client: AbstractFileSystem
    if isinstance(credentials, AbstractFileSystem):
        fs_client = credentials
    else:
        fs_client = fsspec_filesystem(bucket_url, credentials)[0]

    files_chunk: List[FileItem] = []
    for file_model in glob_files(fs_client, bucket_url, file_glob or "**"):
        file_dict = FileItemDict(file_model, fs_client)
        if extract_content:
            file_dict["file_content"] = file_dict.read_bytes()
        files_chunk.append(file_dict)  # ty: ignore[invalid-argument-type]
        # wait for the chunk to be full
        if len(files_chunk) >= files_per_page:
            yield files_chunk
            files_chunk = []
    if files_chunk:
        yield files_chunk


def resource_for_reader(ref: FilesystemReference) -> DltSource | DltResource:
    """Build the filesystem reader resource named by ``ref.reader_name``.

    Threads ``column_types`` into ``read_csv_headless``; every other reader is selected as-is.
    """
    if ref.reader_name != "read_csv_headless":
        return readers(ref.bucket_url, ref.fs, ref.file_glob).with_resources(
            ref.reader_name
        )

    column_names = list(ref.column_types.keys()) if ref.column_types else None

    def read_csv_headless_with_cols(
        items: Iterator[FileItemDict],
        chunksize: int = 10000,
        **pandas_kwargs: Any,
    ) -> Iterator[TDataItems]:
        """Read header-less CSV with the column names derived from ``ref.column_types``."""
        yield from read_csv_headless(
            items,
            chunksize=chunksize,
            column_names=column_names,
            **pandas_kwargs,
        )

    filesystem_resource = filesystem(ref.bucket_url, ref.fs, file_glob=ref.file_glob)
    return filesystem_resource | dlt.transformer(
        name="read_csv_headless", max_table_nesting=0
    )(read_csv_headless_with_cols)
