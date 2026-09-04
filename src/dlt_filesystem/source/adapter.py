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

from typing import Iterator, List, Optional, Tuple, Union

import dlt
from dlt.sources import DltResource
from dlt.sources.credentials import FileSystemCredentials
from dlt.sources.filesystem import FileItem, FileItemDict, fsspec_filesystem
from fsspec import AbstractFileSystem

from dlt_filesystem.source.error import NoFilesFoundError
from dlt_filesystem.source.format import readers as reader_functions
from dlt_filesystem.source.format.readers import ReadersSource
from dlt_filesystem.source.format.registry import (
    READER_REGISTRATIONS,
    ReaderRegistration,
)
from dlt_filesystem.source.lister import glob_files

from .model import FilesystemConfigurationResource


def _resolve_reader(registration: ReaderRegistration):
    """Resolve a registered reader name without making the registry import reader code."""
    reader = getattr(reader_functions, registration.reader_name, None)
    if not callable(reader):
        raise ValueError(
            f"Reader function {registration.reader_name!r} is not defined in "
            "dlt_filesystem.source.format.readers"
        )
    return reader


@dlt.source(_impl_cls=ReadersSource, spec=FilesystemConfigurationResource)
def readers(
    bucket_url: str,
    credentials: Union[FileSystemCredentials, AbstractFileSystem],
    file_glob: Optional[str] = "*",
) -> Tuple[DltResource, ...]:
    """This source provides a few resources that are chunked file readers. Readers can be further parametrized before use
       read_csv(chunksize, **pandas_kwargs)
       read_json(chunksize)
       read_jsonl(chunksize)
       read_parquet(chunksize)

    Args:
        bucket_url (str): The url to the bucket.
        credentials (FileSystemCredentials | AbstractFilesystem): The credentials to the filesystem of fsspec `AbstractFilesystem` instance.
        file_glob (str, optional): The filter to apply to the files in glob format. by default lists all files in bucket_url non-recursively
    """
    filesystem_resource = filesystem(bucket_url, credentials, file_glob=file_glob)

    return tuple(
        filesystem_resource
        | dlt.transformer(
            name=registration.reader_name,
            max_table_nesting=registration.max_table_nesting,
        )(_resolve_reader(registration))
        for registration in READER_REGISTRATIONS
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
    require_file_match: bool = False,
    filesystem_incremental: bool = False,
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
        require_file_match (bool, optional): Raise when the concrete source selection
            matches no file. Defaults to False for direct uses of this resource.
        filesystem_incremental (bool, optional): Resolve trustworthy modification
            times when the listing itself does not carry one. Defaults to False.

    Returns:
        Iterator[List[FileItem]]: The list of files.
    """

    fs_client: AbstractFileSystem
    if isinstance(credentials, AbstractFileSystem):
        fs_client = credentials
    else:
        fs_client = fsspec_filesystem(bucket_url, credentials)[0]

    matched_files = 0
    files_chunk: List[FileItem] = []
    for file_model in glob_files(
        fs_client,
        bucket_url,
        file_glob or "**",
        filesystem_incremental=filesystem_incremental,
    ):
        matched_files += 1
        file_dict = FileItemDict(file_model, fs_client)
        if extract_content:
            file_dict["file_content"] = file_dict.read_bytes()
        files_chunk.append(file_dict)  # ty: ignore[invalid-argument-type]
        # wait for the chunk to be full
        if len(files_chunk) >= files_per_page:
            yield files_chunk
            files_chunk = []
    if require_file_match and matched_files == 0:
        raise NoFilesFoundError(bucket_url, file_glob or "**")
    if files_chunk:
        yield files_chunk
