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

"""Helpers for the filesystem resource."""

from typing import Any, Dict, Generator, Iterable, List, Optional

from dlt.common.typing import TDataItem

from dlt_filesystem.source.format.settings import DEFAULT_CHUNK_SIZE


def add_columns(columns: List[str], rows: List[List[Any]]) -> List[Dict[str, Any]]:
    """Adds column names to the given rows.

    Args:
        columns (List[str]): The column names.
        rows (List[List[Any]]): The rows.

    Returns:
        List[Dict[str, Any]]: The rows with column names.
    """
    result = []
    for row in rows:
        result.append(dict(zip(columns, row)))

    return result


def fetch_arrow(file_data, chunk_size: Optional[int] = None) -> Iterable[TDataItem]:
    """Fetches data from the given CSV file.

    Args:
        file_data (DuckDBPyRelation): The CSV file data.
        chunk_size (int): The number of rows to read at once.

    Yields:
        Iterable[TDataItem]: Data items, read from the given CSV file.
    """
    chunk_size = chunk_size or DEFAULT_CHUNK_SIZE
    batcher = file_data.fetch_arrow_reader(batch_size=chunk_size)
    yield from batcher


def fetch_json(
    file_data, chunk_size: Optional[int] = None
) -> Generator[List[Dict[str, Any]], None, None]:
    """Fetches data from the given CSV file.

    Args:
        file_data (DuckDBPyRelation): The CSV file data.
        chunk_size (int): The number of rows to read at once.

    Yields:
        Iterable[TDataItem]: Data items, read from the given CSV file.
    """
    chunk_size = chunk_size or DEFAULT_CHUNK_SIZE
    while True:
        batch = file_data.fetchmany(chunk_size)
        if not batch:
            break

        yield add_columns(file_data.columns, batch)
