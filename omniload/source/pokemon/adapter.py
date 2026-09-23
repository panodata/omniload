# Copyright 2022-2026 ScaleVector
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

"""
This source provides data extraction from an example source as a starting point for new pipelines.
Available resources: [berries, pokemon]
"""

import typing as t
from typing import Any, Dict, Iterable, Sequence

import dlt
from dlt.common.typing import TDataItem
from dlt.sources import DltResource
from dlt.sources.helpers import requests

from .settings import BERRY_URL, POKEMON_URL


@dlt.resource(write_disposition="replace")
def berries() -> Iterable[TDataItem]:
    """
    Returns a list of berries.
    Yields:
        dict: The berries data.
    """
    yield requests.get(BERRY_URL).json()["results"]


@dlt.resource(write_disposition="replace")
def pokemon() -> Iterable[TDataItem]:
    """
    Returns a list of pokemon.
    Yields:
        dict: The pokemon data.
    """
    yield requests.get(POKEMON_URL).json()["results"]


@dlt.source
def source() -> Sequence[DltResource]:
    """
    The source function that returns all availble resources.
    Returns:
        Sequence[DltResource]: A sequence of DltResource objects containing the fetched data.
    """
    return [berries, pokemon]
