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

"""Strapi source helpers"""

import math
from typing import Iterable

from dlt.common.typing import TDataItem
from dlt.sources.helpers import requests


def get_endpoint(token: str, domain: str, endpoint: str) -> Iterable[TDataItem]:
    """
    A generator that yields data from a paginated API endpoint.

    Args:
        token (str): The access token for the API.
        domain (str): The domain name of the API.
        endpoint (str): The API endpoint to query, defaults to ''.

    Yields:
        TDataItem: A data item from the API endpoint.
    """
    api_endpoint = f"https://{domain}/api/{endpoint}"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
    page_size = 25
    params = {
        "pagination[start]": 0,
        "pagination[limit]": page_size,
        "pagination[withCount]": 1,
    }

    # get the total number of pages
    response = requests.get(api_endpoint, headers=headers, params=params)
    total_results = response.json()["meta"]["pagination"]["total"]
    pages_total = math.ceil(total_results / page_size)

    # yield page by page
    for page_number in range(pages_total):
        params["pagination[start]"] = page_number * page_size
        response = requests.get(api_endpoint, headers=headers, params=params)
        data = response.json().get("data")
        if data:
            yield from data
