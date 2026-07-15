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
Basic strapi source
"""
import dlt
from typing import List, Iterable
from dlt.sources import DltResource

from .helpers import get_endpoint


@dlt.source
def strapi_source(
    endpoints: List[str],
    api_secret_key: str = dlt.secrets.value,
    domain: str = dlt.secrets.value,
) -> Iterable[DltResource]:
    """
    Source function for retrieving data from Strapi.

    Args:
        endpoints (List[str]): List of collections to retrieve data from.
        api_secret_key (str): API secret key for authentication. Defaults to the value in the `dlt.secrets` object.
        domain (str): Domain name for the Strapi API. Defaults to the value in the `dlt.secrets` object.

    Yields:
        DltResource: Data resources from the specified collections.
    """
    for endpoint in endpoints:
        yield dlt.resource(  # type: ignore
            get_endpoint(api_secret_key, domain, endpoint),
            name=endpoint,
            write_disposition="replace",
        )
