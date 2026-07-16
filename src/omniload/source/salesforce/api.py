from typing import Dict, Union
from urllib.parse import parse_qs, urlparse

from omniload.error import MissingValueError, UnsupportedResourceError


class SalesforceSource:
    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key"):
            raise ValueError(
                "Salesforce takes care of incrementality on its own, you should not provide incremental_key"
            )

        params = parse_qs(urlparse(uri).query)
        creds: Dict[str, Union[str, None]] = {
            "username": params.get("username", [None])[0],
            "password": params.get("password", [None])[0],
            "token": params.get("token", [None])[0],
            "domain": params.get("domain", [None])[0],
        }
        for k, v in creds.items():
            if v is None:
                raise MissingValueError(k, "Salesforce")

        from omniload.source.salesforce.adapter import salesforce_source

        src = salesforce_source(**creds)  # type: ignore

        if table.startswith("custom:"):
            custom_object = table.split(":")[1]
            src = salesforce_source(
                **creds,  # ty: ignore[invalid-argument-type]
                custom_object=custom_object,
            )
            return src.with_resources("custom")

        if table not in src.resources:
            raise UnsupportedResourceError(table, "Salesforce")

        return src.with_resources(table)
