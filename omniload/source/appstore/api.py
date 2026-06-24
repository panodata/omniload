import base64
from datetime import datetime, timedelta
from typing import List, Optional
from urllib.parse import parse_qs, urlparse

from omniload.error import MissingValueError, UnsupportedResourceError


class AppleAppStoreSource:
    def handles_incrementality(self) -> bool:
        return True

    def init_client(
        self,
        key_id: str,
        issuer_id: str,
        key_path: Optional[List[str]],
        key_base64: Optional[List[str]],
    ):
        key = None
        if key_path is not None:
            with open(key_path[0]) as f:
                key = f.read()
        else:
            key = base64.b64decode(key_base64[0]).decode()  # type: ignore

        from omniload.source.appstore.client import AppStoreConnectClient

        return AppStoreConnectClient(key.encode(), key_id, issuer_id)

    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key"):
            raise ValueError(
                "App Store takes care of incrementality on its own, you should not provide incremental_key"
            )
        parsed_uri = urlparse(uri)
        params = parse_qs(parsed_uri.query)

        key_id = params.get("key_id")
        if key_id is None:
            raise MissingValueError("key_id", "App Store")

        key_path = params.get("key_path")
        key_base64 = params.get("key_base64")
        key_available = any(
            map(
                lambda x: x is not None,
                [key_path, key_base64],
            )
        )
        if key_available is False:
            raise MissingValueError("key_path or key_base64", "App Store")

        issuer_id = params.get("issuer_id")
        if issuer_id is None:
            raise MissingValueError("issuer_id", "App Store")

        client = self.init_client(key_id[0], issuer_id[0], key_path, key_base64)

        app_ids = params.get("app_id")
        if ":" in table:
            intended_table, app_ids_override = table.split(":", maxsplit=1)
            app_ids = app_ids_override.split(",")
            table = intended_table

        if app_ids is None:
            raise MissingValueError("app_id", "App Store")

        from omniload.source.appstore.adapter import app_store

        src = app_store(
            client,
            app_ids,
            start_date=kwargs.get(
                "interval_start", datetime.now() - timedelta(days=30)
            ),
            end_date=kwargs.get("interval_end"),
        )

        if table not in src.resources:
            raise UnsupportedResourceError(table, "AppStore")

        return src.with_resources(table)
