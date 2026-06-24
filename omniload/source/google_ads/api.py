import base64
import os
import tempfile
from datetime import datetime, timedelta, timezone
from typing import Dict, List
from urllib.parse import parse_qs, urlparse

from omniload.error import MissingValueError, UnsupportedResourceError


class GoogleAdsSource:
    def handles_incrementality(self) -> bool:
        return False

    def init_client(self, params: Dict[str, List[str]]):
        from google.ads.googleads.client import GoogleAdsClient

        dev_token = params.get("dev_token")
        if dev_token is None or len(dev_token) == 0:
            raise MissingValueError("dev_token", "Google Ads")

        client_id = params.get("client_id")
        client_secret = params.get("client_secret")
        refresh_token = params.get("refresh_token")
        oauth_available = all(
            x is not None and len(x) > 0
            for x in [client_id, client_secret, refresh_token]
        )

        credentials_path = params.get("credentials_path")
        credentials_base64 = params.get("credentials_base64")
        service_account_available = any(
            x is not None and len(x) > 0 for x in [credentials_path, credentials_base64]
        )

        if not oauth_available and not service_account_available:
            raise MissingValueError(
                "client_id/client_secret/refresh_token or credentials_path/credentials_base64",
                "Google Ads",
            )

        if oauth_available and service_account_available:
            import logging

            logging.warning(
                "Both OAuth and service account credentials provided for Google Ads; using OAuth."
            )

        fd = None
        if oauth_available:
            conf = {
                "client_id": client_id[0],  # type: ignore
                "client_secret": client_secret[0],  # type: ignore
                "refresh_token": refresh_token[0],  # type: ignore
                "developer_token": dev_token[0],
                "use_proto_plus": True,
            }
        else:
            path = None
            if credentials_path:
                path = credentials_path[0]
            else:
                (fd, path) = tempfile.mkstemp(prefix="secret-")
                secret = base64.b64decode(credentials_base64[0])  # type: ignore
                os.write(fd, secret)
                os.close(fd)

            conf = {
                "json_key_file_path": path,
                "use_proto_plus": True,
                "developer_token": dev_token[0],
            }

        login_customer_id = params.get("login_customer_id")
        if login_customer_id:
            conf["login_customer_id"] = login_customer_id[0]

        try:
            client = GoogleAdsClient.load_from_dict(conf)
        finally:
            if fd is not None:
                os.remove(path)

        return client

    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key") is not None:
            raise ValueError(
                "Google Ads takes care of incrementality on its own, you should not provide incremental_key"
            )

        parsed_uri = urlparse(uri)

        customer_id_raw = parsed_uri.hostname
        if not customer_id_raw:
            raise MissingValueError("customer_id", "Google Ads")

        customer_ids = [cid.strip() for cid in customer_id_raw.split(",")]

        params = parse_qs(parsed_uri.query)

        client = self.init_client(params)

        start_date = kwargs.get("interval_start") or datetime.now(
            tz=timezone.utc
        ) - timedelta(days=30)
        end_date = kwargs.get("interval_end")

        # most combinations of explict start/end dates are automatically handled.
        # however, in the scenario where only the end date is provided, we need to
        # calculate the start date based on the end date.
        if (
            kwargs.get("interval_end") is not None
            and kwargs.get("interval_start") is None
        ):
            start_date = end_date - timedelta(days=30)  # type: ignore

        report_spec = None
        gaql_query = None
        if table.startswith("daily:"):
            report_spec = table
            table = "daily_report"

            from omniload.source.google_ads.adapter import Report

            _, spec_customer_ids = Report.from_spec(report_spec)
            if spec_customer_ids:
                customer_ids = spec_customer_ids
        elif table.startswith("gaql_query:"):
            gaql_query = table[len("gaql_query:") :]
            table = "gaql_query"
        elif ":" in table:
            parts = table.split(":", 1)
            table = parts[0]
            customer_ids = [cid.strip() for cid in parts[1].split(",")]

        from omniload.source.google_ads.adapter import google_ads

        src = google_ads(
            client,
            customer_ids,
            report_spec,
            gaql_query=gaql_query,
            start_date=start_date,
            end_date=end_date,
        )

        if table not in src.resources:
            raise UnsupportedResourceError(table, "Google Ads")

        return src.with_resources(table)
