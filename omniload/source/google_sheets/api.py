import base64
import json
from typing import Callable
from urllib.parse import parse_qs, urlparse

from omniload.core.model import table_string_to_dataclass


class GoogleSheetsSource:
    table_builder: Callable

    def __init__(self, table_builder=None) -> None:
        if table_builder is None:
            from omniload.source.google_sheets.adapter import google_spreadsheet

            table_builder = google_spreadsheet

        self.table_builder = table_builder

    def handles_incrementality(self) -> bool:
        return False

    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key"):
            raise ValueError("Incremental loads are not supported for Google Sheets")

        source_fields = urlparse(uri)
        source_params = parse_qs(source_fields.query)

        cred_path = source_params.get("credentials_path")
        credentials_base64 = source_params.get("credentials_base64")
        if not cred_path and not credentials_base64:
            raise ValueError(
                "credentials_path or credentials_base64 is required in the URI to get data from Google Sheets"
            )

        credentials = {}
        if cred_path:
            with open(cred_path[0], "r") as f:
                credentials = json.load(f)
        elif credentials_base64:
            credentials = json.loads(
                base64.b64decode(credentials_base64[0]).decode("utf-8")
            )

        table_fields = table_string_to_dataclass(table)
        return self.table_builder(
            credentials=credentials,
            spreadsheet_url_or_id=table_fields.dataset,
            range_names=[table_fields.table],
            get_named_ranges=False,
        )
