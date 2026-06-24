from urllib.parse import parse_qs, urlparse


class SmartsheetSource:
    def handles_incrementality(self) -> bool:
        return False

    # smartsheet://?access_token=<access_token>
    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key"):
            raise ValueError("Incremental loads are not supported for Smartsheet")

        if not table:
            raise ValueError(
                "Source table (sheet_id) is required to connect to Smartsheet"
            )

        source_parts = urlparse(uri)
        source_fields = parse_qs(source_parts.query)
        access_token = source_fields.get("access_token")

        if not access_token:
            raise ValueError(
                "access_token in the URI is required to connect to Smartsheet"
            )

        from omniload.source.smartsheets.adapter import smartsheet_source

        return smartsheet_source(
            access_token=access_token[0],
            sheet_id=table,  # table is now a single sheet_id
        )
