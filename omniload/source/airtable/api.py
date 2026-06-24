from urllib.parse import parse_qs, urlparse


class AirtableSource:
    def handles_incrementality(self) -> bool:
        return False

    # airtable://?access_token=<access_token>&base_id=<base_id>

    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key"):
            raise ValueError("Incremental loads are not supported for Airtable")

        if not table:
            raise ValueError("Source table is required to connect to Airtable")

        source_parts = urlparse(uri)
        source_fields = parse_qs(source_parts.query)
        access_token = source_fields.get("access_token")

        if not access_token:
            raise ValueError(
                "access_token in the URI is required to connect to Airtable"
            )

        base_id = source_fields.get("base_id", [None])[0]
        clean_table = table

        table_fields = table.split("/")
        if len(table_fields) == 2:
            clean_table = table_fields[1]
            if not base_id:
                base_id = table_fields[0]

        if not base_id:
            raise ValueError("base_id in the URI is required to connect to Airtable")

        from omniload.source.airtable.adapter import airtable_source

        return airtable_source(
            base_id=base_id, table_names=[clean_table], access_token=access_token[0]
        )
