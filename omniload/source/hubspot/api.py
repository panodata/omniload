from urllib.parse import parse_qs, urlparse


class HubspotSource:
    def handles_incrementality(self) -> bool:
        return True

    # hubspot://?api_key=<api_key>
    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key"):
            raise ValueError(
                "Hubspot takes care of incrementality on its own, you should not provide incremental_key"
            )

        api_key = None
        source_parts = urlparse(uri)
        source_parmas = parse_qs(source_parts.query)
        api_key = source_parmas.get("api_key")

        if not api_key:
            raise ValueError("api_key in the URI is required to connect to Hubspot")

        endpoint = None

        from omniload.source.hubspot.adapter import hubspot

        if table.startswith("custom:"):
            fields = table.split(":", 2)
            if len(fields) != 2 and len(fields) != 3:
                raise ValueError(
                    "Invalid Hubspot custom table format. Expected format: custom:<custom_object_type> or custom:<custom_object_type>:<associations>"
                )

            if len(fields) == 2:
                endpoint = fields[1]
            else:
                endpoint = f"{fields[1]}:{fields[2]}"

            return hubspot(
                api_key=api_key[0],
                custom_object=endpoint,
                start_date=kwargs.get("interval_start"),
                end_date=kwargs.get("interval_end"),
            ).with_resources("custom")

        elif table in [
            "contacts",
            "companies",
            "deals",
            "tickets",
            "products",
            "quotes",
            "calls",
            "emails",
            "feedback_submissions",
            "line_items",
            "meetings",
            "notes",
            "tasks",
            "carts",
            "discounts",
            "fees",
            "invoices",
            "commerce_payments",
            "taxes",
            "owners",
            "schemas",
        ]:
            endpoint = table
        else:
            raise ValueError(
                f"Resource '{table}' is not supported for Hubspot source yet, if you are interested in it please create a GitHub issue at https://github.com/panodata/omniload"
            )

        return hubspot(
            api_key=api_key[0],
            start_date=kwargs.get("interval_start"),
            end_date=kwargs.get("interval_end"),
        ).with_resources(endpoint)
