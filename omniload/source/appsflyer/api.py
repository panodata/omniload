from urllib.parse import parse_qs, urlparse


class AppsflyerSource:
    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        from omniload.source.appsflyer.adapter import appsflyer_source

        if kwargs.get("incremental_key"):
            raise ValueError(
                "Appsflyer_Source takes care of incrementality on its own, you should not provide incremental_key"
            )

        source_fields = urlparse(uri)
        source_params = parse_qs(source_fields.query)
        api_key = source_params.get("api_key")

        if not api_key:
            raise ValueError("api_key in the URI is required to connect to Appsflyer")

        start_date = kwargs.get("interval_start")
        end_date = kwargs.get("interval_end")
        dimensions = []
        metrics = []
        if table.startswith("custom:"):
            fields = table.split(":", 3)
            if len(fields) != 3:
                raise ValueError(
                    "Invalid Adjust custom table format. Expected format: custom:<dimensions>:<metrics>"
                )
            dimensions = fields[1].split(",")
            metrics = fields[2].split(",")
            table = "custom"

        return appsflyer_source(
            api_key=api_key[0],
            start_date=start_date.strftime("%Y-%m-%d") if start_date else None,  # type: ignore
            end_date=end_date.strftime("%Y-%m-%d") if end_date else None,  # type: ignore
            dimensions=dimensions,
            metrics=metrics,
        ).with_resources(table)
