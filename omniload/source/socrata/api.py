class SocrataSource:
    def handles_incrementality(self) -> bool:
        return False

    def dlt_source(self, uri: str, table: str, **kwargs):
        """
        Creates a DLT source for Socrata open data platform.

        URI format: socrata://domain?app_token=TOKEN
        Table: dataset_id (e.g., "6udu-fhnu")

        Args:
            uri: Socrata connection URI with domain and optional auth params
            table: Dataset ID (e.g., "6udu-fhnu")
            **kwargs: Additional arguments:
                - incremental_key: Field to use for incremental loading (e.g., ":updated_at")
                - interval_start: Start date for initial load
                - interval_end: End date for load
                - primary_key: Primary key field for merge operations

        Returns:
            DltResource for the Socrata dataset
        """
        from urllib.parse import parse_qs, urlparse

        parsed = urlparse(uri)

        domain = parsed.netloc
        if not domain:
            raise ValueError(
                "Domain must be provided in the URI.\n"
                "Format: socrata://domain?app_token=TOKEN\n"
                "Example: socrata://evergreen.data.socrata.com?app_token=mytoken"
            )

        query_params = parse_qs(parsed.query)

        dataset_id = table
        if not dataset_id:
            raise ValueError(
                "Dataset ID must be provided as the table parameter.\n"
                "Example: --source-table 6udu-fhnu"
            )

        app_token = query_params.get("app_token", [None])[0]
        username = query_params.get("username", [None])[0]
        password = query_params.get("password", [None])[0]

        incremental = None
        if kwargs.get("incremental_key"):
            start_value = kwargs.get("interval_start")
            end_value = kwargs.get("interval_end")

            if start_value:
                start_value = (
                    start_value.isoformat()
                    if hasattr(start_value, "isoformat")
                    else str(start_value)
                )

            if end_value:
                end_value = (
                    end_value.isoformat()
                    if hasattr(end_value, "isoformat")
                    else str(end_value)
                )

            from dlt.extract import Incremental as dlt_incremental

            incremental = dlt_incremental(
                kwargs.get("incremental_key", ""),
                initial_value=start_value,
                end_value=end_value,
                range_end="open",
                range_start="closed",
            )

        primary_key = kwargs.get("primary_key")

        from omniload.source.socrata.adapter import source

        return source(
            domain=domain,
            dataset_id=dataset_id,
            app_token=app_token,
            username=username,
            password=password,
            incremental=incremental,
            primary_key=primary_key,
        ).with_resources("dataset")
