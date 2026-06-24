class HttpSource:
    """Source for reading CSV, JSON, and Parquet files from HTTP URLs"""

    def handles_incrementality(self) -> bool:
        return False

    def dlt_source(self, uri: str, table: str, **kwargs):
        """
        Create a dlt source for reading files from HTTP URLs.

        URI format: http://example.com/file.csv or https://example.com/file.json

        Args:
            uri: HTTP(S) URL to the file
            table: Can specify file format using #format suffix (e.g., "data#csv_headless")
            **kwargs: Additional arguments:
                - file_format: Optional file format override ('csv', 'csv_headless', 'json', 'parquet')
                - chunksize: Number of records to process at once (default varies by format)
                - merge_key: Merge key for the resource
                - column_types: Dict of column_name -> column_type for csv_headless format

        Returns:
            DltResource for the HTTP file
        """
        from omniload.source.http.adapter import http_source

        # Extract the actual URL (remove the http:// or https:// scheme if duplicated)
        url = uri
        if uri.startswith("http://http://") or uri.startswith("https://https://"):
            url = uri.split("://", 1)[1]

        # Parse file format from table name (e.g., "data#csv_headless")
        file_format = kwargs.get("file_format")
        if "#" in table:
            _, format_suffix = table.split("#", 1)
            if format_suffix in ["csv", "csv_headless", "json", "jsonl", "parquet"]:
                # Map jsonl to json (reader treats them the same)
                file_format = "json" if format_suffix == "jsonl" else format_suffix

        chunksize = kwargs.get("chunksize")
        merge_key = kwargs.get("merge_key")

        # Extract column names from column_types dict (already parsed by main.py)
        column_types = kwargs.get("column_types")
        column_names = list(column_types.keys()) if column_types else None

        reader_kwargs = {}
        if chunksize is not None:
            reader_kwargs["chunksize"] = chunksize

        source = http_source(
            url=url,
            file_format=file_format,
            column_names=column_names,
            **reader_kwargs,
        )

        if merge_key:
            source.apply_hints(merge_key=merge_key)

        return source
