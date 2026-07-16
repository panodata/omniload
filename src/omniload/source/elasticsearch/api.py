from urllib.parse import parse_qs, urlparse


class ElasticsearchSource:
    def handles_incrementality(self) -> bool:
        return False

    def dlt_source(self, uri: str, table: str, **kwargs):

        from dlt.extract import Incremental as dlt_incremental

        incremental = None
        if kwargs.get("incremental_key"):
            start_value = kwargs.get("interval_start")
            end_value = kwargs.get("interval_end")

            incremental = dlt_incremental(
                kwargs.get("incremental_key", ""),
                initial_value=start_value,
                end_value=end_value,
                range_end="closed",
                range_start="closed",
            )

        # elasticsearch://localhost:9200?secure=true&verify_certs=false
        parsed = urlparse(uri)

        index = table
        if not index:
            raise ValueError(
                "Table name must be provided which is the index name in elasticsearch"
            )

        query_params = parsed.query
        params = parse_qs(query_params)

        secure = True
        if "secure" in params:
            secure = params["secure"][0].capitalize() == "True"

        verify_certs = True
        if "verify_certs" in params:
            verify_certs = params["verify_certs"][0].capitalize() == "True"

        scheme = "https" if secure else "http"
        netloc = parsed.netloc
        connection_url = f"{scheme}://{netloc}"

        from omniload.source.elasticsearch.adapter import elasticsearch_source

        return elasticsearch_source(
            connection_url=connection_url,
            index=index,
            verify_certs=verify_certs,
            incremental=incremental,
        ).with_resources(table)
