class ElasticsearchDestination:
    def dlt_dest(self, uri: str, **kwargs):
        from urllib.parse import urlparse

        parsed_uri = urlparse(uri)

        # Extract connection details from URI
        scheme = parsed_uri.scheme or "http"
        host = parsed_uri.hostname or "localhost"
        port = parsed_uri.port or 9200
        username = parsed_uri.username
        password = parsed_uri.password

        # Build connection string
        if username and password:
            connection_string = f"{scheme}://{username}:{password}@{host}:{port}"
        else:
            connection_string = f"{scheme}://{host}:{port}"

        # Add query parameters if any
        if parsed_uri.query:
            connection_string += f"?{parsed_uri.query}"
        from omniload.target.elasticsearch.adapter import elasticsearch_insert

        return elasticsearch_insert(connection_string=connection_string)

    def dlt_run_params(self, uri: str, table: str, **kwargs) -> dict:
        return {
            "table_name": table,
        }

    def post_load(self):
        pass
