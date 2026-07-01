from typing import Callable
from urllib.parse import urlparse


class CouchbaseSource:
    table_builder: Callable

    def __init__(self, table_builder=None) -> None:
        if table_builder is None:
            from omniload.source.couchbase.adapter import couchbase_collection

            table_builder = couchbase_collection

        self.table_builder = table_builder

    def handles_incrementality(self) -> bool:
        return False

    def dlt_source(self, uri: str, table: str, **kwargs):
        """
        Create a dlt source for reading data from Couchbase.

        URI formats:
            - couchbase://username:password@host
            - couchbase://username:password@host/bucket
            - couchbase://username:password@host?ssl=true
            - couchbases://username:password@host (SSL enabled)

        Table formats:
            - bucket.scope.collection (when bucket not in URI)
            - scope.collection (when bucket specified in URI path)

        Note: If password contains special characters (@, :, /, etc.), they must be URL-encoded.

        Examples:
            Local/Self-hosted:
            - couchbase://admin:password123@localhost with table "mybucket.myscope.mycollection"
            - couchbase://admin:password123@localhost/mybucket with table "myscope.mycollection"
            - couchbase://admin:password123@localhost?ssl=true with table "mybucket._default._default"

            Capella (Cloud):
            - couchbases://user:pass@cb.xxx.cloud.couchbase.com with table "travel-sample.inventory.airport"
            - couchbase://user:pass@cb.xxx.cloud.couchbase.com/travel-sample?ssl=true with table "inventory.airport"

        To encode password in Python:
            from urllib.parse import quote
            encoded_pwd = quote("MyPass@123!", safe='')
            uri = f"couchbase://admin:{encoded_pwd}@localhost?ssl=true"

        Args:
            uri: Couchbase connection URI (can include /bucket path and ?ssl=true query parameter)
            table: Format depends on URI:
                - bucket.scope.collection (if bucket not in URI)
                - scope.collection (if bucket in URI path)
            **kwargs: Additional arguments:
                - limit: Maximum number of documents to fetch
                - incremental_key: Field to use for incremental loading
                - interval_start: Start value for incremental loading
                - interval_end: End value for incremental loading

        Returns:
            DltResource for the Couchbase collection
        """
        # Parse the URI to extract connection details
        # urlparse automatically decodes URL-encoded credentials

        parsed = urlparse(uri)

        # Extract username and password from URI
        # Note: urlparse automatically decodes URL-encoded characters in username/password
        from urllib.parse import unquote

        username = parsed.username
        password = unquote(parsed.password) if parsed.password else None

        if not username or not password:
            raise ValueError(
                "Username and password must be provided in the URI.\n"
                "Format: couchbase://username:password@host\n"
                "If password has special characters (@, :, /), URL-encode them.\n"
                "Example: couchbase://admin:MyPass%40123@localhost for password 'MyPass@123'"
            )

        # Reconstruct connection string without credentials
        scheme = parsed.scheme
        netloc = parsed.netloc

        # Remove username:password@ from netloc if present
        if "@" in netloc:
            netloc = netloc.split("@", 1)[1]

        # Parse query parameters from URI
        from urllib.parse import parse_qs, urlencode

        query_params = parse_qs(parsed.query)

        # Check if SSL is requested via URI query parameter (?ssl=true)
        if "ssl" in query_params:
            ssl_value = query_params["ssl"][0].lower()
            use_ssl = ssl_value in ("true", "1", "yes")

            # Apply SSL scheme based on parameter
            if use_ssl and scheme == "couchbase":
                scheme = "couchbases"

        connection_string = f"{scheme}://{netloc}"
        connection_query = urlencode(
            {key: value for key, value in query_params.items() if key != "ssl"},
            doseq=True,
        )
        if connection_query:
            connection_string = f"{connection_string}?{connection_query}"

        # Extract bucket from URI path if present (e.g., couchbase://host/bucket)
        bucket_from_uri = None
        if parsed.path and parsed.path.strip("/"):
            bucket_from_uri = parsed.path.strip("/").split("/")[0]

        # Parse table format: can be "scope.collection" or "bucket.scope.collection"
        table_parts = table.split(".")

        if len(table_parts) == 3:
            # Format: bucket.scope.collection
            bucket, scope, collection = table_parts
        elif len(table_parts) == 2:
            # Format: scope.collection (bucket from URI)
            if bucket_from_uri:
                bucket = bucket_from_uri
                scope, collection = table_parts
            else:
                raise ValueError(
                    "Table format is 'scope.collection' but no bucket specified in URI.\n"
                    f"Either use URI format: couchbase://user:pass@host/bucket\n"
                    f"Or use table format: bucket.scope.collection\n"
                    f"Got table: {table}"
                )
        else:
            raise ValueError(
                "Table format must be 'bucket.scope.collection' or 'scope.collection' (with bucket in URI). "
                f"Got: {table}\n"
                "Examples:\n"
                "  - URI: couchbase://user:pass@host, Table: travel-sample.inventory.airport\n"
                "  - URI: couchbase://user:pass@host/travel-sample, Table: inventory.airport"
            )

        # Handle incremental loading
        incremental = None
        if kwargs.get("incremental_key"):
            start_value = kwargs.get("interval_start")
            end_value = kwargs.get("interval_end")

            from dlt.extract import Incremental as dlt_incremental

            incremental = dlt_incremental(
                kwargs.get("incremental_key", ""),
                initial_value=start_value,
                end_value=end_value,
                range_end="closed",
                range_start="closed",
            )

        # Get optional parameters
        limit = kwargs.get("limit")

        table_instance = self.table_builder(
            connection_string=connection_string,
            username=username,
            password=password,
            bucket=bucket,
            scope=scope,
            collection=collection,
            incremental=incremental,
            limit=limit,
        )
        table_instance.max_table_nesting = 1

        return table_instance
