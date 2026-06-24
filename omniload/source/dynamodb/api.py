import re
from typing import Optional
from urllib.parse import ParseResult, parse_qs, urlparse


class DynamoDBSource:
    AWS_ENDPOINT_PATTERN = re.compile(r".*\.(.+)\.amazonaws\.com")

    def infer_aws_region(self, uri: ParseResult) -> Optional[str]:
        # try to infer from URI
        matches = self.AWS_ENDPOINT_PATTERN.match(uri.netloc)
        if matches is not None:
            return matches[1]

        # else obtain region from query string
        region = parse_qs(uri.query).get("region")
        if region is None:
            return None
        return region[0]

    def get_endpoint_url(self, url: ParseResult) -> str:
        if self.AWS_ENDPOINT_PATTERN.match(url.netloc) is not None:
            return f"https://{url.hostname}"
        return f"http://{url.netloc}"

    def handles_incrementality(self) -> bool:
        return False

    def dlt_source(self, uri: str, table: str, **kwargs):
        parsed_uri = urlparse(uri)

        region = self.infer_aws_region(parsed_uri)
        if not region:
            raise ValueError("region is required to connect to Dynamodb")

        qs = parse_qs(parsed_uri.query)
        access_key = qs.get("access_key_id")

        if not access_key:
            raise ValueError("access_key_id is required to connect to Dynamodb")

        secret_key = qs.get("secret_access_key")
        if not secret_key:
            raise ValueError("secret_access_key is required to connect to Dynamodb")

        from dlt.common.configuration.specs import AwsCredentials

        creds = AwsCredentials(
            aws_access_key_id=access_key[0],
            aws_secret_access_key=secret_key[0],
            region_name=region,
            endpoint_url=self.get_endpoint_url(parsed_uri),
        )

        incremental = None
        incremental_key = kwargs.get("incremental_key")

        from dlt.extract import Incremental as dlt_incremental

        from omniload.source.dynamodb.adapter import dynamodb
        from omniload.util.time import isotime

        if incremental_key:
            incremental = dlt_incremental(
                incremental_key.strip(),
                initial_value=isotime(kwargs.get("interval_start")),
                end_value=isotime(kwargs.get("interval_end")),
                range_end="closed",
                range_start="closed",
            )

        # bug: we never validate table.
        return dynamodb(table, creds, incremental)
