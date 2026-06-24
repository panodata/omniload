from urllib.parse import parse_qs, urlparse

from dlt.common.time import ensure_pendulum_datetime_utc

from omniload.error import MissingValueError


class KinesisSource:
    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        # kinesis://?aws_access_key_id=<AccessKeyId>&aws_secret_access_key=<SecretAccessKey>&region_name=<Region>
        # source table = stream name
        parsed_uri = urlparse(uri)
        params = parse_qs(parsed_uri.query)

        aws_access_key_id = params.get("aws_access_key_id")
        if aws_access_key_id is None:
            raise MissingValueError("aws_access_key_id", "Kinesis")

        aws_secret_access_key = params.get("aws_secret_access_key")
        if aws_secret_access_key is None:
            raise MissingValueError("aws_secret_access_key", "Kinesis")

        region_name = params.get("region_name")
        if region_name is None:
            raise MissingValueError("region_name", "Kinesis")

        start_date = kwargs.get("interval_start")
        if start_date is not None:
            # the resource will read all messages after this timestamp.
            start_date = ensure_pendulum_datetime_utc(start_date)

        from dlt.common.configuration.specs import AwsCredentials

        from omniload.source.kinesis.adapter import kinesis_stream

        credentials = AwsCredentials(
            aws_access_key_id=aws_access_key_id[0],
            aws_secret_access_key=aws_secret_access_key[0],
            region_name=region_name[0],
        )

        if start_date is None:
            raise ValueError("No start date provided")

        return kinesis_stream(
            stream_name=table, credentials=credentials, initial_at_timestamp=start_date
        )
