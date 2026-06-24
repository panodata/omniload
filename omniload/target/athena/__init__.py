import os
from urllib.parse import parse_qs, quote, urlparse

import dlt
from dlt.common.configuration.specs import AwsCredentials


class AthenaDestination:
    def dlt_dest(self, uri: str, **kwargs):
        encoded_uri = quote(uri, safe=":/?&=")
        source_fields = urlparse(encoded_uri)
        source_params = parse_qs(source_fields.query)

        bucket = source_params.get("bucket", [None])[0]
        if not bucket:
            raise ValueError("A bucket is required to connect to Athena.")

        if not bucket.startswith("s3://"):
            bucket = f"s3://{bucket}"

        bucket = bucket.rstrip("/")

        dest_table = kwargs.get("dest_table", None)
        if not dest_table:
            raise ValueError("A destination table is required to connect to Athena.")

        dest_table_fields = dest_table.split(".")
        if len(dest_table_fields) != 2:
            raise ValueError(
                f"Table name must be in the format <schema>.<table>, given: {dest_table}"
            )

        query_result_path = f"{bucket}/{dest_table_fields[0]}_staging/metadata"

        access_key_id = source_params.get("access_key_id", [None])[0]
        secret_access_key = source_params.get("secret_access_key", [None])[0]
        session_token = source_params.get("session_token", [None])[0]
        profile_name = source_params.get("profile", ["default"])[0]
        region_name = source_params.get("region_name", [None])[0]

        if not access_key_id and not secret_access_key:
            import botocore.session

            session = botocore.session.Session(profile=profile_name)
            default = session.get_credentials()
            if not profile_name:
                raise ValueError(
                    "You have to either provide access_key_id and secret_access_key pair or a valid AWS profile name."
                )
            access_key_id = default.access_key
            secret_access_key = default.secret_key
            session_token = default.token
            if region_name is None:
                region_name = session.get_config_variable("region")

        if not region_name:
            raise ValueError("The region_name is required to connect to Athena.")

        os.environ["DESTINATION__BUCKET_URL"] = bucket
        if access_key_id and secret_access_key:
            os.environ["DESTINATION__CREDENTIALS__AWS_ACCESS_KEY_ID"] = access_key_id
            os.environ["DESTINATION__CREDENTIALS__AWS_SECRET_ACCESS_KEY"] = (
                secret_access_key
            )
        if session_token:
            os.environ["DESTINATION__CREDENTIALS__AWS_SESSION_TOKEN"] = session_token

        return dlt.destinations.athena(
            query_result_bucket=query_result_path,
            athena_work_group=source_params.get("workgroup", [None])[0],  # type: ignore
            credentials=AwsCredentials(
                aws_access_key_id=access_key_id,  # type: ignore
                aws_secret_access_key=secret_access_key,  # type: ignore
                aws_session_token=session_token,
                region_name=region_name,
            ),
            destination_name=bucket,
            force_iceberg=True,
        )

    def dlt_run_params(self, uri: str, table: str, **kwargs) -> dict:
        table_fields = table.split(".")
        if len(table_fields) != 2:
            raise ValueError("Table name must be in the format <schema>.<table>")
        return {
            "table_format": "iceberg",
            "dataset_name": table_fields[-2],
            "table_name": table_fields[-1],
        }

    def post_load(self):
        pass
