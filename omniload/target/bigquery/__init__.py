import base64
import json
import os
from urllib.parse import parse_qs, urlparse

import dlt


class BigQueryDestination:
    def dlt_dest(self, uri: str, **kwargs):
        source_fields = urlparse(uri)
        source_params = parse_qs(source_fields.query)

        cred_path = source_params.get("credentials_path")
        credentials_base64 = source_params.get("credentials_base64")

        location = None
        if source_params.get("location"):
            loc_params = source_params.get("location", [])
            if len(loc_params) > 1:
                raise ValueError("Only one location is allowed")
            location = loc_params[0]

        # Following dlt's pattern (like google_analytics), we let dlt's credential resolution
        # handle defaults automatically. When credentials_path or credentials_base64 are not
        # provided, dlt will use Application Default Credentials via GcpServiceAccountCredentials.
        credentials = None
        if cred_path:
            with open(cred_path[0], "r") as f:
                credentials = json.load(f)
        elif credentials_base64:
            credentials = json.loads(
                base64.b64decode(credentials_base64[0]).decode("utf-8")
            )

        staging_bucket = kwargs.get("staging_bucket", None)
        if staging_bucket:
            if not staging_bucket.startswith("gs://"):
                raise ValueError("Staging bucket must start with gs://")

            os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = staging_bucket
            if credentials:
                os.environ["DESTINATION__FILESYSTEM__CREDENTIALS__PROJECT_ID"] = (
                    credentials.get("project_id", None)
                )
                os.environ["DESTINATION__FILESYSTEM__CREDENTIALS__PRIVATE_KEY"] = (
                    credentials.get("private_key", None)
                )
                os.environ["DESTINATION__FILESYSTEM__CREDENTIALS__CLIENT_EMAIL"] = (
                    credentials.get("client_email", None)
                )

        project_id = None
        if source_fields.hostname:
            project_id = source_fields.hostname

        return dlt.destinations.bigquery(
            credentials=credentials,  # type: ignore
            location=location,  # type: ignore
            project_id=project_id,
            **kwargs,
        )

    def dlt_run_params(self, uri: str, table: str, **kwargs) -> dict:
        table_fields = table.split(".")
        if len(table_fields) != 2 and len(table_fields) != 3:
            raise ValueError(
                "Table name must be in the format <dataset>.<table> or <project>.<dataset>.<table>"
            )

        res = {
            "dataset_name": table_fields[-2],
            "table_name": table_fields[-1],
        }

        staging_bucket = kwargs.get("staging_bucket", None)
        if staging_bucket:
            res["staging"] = "filesystem"

        return res

    def post_load(self):
        pass
