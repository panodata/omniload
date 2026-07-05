import abc
import base64
import json
from urllib.parse import parse_qs, urlparse

import dlt.destinations.impl.filesystem.filesystem
from dlt.common.configuration.specs import AwsCredentials
from dlt.common.storages.configuration import FileSystemCredentials

from omniload.error import MissingValueError


class BlobStorageDestination(abc.ABC):
    @abc.abstractmethod
    def credentials(self, params: dict) -> FileSystemCredentials:
        """Build credentials for the blob storage destination."""
        pass

    @property
    @abc.abstractmethod
    def protocol(self) -> str:
        """The protocol used for the blob storage destination."""
        pass

    def dlt_dest(self, uri: str, **kwargs):
        parsed_uri = urlparse(uri)
        params = parse_qs(parsed_uri.query)
        creds = self.credentials(params)

        dest_table = kwargs["dest_table"]

        # only validate if dest_table is not a full URI
        if not parsed_uri.netloc:
            dest_table = self.validate_table(dest_table)

        table_parts = dest_table.split("/")

        if parsed_uri.path.strip("/"):
            path_parts = parsed_uri.path.strip("/ ").split("/")
            table_parts = path_parts + table_parts

        if parsed_uri.netloc:
            table_parts.insert(0, parsed_uri.netloc.strip())

        base_path = "/".join(table_parts[:-1])

        opts = {
            "bucket_url": f"{self.protocol}://{base_path}",
            "credentials": creds,
            # supresses dlt warnings about dataset name normalization.
            # we don't use dataset names in S3 so it's fine to disable this.
            "enable_dataset_name_normalization": False,
        }
        layout = params.get("layout", [None])[0]
        if layout is not None:
            opts["layout"] = layout

        return BlobFS(**opts)  # type: ignore

    def validate_table(self, table: str):
        table = table.strip("/ ")
        if len(table.split("/")) < 2:
            raise ValueError("Table name must be in the format {bucket-name}/{path}")
        return table

    def dlt_run_params(self, uri: str, table: str, **kwargs):
        table_parts = table.split("/")
        return {
            "table_name": table_parts[-1].strip(),
        }


class S3Destination(BlobStorageDestination):
    @property
    def protocol(self) -> str:
        return "s3"

    def credentials(self, params: dict) -> FileSystemCredentials:
        access_key_id = params.get("access_key_id", [None])[0]
        if access_key_id is None:
            raise MissingValueError("access_key_id", "S3")

        secret_access_key = params.get("secret_access_key", [None])[0]
        if secret_access_key is None:
            raise MissingValueError("secret_access_key", "S3")

        endpoint_url = params.get("endpoint_url", [None])[0]
        if endpoint_url is not None:
            parsed_endpoint = urlparse(endpoint_url)
            if not parsed_endpoint.scheme or not parsed_endpoint.netloc:
                raise ValueError("Invalid endpoint_url. Must be a valid URL.")

        return AwsCredentials(
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            endpoint_url=endpoint_url,
        )


class GCSDestination(BlobStorageDestination):
    @property
    def protocol(self) -> str:
        return "gs"

    def credentials(self, params: dict) -> FileSystemCredentials:
        """Builds GCS credentials from the provided parameters."""
        credentials_path = params.get("credentials_path")
        credentials_base64 = params.get("credentials_base64")
        credentials_available = any(
            map(
                lambda x: x is not None,
                [credentials_path, credentials_base64],
            )
        )
        if credentials_available is False:
            raise MissingValueError("credentials_path or credentials_base64", "GCS")

        credentials = None
        if credentials_path:
            with open(credentials_path[0], "r") as f:
                credentials = json.load(f)
        else:
            credentials = json.loads(base64.b64decode(credentials_base64[0]).decode())  # type: ignore

        return credentials


class BlobFSClient(dlt.destinations.impl.filesystem.filesystem.FilesystemClient):
    @property
    def dataset_path(self):
        # override to remove dataset path
        return self.bucket_path


class BlobFS(dlt.destinations.filesystem):
    @property
    def client_class(self):
        return BlobFSClient
