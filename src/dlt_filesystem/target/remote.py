import abc
import base64
import json
from urllib.parse import parse_qs, urlparse

import dlt.destinations.impl.filesystem.filesystem
from dlt.common.configuration.specs import (
    AwsCredentials,
    AzureCredentials,
    AzureServicePrincipalCredentials,
)
from dlt.common.storages.configuration import FileSystemCredentials

from dlt_filesystem.error import MissingConnectorOption
from dlt_filesystem.util.auth import parse_azure_blob_auth


class BlobStorageDestination(abc.ABC):
    def supports_multiple_tables(self) -> bool:
        """The current blob destination contract addresses one table path."""
        return False

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
        filesystem_kwargs = self.filesystem_kwargs(params)
        if filesystem_kwargs:
            opts["kwargs"] = filesystem_kwargs

        return BlobFS(**opts)  # type: ignore

    def filesystem_kwargs(self, params: dict) -> dict:
        """Return extra fsspec constructor arguments for the destination."""
        return {}

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

    def post_load(self) -> None:  # noqa: B027
        # Blob-storage destinations write the dlt layout directly to the bucket;
        # unlike Athena/Clickhouse there is no follow-up fixup to run. The hook
        # is required by DestinationProtocol (run_ingest calls it directly), so
        # it must exist as a concrete no-op default that subclasses inherit
        # (deliberately not @abstractmethod, hence noqa B027).
        pass


class S3Destination(BlobStorageDestination):
    @property
    def protocol(self) -> str:
        return "s3"

    def credentials(self, params: dict) -> FileSystemCredentials:
        access_key_id = params.get("access_key_id", [None])[0]
        if access_key_id is None:
            raise MissingConnectorOption("access_key_id", "S3")

        secret_access_key = params.get("secret_access_key", [None])[0]
        if secret_access_key is None:
            raise MissingConnectorOption("secret_access_key", "S3")

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
            map(  # noqa: C417
                lambda x: x is not None,
                [credentials_path, credentials_base64],
            )
        )
        if credentials_available is False:
            raise MissingConnectorOption(
                "credentials_path or credentials_base64", "GCS"
            )

        credentials = None
        if credentials_path:
            with open(credentials_path[0], "r") as f:
                credentials = json.load(f)
        else:
            credentials = json.loads(base64.b64decode(credentials_base64[0]).decode())  # type: ignore

        return credentials


class AzureDestination(BlobStorageDestination):
    """Azure Blob Storage / ADLS Gen2 destination (``az://``, ``adls://``, ``abfss://``).

    The ``az://`` bucket-url scheme serves both Blob and ADLS Gen2 (they differ
    only by the account's hierarchical namespace), so ``adls://`` / ``abfss://``
    are registry aliases onto this class and the fsspec backend only ever sees
    ``az://``.
    """

    @property
    def protocol(self) -> str:
        return "az"

    def credentials(self, params: dict) -> FileSystemCredentials:
        """Build dlt Azure credentials from URI query params.

        Maps the ingestr-style short names onto dlt's spec fields; dlt's
        ``to_adlfs_credentials()`` handles the fsspec mapping downstream. Only
        supplied fields are set (dlt's spec fields default to ``None`` but are
        typed non-optional), which also narrows the parsed optionals for ``ty``.
        """
        auth = parse_azure_blob_auth(params)

        if auth.connection_string is not None:
            # dlt does not model Azure connection strings in its credential union.
            # The actual credential is forwarded through filesystem_kwargs; a
            # resolved empty object prevents dlt from probing for unrelated account
            # fields before it constructs the adlfs filesystem.
            return AzureCredentials().resolve()

        if auth.is_service_principal:
            # parse_azure_blob_auth guarantees the full triplet here; the
            # per-field `is not None` guards mirror the account-key branch and
            # narrow the parsed optionals to str for ty.
            sp_credentials = AzureServicePrincipalCredentials()
            if auth.account_name is not None:
                sp_credentials.azure_storage_account_name = auth.account_name
            if auth.tenant_id is not None:
                sp_credentials.azure_tenant_id = auth.tenant_id
            if auth.client_id is not None:
                sp_credentials.azure_client_id = auth.client_id
            if auth.client_secret is not None:
                sp_credentials.azure_client_secret = auth.client_secret
            if auth.account_host is not None:
                sp_credentials.azure_account_host = auth.account_host
            return sp_credentials

        key_credentials = AzureCredentials()
        if auth.account_name is not None:
            key_credentials.azure_storage_account_name = auth.account_name
        if auth.account_key is not None:
            key_credentials.azure_storage_account_key = auth.account_key
        if auth.sas_token is not None:
            key_credentials.azure_storage_sas_token = auth.sas_token
        if auth.account_host is not None:
            key_credentials.azure_account_host = auth.account_host
        return key_credentials

    def filesystem_kwargs(self, params: dict) -> dict:
        """Forward connection-string credentials that dlt does not model."""
        auth = parse_azure_blob_auth(params)
        if auth.connection_string is None:
            return {}
        # adlfs does not forward api_version when constructing a client from a
        # connection string, so do not pass that parsed option here.
        return {"connection_string": auth.connection_string}


#: Blob protocols whose URI query parameters dlt models as typed credentials.
#: Every other protocol reaches its fsspec backend as plain constructor arguments.
_CREDENTIAL_BUILDERS: dict[str, type[BlobStorageDestination]] = {
    "az": AzureDestination,
    "gs": GCSDestination,
    "s3": S3Destination,
}


def blob_destination_options(protocol: str, params: dict) -> dict:
    """Build the dlt filesystem options a blob protocol derives from URI query params.

    ``params`` is the ``urllib.parse.parse_qs`` output. The return value carries
    ``credentials``, and ``kwargs`` where the protocol forwards anything dlt does
    not model, ready to splat into a filesystem destination.

    An empty query returns no options, so the caller omits ``credentials`` and dlt
    resolves them from the environment as it does for a bare bucket URL. A
    non-empty query is built strictly instead: a partial credential set raises
    ``MissingConnectorOption`` naming the missing option, because a URI that
    carries half a credential asked for that credential to be used.
    """
    builder_class = _CREDENTIAL_BUILDERS.get(protocol)
    if builder_class is None or not params:
        return {}

    builder = builder_class()
    options: dict = {"credentials": builder.credentials(params)}
    filesystem_kwargs = builder.filesystem_kwargs(params)
    if filesystem_kwargs:
        options["kwargs"] = filesystem_kwargs
    return options


class BlobFSClient(dlt.destinations.impl.filesystem.filesystem.FilesystemClient):
    @property
    def dataset_path(self):
        # override to remove dataset path
        return self.bucket_path


class BlobFS(dlt.destinations.filesystem):
    @property
    def client_class(self):
        return BlobFSClient
