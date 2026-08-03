import base64
import json
from dataclasses import dataclass
from typing import Any, Optional

from dlt_filesystem.error import MissingConnectorOption

AZURE_SERVICE_PRINCIPAL_FIELDS = ("tenant_id", "client_id", "client_secret")


def _first(params: dict[str, list[str]], key: str) -> Optional[str]:
    """Return the first query-parameter value, matching existing URI semantics."""
    return params.get(key, [None])[0]


def s3_filesystem_kwargs(
    params: dict[str, list[str]], connector: str = "S3"
) -> dict[str, Any]:
    """Translate omniload S3 URI parameters into ``s3fs`` arguments."""
    access_key_id = _first(params, "access_key_id")
    if not access_key_id:
        raise MissingConnectorOption("access_key_id", connector)

    secret_access_key = _first(params, "secret_access_key")
    if not secret_access_key:
        raise MissingConnectorOption("secret_access_key", connector)

    kwargs: dict[str, Any] = {
        "key": access_key_id,
        "secret": secret_access_key,
        # S3FileSystem caches directory listings by default. Disable the cache so
        # long-lived processes see objects created between incremental runs.
        "use_listings_cache": False,
    }
    endpoint_url = _first(params, "endpoint_url")
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
    return kwargs


def gcs_filesystem_kwargs(
    params: dict[str, list[str]], inherited: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Translate omniload GCS URI parameters into ``gcsfs`` arguments."""
    kwargs = dict(inherited or {})
    remaining = {key: list(values) for key, values in params.items()}
    credentials_path = _first(remaining, "credentials_path")
    credentials_base64 = _first(remaining, "credentials_base64")
    remaining.pop("credentials_path", None)
    remaining.pop("credentials_base64", None)

    for key, values in remaining.items():
        kwargs.setdefault(key, values[0])

    if "token" not in kwargs:
        if credentials_path:
            kwargs["token"] = credentials_path
        elif credentials_base64:
            kwargs["token"] = json.loads(base64.b64decode(credentials_base64).decode())
        else:
            kwargs["token"] = "anon"  # noqa: S105 - gcsfs anonymous-access sentinel
    return kwargs


@dataclass
class AzureBlobAuth:
    """Resolved Azure blob-storage credentials parsed from URI query params.

    Holds the ingestr-style short names (``account_name`` / ``account_key`` /
    ``sas_token`` / ``tenant_id`` / ``client_id`` / ``client_secret`` /
    ``account_host``). These names match ``adlfs.AzureBlobFileSystem`` kwargs
    exactly, so the source can pass them straight through; the destination maps
    them onto dlt's ``AzureCredentials`` / ``AzureServicePrincipalCredentials``
    spec fields.
    """

    account_name: Optional[str] = None
    account_key: Optional[str] = None
    sas_token: Optional[str] = None
    tenant_id: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    account_host: Optional[str] = None
    connection_string: Optional[str] = None
    api_version: Optional[str] = None

    @property
    def is_service_principal(self) -> bool:
        """True when a full service-principal triplet is present.

        Derived from all three fields (not just ``tenant_id``) so it stays
        honest even if an ``AzureBlobAuth`` is built outside ``parse_azure_blob_auth``.
        """
        return (
            self.tenant_id is not None
            and self.client_id is not None
            and self.client_secret is not None
        )


def parse_azure_blob_auth(params: dict) -> AzureBlobAuth:
    """Parse and validate Azure blob-storage credentials from URI query params.

    ``params`` is the ``urllib.parse.parse_qs`` output (each value is a list).
    Values must be URL-encoded in the URI: Azure account keys are base64
    (``+`` / ``/`` / ``=``) and SAS tokens embed their own ``&`` / ``=`` pairs,
    which ``parse_qs`` would otherwise mangle (``+`` becomes a space, an
    unencoded SAS token shatters into junk params).

    Three auth modes are supported:

    * connection string: ``connection_string``
    * account-key / SAS: ``account_key`` or ``sas_token``
    * service principal: the full ``tenant_id`` + ``client_id`` +
      ``client_secret`` triplet

    Raises:
        MissingValueError: if ``account_name`` is absent, if no auth material is
            supplied, or if the service-principal triplet is only partially
            supplied (naming the missing field(s)).
        ValueError: if mutually exclusive credentials are supplied together,
            rather than silently picking one.
    """

    connection_string = _first(params, "connection_string")
    api_version = _first(params, "api_version")
    account_name = _first(params, "account_name")
    account_key = _first(params, "account_key")
    sas_token = _first(params, "sas_token")
    sp_values = {
        field: _first(params, field) for field in AZURE_SERVICE_PRINCIPAL_FIELDS
    }
    account_host = _first(params, "account_host")

    if connection_string is not None:
        conflicting_fields = [
            field
            for field, value in {
                "account_name": account_name,
                "account_key": account_key,
                "sas_token": sas_token,
                "account_host": account_host,
                **sp_values,
            }.items()
            if value is not None
        ]
        if conflicting_fields:
            raise ValueError(
                "Conflicting Azure credentials: connection_string cannot be "
                f"combined with {', '.join(conflicting_fields)}."
            )
        return AzureBlobAuth(
            connection_string=connection_string,
            api_version=api_version,
        )

    if account_name is None:
        raise MissingConnectorOption("account_name", "Azure")

    if account_key is not None and sas_token is not None:
        raise ValueError(
            "Conflicting Azure credentials: supply either account_key or "
            "sas_token, not both."
        )

    has_shared_key = account_key is not None or sas_token is not None
    supplied_sp_fields = [f for f, v in sp_values.items() if v is not None]
    has_service_principal = len(supplied_sp_fields) > 0

    if has_shared_key and has_service_principal:
        raise ValueError(
            "Conflicting Azure credentials: supply either account_key/sas_token "
            "or the service-principal triplet (tenant_id, client_id, "
            "client_secret), not both."
        )

    if has_service_principal:
        missing = [f for f in AZURE_SERVICE_PRINCIPAL_FIELDS if sp_values[f] is None]
        if missing:
            raise MissingConnectorOption(", ".join(missing), "Azure service principal")
    elif not has_shared_key:
        raise MissingConnectorOption(
            "account_key, sas_token, or a service-principal triplet "
            "(tenant_id, client_id, client_secret)",
            "Azure",
        )

    return AzureBlobAuth(
        account_name=account_name,
        account_key=account_key,
        sas_token=sas_token,
        account_host=account_host,
        connection_string=connection_string,
        api_version=api_version,
        **sp_values,
    )


def azure_blob_filesystem_kwargs(auth: AzureBlobAuth) -> dict[str, Any]:
    """Translate parsed Azure credentials into ``adlfs`` arguments."""
    kwargs: dict[str, Any] = {"account_name": auth.account_name}
    if auth.account_key is not None:
        kwargs["account_key"] = auth.account_key
    if auth.sas_token is not None:
        kwargs["sas_token"] = auth.sas_token
    if auth.tenant_id is not None:
        kwargs["tenant_id"] = auth.tenant_id
    if auth.client_id is not None:
        kwargs["client_id"] = auth.client_id
    if auth.client_secret is not None:
        kwargs["client_secret"] = auth.client_secret
    if auth.account_host is not None:
        kwargs["account_host"] = auth.account_host
    if auth.connection_string is not None:
        kwargs["connection_string"] = auth.connection_string
    if auth.api_version is not None:
        kwargs["api_version"] = auth.api_version
    return kwargs
