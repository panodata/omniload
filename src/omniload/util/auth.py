import json
import struct
from dataclasses import dataclass
from typing import Optional

import requests

from omniload.error import MissingValueError

# The fields making up an Azure service-principal credential. All three must be
# supplied together; a partial set is a configuration error, not a fall-through.
AZURE_SERVICE_PRINCIPAL_FIELDS = ("tenant_id", "client_id", "client_secret")


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

    account_name: str
    account_key: Optional[str] = None
    sas_token: Optional[str] = None
    tenant_id: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    account_host: Optional[str] = None

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

    Two auth modes are supported:

    * account-key / SAS: ``account_key`` or ``sas_token``
    * service principal: the full ``tenant_id`` + ``client_id`` +
      ``client_secret`` triplet

    Raises:
        MissingValueError: if ``account_name`` is absent, if no auth material is
            supplied, or if the service-principal triplet is only partially
            supplied (naming the missing field(s)).
        ValueError: if mutually exclusive credentials are supplied together
            (``account_key`` with ``sas_token``, or account-key/SAS material
            with service-principal material), rather than silently picking one.
    """

    def one(key: str) -> Optional[str]:
        return params.get(key, [None])[0]

    account_name = one("account_name")
    if account_name is None:
        raise MissingValueError("account_name", "Azure")

    account_key = one("account_key")
    sas_token = one("sas_token")
    sp_values = {field: one(field) for field in AZURE_SERVICE_PRINCIPAL_FIELDS}
    account_host = one("account_host")

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
            raise MissingValueError(", ".join(missing), "Azure service principal")
    elif not has_shared_key:
        raise MissingValueError(
            "account_key, sas_token, or a service-principal triplet "
            "(tenant_id, client_id, client_secret)",
            "Azure",
        )

    return AzureBlobAuth(
        account_name=account_name,
        account_key=account_key,
        sas_token=sas_token,
        account_host=account_host,
        **sp_values,
    )


def serialize_azure_token(token):
    # https://github.com/mkleehammer/pyodbc/issues/228#issuecomment-494773723
    encoded = token.encode("utf_16_le")
    return struct.pack("<i", len(encoded)) + encoded


def get_databricks_oauth_token(
    server_hostname: str, client_id: str, client_secret: str
) -> str:
    """
    Exchange Databricks OAuth M2M client credentials for an access token.

    This implements the OAuth 2.0 client credentials grant flow for Databricks
    service principal authentication.

    Args:
        server_hostname: The Databricks workspace hostname (e.g., dbc-xxx.cloud.databricks.com)
        client_id: The service principal's client ID (application ID)
        client_secret: The OAuth secret for the service principal

    Returns:
        The access token string

    Raises:
        ValueError: If inputs are invalid or the token request fails
    """  # noqa: E501
    if not server_hostname:
        raise ValueError("server_hostname is required for OAuth token exchange")
    if not client_id:
        raise ValueError("client_id is required for OAuth token exchange")
    if not client_secret:
        raise ValueError("client_secret is required for OAuth token exchange")

    token_url = f"https://{server_hostname}/oidc/v1/token"

    try:
        response = requests.post(
            token_url,
            data={
                "grant_type": "client_credentials",
                "scope": "all-apis",
            },
            auth=(client_id, client_secret),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30,
        )
    except requests.exceptions.RequestException as e:
        raise ValueError(
            f"Failed to connect to Databricks OAuth endpoint at {token_url}: {e}"
        ) from e

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        raise ValueError(
            f"Failed to obtain Databricks OAuth token: HTTP {response.status_code}"
        ) from e

    try:
        token_data = response.json()
    except (json.JSONDecodeError, ValueError) as e:
        raise ValueError("Invalid JSON response from Databricks OAuth endpoint") from e

    if "access_token" not in token_data:
        raise ValueError("Databricks OAuth response missing 'access_token' field")

    return token_data["access_token"]
