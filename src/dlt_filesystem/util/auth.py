from dataclasses import dataclass
from typing import Optional

from dlt_filesystem.error import MissingConnectorOption

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
        raise MissingConnectorOption("account_name", "Azure")

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
        **sp_values,
    )
