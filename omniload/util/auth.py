import json
import struct

import requests


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
