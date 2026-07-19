(onedrive)=

# OneDrive

[OneDrive] is a file-hosting service operated by Microsoft. it allows
registered users to store, share, back-up and synchronize their files.
OneDrive also works as the storage backend of the web version of
Microsoft 365.
`omniload` supports OneDrive as a data source.

## URI format

The URI for connecting to OneDrive is structured as follows.

```text
onedrive://<drive_name>/path/to/data.xlsx?client_id=1d2befad-2f22-4124-a779-b147dfeca342&tenant_id=6b337423-f504-4060-a91b-e9eaaf782609&client_secret=abc~xyz789EXAMPLE_foo
```

## URI parameters

:drive_id:
  The ID of the OneDrive drive. If provided, enables single-site mode.

:client_id:
  OAuth2 client ID. Can also be set via MSGRAPHFS_CLIENT_ID
  or AZURE_CLIENT_ID environment variables.

:tenant_id:
  OAuth2 tenant ID. Can also be set via MSGRAPHFS_TENANT_ID
  or AZURE_TENANT_ID environment variables.

:client_secret:
  OAuth2 client secret. Can also be set via MSGRAPHFS_CLIENT_SECRET
  or AZURE_CLIENT_SECRET environment variables.

:site_name:
  The name of the OneDrive site. If provided with drive_name,
  enables single-site mode.

:drive_name:
  The name of the OneDrive drive/library (e.g., "Documents",
  "CustomLibrary"). If provided with `site_name`, enables
  single-site mode.

:url_path:
  URL-style path specification (e.g., "msgd://TestSite/Documents").
  If provided, extracts `site_name` and `drive_name` from the URL.
  URL parameters override direct `site_name`/`drive_name` parameters.

:oauth2_client_params:
  Parameters for the OAuth2 client. If not provided, will be built
  from `client_id`, `tenant_id`, `client_secret`.
  Type: `dict`. Use JSON to encode the dictionary.

:use_recycle_bin:
  If True, deleted files are moved to recycle bin. Default is False.
  Truthy values are `"true", "yes", "on", "y", "t", "1"`.
  Falsy values are `"false", "no", "off", "n", "f", "0"`.

:::{note}
Access works unified for both OneDrive sites and drives.
The module handles both single-site/drive operations and multi-site
operations based on the parameters provided during initialization:

- Single-site mode: When `site_name` + `drive_name` or `drive_id` are provided
- Multi-site mode: When neither `site_name` + `drive_name` nor `drive_id` are provided

Multi-site mode can handle URL-based paths that specify
the site and drive dynamically (e.g., `msgd://SiteA/DriveB/file.txt`).
:::

## Authentication

OneDrive uses OAuth 2.0 for authentication.

- [Set up OAuth 2.0 authentication for OneDrive]

## Example: Load CSV file from OneDrive into DuckDB

```sh
omniload ingest \
    --source-uri   'onedrive://?client_id=1d2befad-2f22-4124-a779-b147dfeca342&tenant_id=6b337423-f504-4060-a91b-e9eaaf782609&client_secret=abc~xyz789EXAMPLE_foo' \
    --source-table '<site_name>/<drive_name>/path/to/user.csv' \
    --dest-uri     'duckdb:///OneDrive_data.duckdb' \
    --dest-table   'dest.users_details'
```

Running the command creates a table named `users_details` within the
`dest` schema in the DuckDB database file located at `OneDrive_data.duckdb`.

:::{tip}
Here, instead of defining the remote resource exclusively per source URI
using its `<path>` component, the `--source-table` option can specify the
base directory on the server where `omniload` should start looking for files.
:::


[OneDrive]: https://en.wikipedia.org/wiki/OneDrive
[Set up OAuth 2.0 authentication for OneDrive]: https://docs.aws.amazon.com/bedrock/latest/userguide/kb-managed-onedrive-oauth2-setup.html
