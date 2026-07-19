(sharepoint)=

# SharePoint

[SharePoint] is a web-based collaborative platform primarily used for building
corporate intranets, document and content management, and file sharing. 
`omniload` supports SharePoint as a data source.

## URI format

The URI for connecting to SharePoint is structured as follows.

```text
sharepoint://<site_name>/<drive_name>/path/to/data.xlsx?client_id=1d2befad-2f22-4124-a779-b147dfeca342&tenant_id=6b337423-f504-4060-a91b-e9eaaf782609&client_secret=abc~xyz789EXAMPLE_foo
```

## URI parameters

:drive_id:
  The ID of the SharePoint drive. If provided, enables single-site mode.

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
  The name of the SharePoint site. If provided with drive_name,
  enables single-site mode.

:drive_name:
  The name of the SharePoint drive/library (e.g., "Documents",
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
Access works unified for both SharePoint sites and drives.
The module handles both single-site/drive operations and multi-site
operations based on the parameters provided during initialization:

- Single-site mode: When `site_name` + `drive_name` or `drive_id` are provided
- Multi-site mode: When neither `site_name` + `drive_name` nor `drive_id` are provided

Multi-site mode can handle URL-based paths that specify
the site and drive dynamically (e.g., `msgd://SiteA/DriveB/file.txt`).
:::

## Authentication

SharePoint uses OAuth 2.0 for authentication.

- [Mastering File Access in SharePoint with OAuth 2.0: A Comprehensive Guide]
- [Understanding Microsoft Entra ID and OAuth 2.0 in the context of SharePoint Online modern development]

## Example: Load CSV file from SharePoint into DuckDB

```sh
omniload ingest \
    --source-uri   'sharepoint://?client_id=1d2befad-2f22-4124-a779-b147dfeca342&tenant_id=6b337423-f504-4060-a91b-e9eaaf782609&client_secret=abc~xyz789EXAMPLE_foo' \
    --source-table '<site_name>/<drive_name>/path/to/user.csv' \
    --dest-uri     'duckdb:///sharepoint_data.duckdb' \
    --dest-table   'dest.users_details'
```

Running the command creates a table named `users_details` within the
`dest` schema in the DuckDB database file located at `sharepoint_data.duckdb`.

:::{tip}
Here, instead of defining the remote resource exclusively per source URI
using its `<path>` component, the `--source-table` option can specify the
base directory on the server where `omniload` should start looking for files.
:::


[Mastering File Access in SharePoint with OAuth 2.0: A Comprehensive Guide]: https://medium.com/@pavithrasainath7/mastering-file-access-in-sharepoint-with-oauth-2-0-a-comprehensive-guide-0a6b2d53736a
[SharePoint]: https://en.wikipedia.org/wiki/SharePoint
[Understanding Microsoft Entra ID and OAuth 2.0 in the context of SharePoint Online modern development]: https://learn.microsoft.com/en-us/sharepoint/dev/sp-add-ins-modernize/understanding-aad-and-oauth-for-spo-modern
