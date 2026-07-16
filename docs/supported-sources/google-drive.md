(gdrive)=
(google-drive)=

# Google Drive

[Google Drive] is a secure cloud storage platform for seamless file sharing
and enhanced collaboration.
`omniload` supports access to files on Google Drive as a source.

## URI format

The URI for connecting to Google Drive files is structured as follows.

```text
gdrive://path/to/data.parquet?token=anon
```

## URI parameters

:root_file_id:
  If you have a share, drive or folder ID to treat as the FS root, enter
  it here. Otherwise, you will get your default drive
  Default: None.

:token:
  One of "anon", "browser", "cache", "service_account". Using "browser" will prompt a URL to
  be put in a browser, and cache the response for future use with token="cache".
  "browser" will remove any previously cached token file, if it exists.

:access:
  One of `full_control` or `read_only`.

:spaces:
  Category of files to search; can be `drive`, `appDataFolder` or `photos`.
  Of these, only the first is general.

:creds:
  Required just for "service_account" token, a dict containing the service account
  credentials obtained in GCP console. The dict content is the same as the json file
  downloaded from GCP console. See also [service account keys].
  This credential can be useful when integrating with other GCP services, and when you
  don't want the user to be prompted to authenticate.
  The files need to be shared with the service account email address, that can be found
  in the json file.
  Type: `dict`. Use JSON to encode the dictionary.

:auth_kwargs:
  Additional keyword arguments passed to the authentication backend
  (`pydata_google_auth.get_user_credentials` for user OAuth, or
  `service_account.Credentials.from_service_account_info` for service
  accounts). For headless or remote environments where a local callback
  server is unavailable, pass `use_local_webserver=False` to request a
  token via the console.
  Type: `dict`. Use JSON to encode the dictionary.

:use_local_webserver:
  Type: `bool`. Default: `true`. 

## Authentication

There are several methods to authenticate with Google Drive.

### 1. Service account credentials

In this method, you provide a dictionary containing the service account
credentials obtained in the GCP console. The dictionary content is the
same as the JSON file downloaded from the GCP console. See also
[service account keys].

This credential can be useful when integrating with other GCP services,
and when you don't want the user to be prompted to authenticate.

Example:
```text
?token=service_account&creds=%7B%22type%22%3A%22service_account%22%2C%22project_id%22%3A%22my-project%22%2C%22private_key_id%22%3A%22key-id%22%2C%22private_key%22%3A%22-----BEGIN%20PRIVATE%20KEY-----%5Cn...%5Cn-----END%20PRIVATE%20KEY-----%5Cn%22%2C%22client_email%22%3A%22omniload%40my-project.iam.gserviceaccount.com%22%2C%22client_id%22%3A%221234567890%22%2C%22token_uri%22%3A%22https%3A%2F%2Foauth2.googleapis.com%2Ftoken%22%7D
```

In this example, the JSON object from a downloaded Google service-account
key is percent-encoded as one `creds` query value.

#### 2. OAuth with user credentials

A browser will be opened to complete the OAuth authentication flow.
Afterwards, the access token will be stored locally, and you can reuse
it in subsequent sessions.

Example:
```text
?token=browser
```

On headless or remote machines (SSH sessions, containers, CI, and similar
environments), you may not be able to bind a local callback server or open
a browser on the same host. In that case, pass `use_local_webserver: False`
in `auth_kwargs` to request a token via the console.

Example:
```text
?token=browser&use_local_webserver=false
```

#### 3. Anonymous (read-only) access

If you want to interact with files that are shared publicly ("anyone with
the link"), then you do not need to authenticate to Google Drive.

Example:
```text
?token=anon
```

## Example: Load Parquet file from Google Drive into DuckDB

```sh
omniload ingest \
    --source-uri   'gdrive://path/to/data.parquet?token=anon' \
    --dest-uri     'duckdb:///demo.duckdb' \
    --dest-table   'testdrive.data'
```

Running the command creates a table named `data` within the `testdrive`
schema in the DuckDB database file located at `demo.duckdb`.


[Google Drive]: https://workspace.google.com/products/drive/
[service account keys]: https://docs.cloud.google.com/iam/docs/service-account-creds#key-types
