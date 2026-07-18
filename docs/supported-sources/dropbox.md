(dropbox)=

# Dropbox

[Dropbox] is a file hosting service that offers cloud storage, file
synchronization, personal cloud, and client software.
`omniload` supports Dropbox as a data source.

## URI format

The URI for connecting to Dropbox is structured as follows.

```text
dropbox://path/to/data.parquet?token=secret
```

## URI parameters

:token:
  Generated key by adding a dropbox app in the user dropbox account.

## Authentication

To integrate `omniload` with Dropbox, you need to authenticate with the
Dropbox API using an access token.
See [generate an access token for your own account].

## Example: Load CSV file from Dropbox into DuckDB

```sh
omniload ingest \
    --source-uri   'dropbox://?token=secret' \
    --source-table 'path/to/user.csv' \
    --dest-uri     'duckdb:///dropbox_data.duckdb' \
    --dest-table   'dest.users_details'
```

Running the command creates a table named `users_details` within the
`dest` schema in the DuckDB database file located at `dropbox_data.duckdb`.

:::{tip}
Here, instead of defining the remote resource exclusively per source URI
using its `<path>` component, the `--source-table` option can specify the
base directory on the server where `omniload` should start looking for files.
:::


[Dropbox]: https://www.dropbox.com/dropbox
[generate an access token for your own account]: https://dropbox.tech/developers/generate-an-access-token-for-your-own-account
