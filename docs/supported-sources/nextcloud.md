(nextcloud)=

# Nextcloud

[Nextcloud Files] is a secure cloud storage and file sharing software for easy
sync, sharing and collaboration on your files. It uses the {ref}`webdav`
connector.
`omniload` supports Nextcloud as a data source.

## URI format

The URI for connecting to Nextcloud is structured as follows.
```text
https+webdav://<USERNAME>:<PASSWORD>@cloud.example.org/remote.php/webdav
```

## Authentication

To integrate `omniload` with Nextcloud, you need to authenticate like you
do with any HTTP server.

## Example: Load CSV file from Nextcloud into DuckDB

```sh
omniload ingest \
    --source-uri   'https+webdav://<USERNAME>:<PASSWORD>@cloud.example.org/remote.php/webdav' \
    --source-table 'path/to/data.csv' \
    --dest-uri     'duckdb:///demo.duckdb' \
    --dest-table   'testdrive.data'
```

Running the command creates a table named `data` within the `testdrive`
schema in the DuckDB database file located at `demo.duckdb`.


[Nextcloud Files]: https://nextcloud.com/files/
