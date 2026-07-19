(ftp)=

# FTP

The [File Transfer Protocol (FTP)] is a standard communication protocol used
for the transfer of computer files from a server to a client over a computer
network.
`omniload` supports FTP as a data source.

## URI format

The URI for connecting to an FTP server is structured as follows.

```text
ftp://username:password@intranet.example.org/path/to/data.parquet?tls=tls
```

## URI parameters

:host:
  The remote server name/ip to connect to.

:port:
  Port to connect with.
  Type: `int`. Default: `21`.

:username:
  The username for authenticating (optional).

:password:
  The password for authenticating (optional).

:acct:
  Some servers also need an "account" string for authentication.

:block_size:
  The read-ahead or write buffer size in bytes.
  Type: `int`. Default: `65536`.

:tempdir:
  Directory on remote to put temporary files when in a transaction.

:timeout:
  Timeout of the FTP connection in seconds.
  Type: `int`. Default: `30`.

:encoding:
  Encoding to use for directories and filenames in FTP connection.
  Default: `utf-8`.

:tls:
  Enable FTP-TLS for secure connections.
  Type: `bool` or `str`. Default: `False`.
  Accepted values are:

  - `false`: Use plain FTP (default).
  - `true`: Use explicit TLS (FTPS with AUTH TLS command).
  - `tls`: Auto-negotiate the highest protocol.
  - `tlsv1`: TLS v1.0
  - `tlsv1_1`: TLS v1.1
  - `tlsv1_2`: TLS v1.2

## Authentication

Authentication will be anonymous if username/password credentials are not
provided.

## Examples

To integrate `omniload` with an FTP server, you need the server's
hostname, port, a valid username, and a password.

### Load CSV data from FTP into DuckDB

```sh
omniload ingest \
    --source-uri   'ftp://username:password@intranet.example.org?tls=tls' \
    --source-table '/path/to/user.csv' \
    --dest-uri     'duckdb:///ftp_data.duckdb' \
    --dest-table   'dest.users_details'
```

Running the command creates a table named `users_details` within the
`dest` schema in the DuckDB database file located at `ftp_data.duckdb`.

:::{tip}
Here, instead of defining the remote resource exclusively per source URI
using its `<path>` component, the `--source-table` option can specify the
base directory on the server where `omniload` should start looking for files.
:::


[File Transfer Protocol (FTP)]: https://en.wikipedia.org/wiki/File_Transfer_Protocol
