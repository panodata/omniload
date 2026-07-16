(webdav)=

# WebDAV

[WebDAV] (Web Distributed Authoring and Versioning) is a set of extensions to
the Hypertext Transfer Protocol (HTTP) allowing user agents to collaboratively
edit content directly in an HTTP server, which may act as a file server.
`omniload` supports WebDAV as a data source.

## URI format

The URI for connecting to WebDAV is structured as follows.

```text
https+webdav://<USERNAME>:<PASSWORD>@www.example.org/path/to/data.parquet
```

## URI parameters

:retry: Disable retry on the HTTP client. When enabled, some well-known
  errors are handled and retried a few times with backoff.
  Type: `bool`. Default: `true`.

:chunk_size:
  The chunk size in bytes.
  Type: `int`. Default: `4194304` (`4 MB`).

:headers:
  HTTP headers to include when sending requests.
  Type: `dict`. Use JSON to encode the dictionary.

:cookies:
  Cookie items to include when sending requests.
  Type: `dict`. Use JSON to encode the dictionary.

:verify:
  SSL certificates used to verify the identity of requested
  hosts. Can be any of:

  - `true`: Use default CA bundle.
  - `false`: Disable verification.
  - Path to an SSL certificate file.

  Default: `true`.

:cert:
  An SSL certificate used by the requested host to
  authenticate the client.
  Either a path to an SSL certificate file,
  or two-tuple of (certificate file, key file),
  or a three-tuple of (certificate file, key file, password).
  Note: Tuple decoding is not implemented yet.

:proxies:
  A mapping of proxy keys to proxy URLs.
  Type: `dict`. Use JSON to encode the dictionary.

:timeout:
  The socket timeout in seconds.
  Type: `float`.

:max_redirects:
  The maximum number of redirect responses that should be followed.
  Type: `int`.

:trust_env:
  Enable or disable usage of environment variables for configuration.
  Type: `bool`. Default: `true`.

## Authentication

To integrate `omniload` with WebDAV, you need to authenticate like you
do with any HTTP server.

:::{note}
The module currently forwards parameters for HTTP Basic authentication
using username/password credentials. In theory, all [authentication types
supported by HTTPX] can be unlocked. Please [create an issue] to let us
know about your needs.
:::

## Example: Load CSV file from WebDAV into DuckDB

```sh
omniload ingest \
    --source-uri   'https+webdav://<USERNAME>:<PASSWORD>@www.example.org' \
    --source-table 'path/to/data.csv' \
    --dest-uri     'duckdb:///demo.duckdb' \
    --dest-table   'testdrive.data'
```

Running the command creates a table named `data` within the `testdrive`
schema in the DuckDB database file located at `demo.duckdb`.

:::{tip}
Here, instead of defining the remote resource exclusively per source URI
using its `<path>` component, the `--source-table` option can specify the
base directory on the server where `omniload` should start looking for files.
:::


[authentication types supported by HTTPX]: https://www.python-httpx.org/advanced/authentication/
[create an issue]: https://github.com/panodata/omniload/issues
[WebDAV]: https://en.wikipedia.org/wiki/WebDAV
