(webhdfs)=

# WebHDFS

[WebHDFS] provides a complete FileSystem interface for HDFS over HTTP.
Supports also HttpFS gateways.
`omniload` supports WebHDFS as a data source.

## URI Format

The URI for connecting to WebHDFS is structured as follows. Either access the resource
anonymously, use key/secret credentials, or a security token for authentication.
In this form, the remote resource is addressed exclusively using a single
parameter, an URI, encoding all required options.

```text
webhdfs://host:9870/path
```

## URI parameters

:host: str
  Name-node address.

:port: int
  Port for webHDFS server.

:kerberos:
  Whether to authenticate with kerberos for this connection.
  Type: `bool`. Default: `false`.

:token:
  If given, use this token on every call to authenticate. A user
  and user-proxy may be encoded in the token and should not be also
  given.

:user:
  If given, assert the user name to connect with.

:password:
  If given, assert the password to use for basic auth. If password
  is provided, user must be provided also.

:proxy_to:
  If given, the user has the authority to proxy, and this value is
  the user in who's name actions are taken.

:kerberos_options:
  Any extra arguments for HTTPKerberosAuth, see [kerberos_.py].
  Type: `dict`. Use JSON to encode the dictionary.

:data_proxy:
  Map host names / data-node addresses `host->data_proxy[host]`.
  This can be necessary if the HDFS cluster is behind a proxy,
  running on Docker or otherwise has a mismatch between the
  host-names given by the name-node and the address by which to
  refer to them from the client.
  Type: `dict`. Use JSON to encode the dictionary.

:use_https:
  Whether to connect to the Name-node using HTTPS instead of HTTP.
  Type: `bool`. Default: `false`.

:session_cert:
  Path to a certificate file, or tuple of (cert, key) files to use
  for the conversation.
  Remark: Decoding tuples is not implemented yet.

:session_verify:
  Whether to verify the conversation, or the path to a certificate file
  for doing so.
  Type: `str|bool`. Default: `true`.

## Authentication

Four authentication mechanisms are supported.

:insecure:
  No authentication is performed, and the user is assumed to be whoever they
  say they are (parameter `user`), or a predefined value such as "dr.who"
  if not given.

:spnego:
  When kerberos authentication is enabled, authentication is negotiated by
  [requests_kerberos].
  This establishes a session based on existing kinit login and/or
  specified principal/password; parameters are passed with `kerberos_options`.

:token:
  Uses an existing Hadoop delegation token from another secured
  service. Indeed, this client can also generate such tokens when
  not insecure. Note that tokens expire, but can be renewed (by a
  previously specified user) and may allow for proxying.

:basic-auth:
  Used when both parameter `user` and parameter `password` are provided.

## Examples

### Load Parquet data from WebHDFS into DuckDB

The following command demonstrates how to copy data from a specified OSS
location into a DuckDB database.

```sh
omniload ingest \
    --source-uri   'webhdfs://host:9870/path' \
    --source-table 'path/to/data.parquet' \
    --dest-uri     'duckdb:///demo.duckdb' \
    --dest-table   'testdrive.data'
```

Running the command creates a table named `data` within the `testdrive`
schema in the DuckDB database file located at `demo.duckdb`.


[kerberos_.py]: https://github.com/requests/requests-kerberos/blob/master/requests_kerberos/kerberos_.py
[requests_kerberos]: https://github.com/requests/requests-kerberos
[WebHDFS]: https://hadoop.apache.org/docs/r1.0.4/webhdfs.html
