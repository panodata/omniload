(smb)=

# SMB

[Server Message Block (SMB)] is a communication protocol used to share files,
printers, serial ports, and miscellaneous communications between nodes on a
network, mostly used by Microsoft Windows operating systems.

`omniload` supports SMB as a data source for reading files on Microsoft
Windows Server Shares.

## URI Format

The URI for connecting to SMB is structured as follows. Authentication
either works transparently using NTLM or Kerberos, or by using
username/password credentials.

```text
smb://workgroup;user:password@server.example.org:445/path/to/data.parquet
```

## URI parameters

:host:
  The remote server name or IP address to connect to.

:port:
  Port to connect with. Usually `445`, sometimes `139`.
  Type: `int`. Default: `445`.

:username:
  Username to connect with.
  Required when not using Kerberos authentication.

:password:
  User's password on the server, if using username.

:timeout:
  Connection timeout in seconds.
  Type: `int`.

:encrypt:
  Whether to force encryption or not. Once this has been set to `True`
  the session cannot be changed back to `False`.
  Type: `bool`. Default: `False`.

:share_access:
  Specifies the default access mode applied to file `open` operations
  performed with this file system object.
  This affects whether other processes can concurrently open a handle
  to the same file.

  - `None`: exclusively locks the file until closed (default).
  - `r`: Allow other handles to be opened with read access.
  - `w`: Allow other handles to be opened with write access.
  - `d`: Allow other handles to be opened with delete access.

:register_session_retries:
  Number of retries to register a session with the server. Retries are not performed
  for authentication errors, as they are considered as invalid credentials and not network
  issues. If set to negative value, no register attempts will be performed.
  Type: `int`. Default: `4`.

:register_session_retry_wait:
  Time in seconds to wait between each retry. Number must be non-negative.
  Type: `int`. Default: `1`.

:register_session_retry_factor:
  Base factor for the wait time between each retry. The wait time
  is calculated using the exponential function. For `factor=1` all wait times
  will be equal to `register_session_retry_wait`. For any number of retries,
  the last wait time will be equal to `register_session_retry_wait` and for retries>1
  the first wait time will be equal to `register_session_retry_wait / factor`.
  Number must be equal to or greater than `1`. The optimal factor is `10`.
  Type: `int`. Default: `10`.

:auto_mkdir:
  Whether, when opening a file, the directory containing it should
  be created (if it doesn't already exist).
  Type: `bool`. Default: `false`.

## Examples

To integrate `omniload` with SMB, you need the SMB connection address
to connect to.

### Load Parquet data from SMB into DuckDB

The following command demonstrates how to copy data from a specified SMB
location into a DuckDB database.

```sh
omniload ingest \
    --source-uri   'smb://workgroup;user:password@server.example.org:445' \
    --source-table 'path/to/data.parquet' \
    --dest-uri     'duckdb:///demo.duckdb' \
    --dest-table   'testdrive.data'
```

Running the command creates a table named `data` within the `testdrive`
schema in the DuckDB database file located at `demo.duckdb`.

:::{tip}
Here, instead of defining the remote resource exclusively per source URI
using its `<path>` component, the file glob pattern is specified using the
separate `--source-table` option. Both addressing variants are supported equally.
:::


[Server Message Block (SMB)]: https://en.wikipedia.org/wiki/Server_Message_Block
