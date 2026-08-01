(rsync)=

# rsync

[rsync] is a utility for efficiently transferring and synchronizing files
between a local and a remote system. `omniload` supports rsync as a data
source through the [oc-rsync] binary, which must be installed separately.

The connector stages remote files into a local directory, then reads them
through the shared filesystem reader. All supported
{ref}`file formats <file-formats>` work the same as with `file://`.

## URI formats

Two transport schemes are supported, both handled by the same connector.

### SSH transport

```text
rsync+ssh://[<user>@]<host>[:<ssh_port>]/path/to/data.csv
```

Reaches the remote through an SSH shell. The remote path uses rsync's
single-colon `host:path` wire format internally.

### rsync daemon transport

```text
rsync://[<user>[:<password>]@]<host>[:<port>]/module/path/to/data.csv
```

Connects directly to an rsync daemon on TCP port 873 (default).

## URI parameters

Parameters are passed as query-string values on the source URI.

### Connection (SSH)

:user:
  SSH username. Can also be placed in the URI authority (user@host).

:ssh_port:
  SSH port. Can also be placed in the URI authority (host:2222).
  Type: `int`. Default: `22`.

:ssh_key:
  Path to an SSH private key file.

:ssh_options:
  Additional SSH options, shell-split (e.g. `?ssh_options=-o%20StrictHostKeyChecking%3Dno`).

:rsh:
  Override the entire remote-shell command. When set, ssh_key, ssh_port,
  and ssh_options are ignored.

### Connection (daemon)

:user:
  Daemon username. Can also be placed in the URI authority.

:password:
  Inline daemon password. Promoted to the RSYNC_PASSWORD environment variable
  so it never appears on the command line. Can also be placed in the URI authority.

:password_file:
  Path to an rsync password file. Takes precedence over an inline password.

:port:
  Daemon port. Can also be placed in the URI authority.
  Type: `int`. Default: `873`.

:no_motd:
  Suppress the daemon message-of-the-day.
  Type: `bool`. Default: `true`.

### Transfer tuning

:compress:
  Enable transfer compression (`-z`).
  Type: `bool`. Default: `true`.

:timeout:
  I/O timeout in seconds (`--timeout`).
  Type: `int`.

:contimeout:
  Connection timeout in seconds (`--contimeout`).
  Type: `int`.

:bwlimit:
  Bandwidth limit (`--bwlimit`), e.g. 2m for 2 MiB/s.

:rsync_path:
  Path to rsync on the remote machine (`--rsync-path`).

:staging_dir:
  Local directory for staging transferred files. Defaults to a
  deterministic subdirectory under the system temp directory. Repeated
  runs reuse the same staging cache so rsync transfers only the delta.

:extra_args:
  Escape hatch for arbitrary rsync flags, shell-split
  (e.g. `?extra_args=--chmod%3DD755%20--numeric-ids`).

### Remote path and file selection

The remote path is supplied via --source-table. When empty, the path
component of the source URI is used as a fallback. The path may contain
glob patterns:

```
┌─────────────────────────┬─────────────────────────────────┐
│         Pattern         │           Description           │
├─────────────────────────┼─────────────────────────────────┤
│ module/exports/*.csv    │ All CSV files at the top level. │
├─────────────────────────┼─────────────────────────────────┤
│ module/exports/**/*.csv │ All CSV files recursively.      │
├─────────────────────────┼─────────────────────────────────┤
│ /srv/data/report.jsonl  │ A single file (SSH transport).  │
└─────────────────────────┴─────────────────────────────────┘
```

A #format or #key=value fragment on the table selects the reader and
passes reader hints, exactly as with `file://`.

### Incremental loading

The rsync connector manages incrementality through file modification time.
Unchanged files are skipped by rsync on re-sync, and the reader's
mtime-based selection filters files on the staged tree. Row-level
`--incremental-key` is not supported and will be rejected.

## Examples

Load CSV from an rsync daemon into DuckDB.

```shell
omniload ingest \
    --source-uri   'rsync://user@data.example.com' \
    --source-table 'module/exports/*.csv' \
    --dest-uri     'duckdb:///demo.duckdb' \
    --dest-table   'testdrive.exports'
```

Load JSONL from a remote server via SSH.

```shell
omniload ingest \
    --source-uri   'rsync+ssh://deploy@host?ssh_key=/keys/id_ed25519' \
    --source-table '/srv/data/**/*.jsonl' \
    --dest-uri     'duckdb:///demo.duckdb' \
    --dest-table   'testdrive.data'
```

Load with bandwidth limit and custom port.

```shell
omniload ingest \
    --source-uri   'rsync://host:8730/mod?bwlimit=2m&timeout=30' \
    --source-table 'mod/reports/*.parquet' \
    --dest-uri     'duckdb:///demo.duckdb' \
    --dest-table   'testdrive.reports'
```

Load Excel with a reader hint.

```shell
omniload ingest \
    --source-uri   'rsync+ssh://host' \
    --source-table '/data/book.xlsx#sheet_name=Sheet1' \
    --dest-uri     'duckdb:///demo.duckdb' \
    --dest-table   'testdrive.staff'
```

:::{tip}
The `--source-table` option carries both the remote path and optional reader
hints. When the URI already contains a path, the table value takes precedence.
:::

:::{note}
`rsync` or `oc-rsync` must be installed and available on the system PATH.
:::
