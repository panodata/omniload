# Exasol

[Exasol] is an in-memory, massively parallel analytics database.

omniload supports Exasol as a source.

## Installation

```shell
pip install 'omniload[exasol]'
```

The extra installs [sqlalchemy-exasol] and its websocket driver, [pyexasol].
sqlalchemy-exasol does not support Python 3.15 yet, so the extra installs nothing
there.

## URI format

```text
exa+websocket://<username>:<password>@<host>:<port>[/<schema>][?<options>]
```

`exa://` is accepted as the same thing.

URI parameters:

- `username`, `password`: the database user and its password.
- `host`, `port`: the database server. The port is required; Exasol's default is
  `8563`.
- `schema`: an optional default schema for the connection.

Options:

- `ENCRYPTION`: `Yes` (the default) connects over TLS. `No` connects without it, and
  needs `SSLCertificate=SSL_VERIFY_NONE` as well.
- `FINGERPRINT`: the SHA-256 fingerprint of the server's certificate, as hexadecimal
  digits without separators. The connection is accepted when the certificate matches
  it, so it suits a server with a self-signed certificate.
- `SSLCertificate=SSL_VERIFY_NONE`: accept any certificate without checking it, for
  a local test server.

Name the source table as `<schema>.<table>`. Exasol has schemas and no catalogs, so a
three-part name is refused.

## Example

Load a table from a local Exasol test server into DuckDB:

```shell
omniload ingest \
  --source-uri 'exa+websocket://sys:exasol@localhost:8563?SSLCertificate=SSL_VERIFY_NONE' \
  --source-table 'retail.sales' \
  --dest-uri 'duckdb:///warehouse.duckdb' \
  --dest-table 'raw.sales'
```

## Data types

How each Exasol type arrives from a table named with `--source-table`, as loaded into
DuckDB. Both the default `pyarrow` backend and `--sql-backend sqlalchemy` give the same
result. A `query:` source has no table to reflect, so its column types are inferred
from the values instead and can differ from this table.

| Exasol | Loaded as | Value |
|---|---|---|
| `BIGINT` | `DECIMAL(36,0)` | Exasol stores `BIGINT` as `DECIMAL(36,0)` |
| `BOOLEAN` | `BOOLEAN` | |
| `CHAR(n)` | `VARCHAR` | Blank-padded to `n` characters |
| `DATE` | `DATE` | |
| `DECIMAL(p,0)` up to 18 digits, `INTEGER` | `BIGINT` | |
| `DECIMAL(p,s)`, other | `DECIMAL(p,s)` | All 36 digits kept |
| `DOUBLE` | `DOUBLE` | |
| `GEOMETRY` | `VARCHAR` | Well-known text, such as `POINT (1 2)` |
| `HASHTYPE` | `VARCHAR` | Hexadecimal digits |
| `INTERVAL DAY TO SECOND` | not supported | See the note below |
| `INTERVAL YEAR TO MONTH` | `VARCHAR` | Such as `+05-03` |
| `TIMESTAMP`, `TIMESTAMP(p)` | `TIMESTAMP` | Up to microseconds; finer digits are cut off |
| `TIMESTAMP WITH LOCAL TIME ZONE` | `TIMESTAMP WITH TIME ZONE` | The exact instant |
| `VARCHAR` | `VARCHAR` | |

:::{note}
**Sessions run in UTC.** Exasol returns `TIMESTAMP WITH LOCAL TIME ZONE` values as
wall-clock time in the session's time zone, and they are only read as the right
instant in a UTC session. So omniload sets `TIME_ZONE = 'UTC'` on every connection,
and fails the run if that statement fails. This has consequences beyond those
columns:

- In a `query:` source, functions that follow the session zone, such as
  `CURRENT_TIMESTAMP`, and comparisons between `TIMESTAMP` and
  `TIMESTAMP WITH LOCAL TIME ZONE` values are evaluated in UTC.
- `--interval-start` and `--interval-end` values with a UTC offset are converted to
  UTC. Values without an offset are used as written.
- The `adbcbridge` and `connectorx` SQL backends read over a connection of their own,
  outside the UTC session, so they are refused for Exasol.

Plain `TIMESTAMP` values are not affected.
:::

:::{note}
**`INTERVAL DAY TO SECOND` columns fail the load.** Exclude the column with
`--sql-exclude-columns`, or read it as text from a `query:` source:

```shell
--source-table 'query:SELECT id, CAST(duration AS VARCHAR(40)) AS duration FROM retail.calls'
```

This loads values such as `+02 12:30:15.500`.
:::

:::{tip}
pyexasol 3.0 and newer send a usage event to Exasol on every connection. Set the
environment variable `EXASOL_TELEMETRY_DISABLE` to any value to turn that off. It is
read once, when the first connection is made, so set it before omniload starts.
:::

[Exasol]: https://www.exasol.com/
[pyexasol]: https://pypi.org/project/pyexasol/
[sqlalchemy-exasol]: https://pypi.org/project/sqlalchemy-exasol/
