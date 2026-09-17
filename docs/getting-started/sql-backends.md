(sql-backends)=

# SQL backends

For SQL sources, omniload reflects the table with SQLAlchemy and then reads its
rows through one of dlt's table-loader backends, selected with
`--sql-backend`:

| Backend | Rows arrive as | Connection | Notes |
|---|---|---|---|
| `sqlalchemy` | Python rows | the SQLAlchemy driver in the source URI | The one `scd2` and `query:` sources use. |
| `pyarrow` | Arrow tables built from Python rows | the SQLAlchemy driver | The default for most sources. |
| `connectorx` | Arrow tables | ConnectorX, from the source URI | Needs `connectorx`. |
| `adbcbridge` | Arrow record batches straight from the database's ODBC driver | [adbcBridge], an ADBC driver over ODBC | Needs `omniload[adbcbridge]` and an ODBC driver manager. |

Whatever the backend, SQLAlchemy still builds the query, so `--incremental-key`,
`--interval-start`/`--interval-end`, `--sql-limit` and `--sql-exclude-columns`
work the same way for all of them, and the `append` and `merge` strategies keep
their cursor state as usual.

## `adbcbridge`: ADBC over ODBC

```shell
pip install 'omniload[adbcbridge]'
```

The `adbcbridge` backend executes the compiled query through the database's
ODBC driver via [adbcBridge] and yields Arrow record batches without a Python
row in between. It needs an ODBC driver manager (unixODBC on Linux and macOS,
the built-in one on Windows) and the database's ODBC driver. The omniload
container image already ships unixODBC with the PostgreSQL (`psqlodbc`) and
Microsoft SQL Server (`msodbcsql18`) drivers, so these two need nothing more:

```shell
omniload ingest \
    --source-uri 'postgresql://user:password@localhost:5432/shop' \
    --source-table 'public.orders' \
    --dest-uri 'duckdb:///warehouse.duckdb' \
    --dest-table 'shop.orders' \
    --sql-backend adbcbridge
```

```shell
omniload ingest \
    --source-uri 'mssql://user:password@localhost:1433/shop?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes' \
    --source-table 'dbo.orders' \
    --dest-uri 'duckdb:///warehouse.duckdb' \
    --dest-table 'shop.orders' \
    --sql-backend adbcbridge
```

### The ODBC connection string

The backend derives the ODBC connection string from the source URI:

| Source URI | ODBC connection string |
|---|---|
| `postgresql://u:p@host:5432/db?sslmode=require` | `Driver={PostgreSQL Unicode};Server=host;Port=5432;Database=db;Uid=u;SSLmode=require;Pwd=p;` |
| `mssql://u:p@host:1433/db?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes` | `Driver={ODBC Driver 18 for SQL Server};Server=host,1433;Database=db;Uid=u;Pwd=p;TrustServerCertificate=yes;` |
| `crate://crate@host:4200/` | `Driver={PostgreSQL Unicode};Server=host;Port=5432;Database=doc;Uid=crate;` |
| `mysql://u:p@host:3306/db` | `Driver={MySQL ODBC 9.4 Unicode Driver};Server=host;Port=3306;Database=db;User=u;Password=p;` |

CrateDB is reached over its PostgreSQL wire protocol, so a `crate://` URI that
names the HTTP port (4200), or no port, maps to port 5432; any other port is
taken as the wire-protocol port. The `mssql://` URI's own `driver` parameter
names the driver, and its other parameters (`TrustServerCertificate`,
`Encrypt`, ...) are passed through.

Two overrides cover everything else:

- `OMNILOAD_ODBC_DRIVER_POSTGRESQL`, `OMNILOAD_ODBC_DRIVER_MSSQL`,
  `OMNILOAD_ODBC_DRIVER_MYSQL`, `OMNILOAD_ODBC_DRIVER_CRATEDB`: the driver's
  registered name or the path to its library, when it is not registered under
  the default name above.
- `--sql-odbc-uri` (or `OMNILOAD_SQL_ODBC_URI`): the whole ODBC connection
  string, for any database with an ODBC driver, a DSN, or a non-default port.
  The source URI is still used for reflection and query compilation.

```shell
omniload ingest \
    --source-uri 'crate://crate@localhost:4200/' \
    --source-table 'doc.orders' \
    --dest-uri 'duckdb:///warehouse.duckdb' \
    --dest-table 'shop.orders' \
    --sql-backend adbcbridge \
    --sql-odbc-uri 'Driver={PostgreSQL Unicode};Server=localhost;Port=15432;Database=doc;Uid=crate;'
```

### What to expect

- Column types come from the ODBC driver's description of the result, at the
  precision the column declares: a PostgreSQL `TIMESTAMP(0)` arrives as a
  second-precision timestamp, `TIMESTAMPTZ(3)` as milliseconds with a UTC zone.
- `--page-size` sets the Arrow batch size the driver hands back.
- An ODBC driver the driver manager cannot find fails before the first row,
  with a message naming `OMNILOAD_ODBC_DRIVER_<FAMILY>` and `--sql-odbc-uri`;
  register the driver in `odbcinst.ini` or point either of those at its library.
- `scd2` is rejected with this backend, as with `pyarrow` and `connectorx`:
  dlt computes the row hash SCD2 needs only for Python rows. A `query:` source
  is read with `sqlalchemy` whatever backend is named.
- adbcBridge's [compatibility matrix] records, per database and ODBC driver,
  what the route reads and writes correctly.

[adbcBridge]: https://github.com/singhpratech/adbcbridge
[compatibility matrix]: https://adbcbridge.org/matrix/
