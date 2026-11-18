# PostgreSQL

PostgreSQL is an open source, object-relational database system that provides reliability, data integrity, and correctness.

omniload supports PostgreSQL as both a source and destination.

## URI format
The URI format for PostgreSQL is as follows:

```text
postgresql://<username>:<password>@<host>:<port>/<database-name>?sslmode=<sslmode>
```

URI parameters:
- `username`: the user name to connect to the database
- `password`: the password for the user
- `host`: the host address of the database server
- `port`: the port number the database server is listening on, the default is 5432
- `database-name`: the name of the database to connect to
- `sslmode`: optional, the SSL mode to use when connecting to the database

The same URI structure can be used both for sources and destinations. You can read more about SQLAlchemy's PostgreSQL dialect [here](https://docs.sqlalchemy.org/en/14/dialects/postgresql.html).

As a source, PostgreSQL can also be read through its ODBC driver with
`--sql-backend adbcbridge`, which yields Arrow batches straight from the driver;
see {ref}`sql-backends`.

## Interval columns

Through the `sqlalchemy` and `pyarrow` SQL backends, an `interval` column loads
as text in PostgreSQL's own format, such as `1 day 02:03:04.5` or `-03:00:00`,
which casts back with `CAST(d AS INTERVAL)`. Their driver, psycopg2, counts a
year as 365 days and a month as 30 before omniload sees the value, so
`1 year 2 mons` loads as `425 days`. Interval columns read through
`--sql-backend adbcbridge` are not covered. To keep calendar units, read the
column as text from a `query:` source:

```shell
--source-table 'query:SELECT id, CAST(d AS TEXT) AS d FROM public.t'
```
