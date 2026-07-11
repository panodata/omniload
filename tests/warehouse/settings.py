from tests.warehouse.manager import registry

SOURCES = {
    "postgres": registry.postgresql,
    "duckdb": registry.duckdb_source,
    "mysql8": registry.mysql,
}
DESTINATIONS = {
    "postgres": registry.postgresql,
    "duckdb": registry.duckdb_destination,
    "clickhouse+native": registry.clickhouse,
    "cratedb": registry.cratedb,
}

if registry.mssql is not None:
    SOURCES.update({"sqlserver": registry.mssql})
