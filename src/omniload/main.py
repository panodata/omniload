import logging
import warnings
from datetime import datetime
from typing import Optional

import typer
from typing_extensions import Annotated

from omniload.api import (
    run_ingest,
)
from omniload.error import IngestJobError, ValidationError
from omniload.model import (
    IncrementalStrategy,
    LoaderFileFormat,
    Progress,
    SchemaNaming,
    SqlBackend,
    SqlReflectionLevel,
)
from omniload.util.log import setup_logging

logger = logging.getLogger(__name__)

try:
    from duckdb_engine import DuckDBEngineWarning

    warnings.filterwarnings("ignore", category=DuckDBEngineWarning)
except ImportError:
    # duckdb-engine not installed
    pass

app = typer.Typer(
    name="omniload",
    help="omniload is the CLI tool to ingest data from one source to another",
    rich_markup_mode="rich",
    pretty_exceptions_enable=False,
)

DATE_FORMATS = [
    "%Y-%m-%d",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S.%f%z",
]


@app.command()
def ingest(
    source_uri: Annotated[
        str,
        typer.Option(
            help="The URI of the [green]source[/green]",
            envvar=["SOURCE_URI", "OMNILOAD_SOURCE_URI"],
        ),
    ],
    dest_uri: Annotated[
        str,
        typer.Option(
            help="The URI of the [cyan]destination[/cyan]",
            envvar=["DESTINATION_URI", "OMNILOAD_DESTINATION_URI"],
        ),
    ],
    source_table: Annotated[  # ty: ignore[invalid-parameter-default]
        str,
        typer.Option(
            help="The table name in the [green]source[/green] to fetch",
            envvar=["SOURCE_TABLE", "OMNILOAD_SOURCE_TABLE"],
        ),
    ] = None,
    dest_table: Annotated[  # ty: ignore[invalid-parameter-default]
        str,
        typer.Option(
            help="The table in the [cyan]destination[/cyan] to save the data into",
            envvar=["DESTINATION_TABLE", "OMNILOAD_DESTINATION_TABLE"],
        ),
    ] = None,
    incremental_key: Annotated[
        Optional[str],
        typer.Option(
            help="The incremental key from the table "
            "to be used for incremental strategies",
            envvar=["INCREMENTAL_KEY", "OMNILOAD_INCREMENTAL_KEY"],
        ),
    ] = None,
    incremental_strategy: Annotated[
        Optional[IncrementalStrategy],
        typer.Option(
            help="The incremental strategy to use. When omitted, defaults to "
            "'append' for filesystem sources (file/s3/gcs/azure/sftp) and "
            "'replace' for ordinary sources; sources that manage their own "
            "incrementality (many SaaS/streaming sources) keep their own "
            "per-resource disposition instead.",
            envvar=["INCREMENTAL_STRATEGY", "OMNILOAD_INCREMENTAL_STRATEGY"],
        ),
    ] = None,
    filesystem_incremental: Annotated[
        bool,
        typer.Option(
            help="Read filesystem files newer than the previous modification-time "
            "boundary, plus unseen files at that boundary. This opt-in mode requires "
            "append loading and durable pipeline state.",
            envvar=[
                "FILESYSTEM_INCREMENTAL",
                "OMNILOAD_FILESYSTEM_INCREMENTAL",
            ],
        ),
    ] = False,
    interval_start: Annotated[
        Optional[datetime],
        typer.Option(
            help="The start of the interval the incremental key will cover",
            formats=DATE_FORMATS,
            envvar=["INTERVAL_START", "OMNILOAD_INTERVAL_START"],
        ),
    ] = None,
    interval_end: Annotated[
        Optional[datetime],
        typer.Option(
            help="The end of the interval the incremental key will cover",
            formats=DATE_FORMATS,
            envvar=["INTERVAL_END", "OMNILOAD_INTERVAL_END"],
        ),
    ] = None,
    primary_key: Annotated[
        Optional[list[str]],
        typer.Option(
            help="The key that will be used to deduplicate the resulting table",
            envvar=["PRIMARY_KEY", "OMNILOAD_PRIMARY_KEY"],
        ),
    ] = None,
    partition_by: Annotated[
        Optional[str],
        typer.Option(
            help="The partition key to be used for partitioning the destination table",
            envvar=["PARTITION_BY", "OMNILOAD_PARTITION_BY"],
        ),
    ] = None,
    cluster_by: Annotated[
        Optional[str],
        typer.Option(
            help="The clustering key to be used for clustering the destination table, "
            "not every destination supports clustering.",
            envvar=["CLUSTER_BY", "OMNILOAD_CLUSTER_BY"],
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option(
            help="Display data transfer plan but don't invoke it",
            envvar=["DRY_RUN", "OMNILOAD_DRY_RUN"],
        ),
    ] = False,
    full_refresh: Annotated[
        bool,
        typer.Option(
            help="Ignore the state and refresh the destination table completely",
            envvar=["FULL_REFRESH", "OMNILOAD_FULL_REFRESH"],
        ),
    ] = False,
    progress: Annotated[
        Progress,
        typer.Option(
            help="The progress display type, must be one of 'interactive', 'log'",
            envvar=["PROGRESS", "OMNILOAD_PROGRESS"],
        ),
    ] = Progress.interactive,
    sql_backend: Annotated[
        SqlBackend,
        typer.Option(
            help="The SQL backend to use",
            envvar=["SQL_BACKEND", "OMNILOAD_SQL_BACKEND"],
        ),
    ] = SqlBackend.default,
    loader_file_format: Annotated[
        Optional[LoaderFileFormat],
        typer.Option(
            help="The file format to use when loading data",
            envvar=["LOADER_FILE_FORMAT", "OMNILOAD_LOADER_FILE_FORMAT"],
        ),
    ] = None,
    page_size: Annotated[
        int,
        typer.Option(
            help="The page size to be used when fetching data from SQL sources",
            envvar=["PAGE_SIZE", "OMNILOAD_PAGE_SIZE"],
        ),
    ] = 50000,
    loader_file_size: Annotated[
        int,
        typer.Option(
            help="The file size to be used by the loader to split the data into "
            "multiple files. This can be set independent of the page size, "
            "since page size is used for fetching the data from the sources "
            "whereas this is used for the processing/loading part.",
            envvar=["LOADER_FILE_SIZE", "OMNILOAD_LOADER_FILE_SIZE"],
        ),
    ] = 100000,
    schema_naming: Annotated[
        SchemaNaming,
        typer.Option(
            help="The naming convention to use when moving the tables from source to "
            "destination. The default behavior is explained here: "
            "https://dlthub.com/docs/general-usage/schema#naming-convention",
            envvar=["SCHEMA_NAMING", "OMNILOAD_SCHEMA_NAMING"],
        ),
    ] = SchemaNaming.default,
    pipelines_dir: Annotated[
        Optional[str],
        typer.Option(
            help="The path to store dlt-related pipeline metadata. By default, "
            "omniload will create a temporary directory and delete it after "
            "the execution is done in order to make retries stateless.",
            envvar=["PIPELINES_DIR", "OMNILOAD_PIPELINES_DIR"],
        ),
    ] = None,
    extract_parallelism: Annotated[
        int,
        typer.Option(
            help="The number of parallel jobs to run for extracting data "
            "from the source, only applicable for certain sources",
            envvar=["EXTRACT_PARALLELISM", "OMNILOAD_EXTRACT_PARALLELISM"],
        ),
    ] = 5,
    sql_reflection_level: Annotated[
        SqlReflectionLevel,
        typer.Option(
            help="The reflection level to use when reflecting the table schema "
            "from the source",
            envvar=["SQL_REFLECTION_LEVEL", "OMNILOAD_SQL_REFLECTION_LEVEL"],
        ),
    ] = SqlReflectionLevel.full,
    sql_limit: Annotated[
        Optional[int],
        typer.Option(
            help="The limit to use when fetching data from the source",
            envvar=["SQL_LIMIT", "OMNILOAD_SQL_LIMIT"],
        ),
    ] = None,
    sql_exclude_columns: Annotated[
        Optional[list[str]],
        typer.Option(
            help="The columns to exclude from the source table",
            envvar=["SQL_EXCLUDE_COLUMNS", "OMNILOAD_SQL_EXCLUDE_COLUMNS"],
        ),
    ] = None,
    columns: Annotated[
        Optional[list[str]],
        typer.Option(
            help="The column types to be used for the destination table "
            "in the format of 'column_name:column_type'",
            envvar=["OMNILOAD_COLUMNS"],
        ),
    ] = None,
    yield_limit: Annotated[
        Optional[int],
        typer.Option(
            help="Limit the number of pages yielded from the source",
            envvar=["YIELD_LIMIT", "OMNILOAD_YIELD_LIMIT"],
        ),
    ] = None,
    staging_bucket: Annotated[
        Optional[str],
        typer.Option(
            help="The staging bucket to be used for the ingestion, "
            "must be prefixed with 'gs://' or 's3://'",
            envvar=["STAGING_BUCKET", "OMNILOAD_STAGING_BUCKET"],
        ),
    ] = None,
    mask: Annotated[
        Optional[list[str]],
        typer.Option(
            help="Column masking configuration in format 'column:algorithm[:param]'. "
            "Can be specified multiple times.",
            envvar=["MASK", "OMNILOAD_MASK"],
        ),
    ] = None,
):
    setup_logging()
    try:
        run_ingest(
            source_uri=source_uri,
            dest_uri=dest_uri,
            source_table=source_table,
            dest_table=dest_table,
            incremental_key=incremental_key,
            incremental_strategy=incremental_strategy,
            filesystem_incremental=filesystem_incremental,
            interval_start=interval_start,
            interval_end=interval_end,
            primary_key=primary_key,
            partition_by=partition_by,
            cluster_by=cluster_by,
            dry_run=dry_run,
            full_refresh=full_refresh,
            progress=progress,
            sql_backend=sql_backend,
            loader_file_format=loader_file_format,
            page_size=page_size,
            loader_file_size=loader_file_size,
            schema_naming=schema_naming,
            pipelines_dir=pipelines_dir,
            extract_parallelism=extract_parallelism,
            sql_reflection_level=sql_reflection_level,
            sql_limit=sql_limit,
            sql_exclude_columns=sql_exclude_columns,
            columns=columns,
            yield_limit=yield_limit,
            staging_bucket=staging_bucket,
            mask=mask,
        )
    except ValidationError as e:
        logger.error("Validation failed: %s", e)
        raise typer.Abort()
    except IngestJobError:
        raise typer.Exit(1)


@app.command()
def example_uris():
    # ruff: disable[E501,T201]
    print()
    typer.echo(
        "Following are some example URI formats for supported sources and destinations:"
    )

    print()
    print(
        "[bold green]Postgres:[/bold green] [white]postgres://user:password@host:port/dbname?sslmode=require [/white]"
    )
    print(
        "[white dim]└── https://docs.sqlalchemy.org/en/20/core/engines.html#postgresql[/white dim]"
    )

    print()
    print(
        "[bold green]BigQuery:[/bold green] [white]bigquery://project-id?credentials_path=/path/to/credentials.json&location=US [/white]"
    )
    print(
        "[white dim]└── https://github.com/googleapis/python-bigquery-sqlalchemy?tab=readme-ov-file#connection-string-parameters[/white dim]"
    )

    print()
    print(
        "[bold green]Snowflake:[/bold green] [white]snowflake://user:password@account/dbname?warehouse=COMPUTE_WH [/white]"
    )
    print(
        "[white dim]└── https://docs.snowflake.com/en/developer-guide/python-connector/sqlalchemy#connection-parameters"
    )

    print()
    print(
        "[bold green]Redshift:[/bold green] [white]redshift://user:password@host:port/dbname?sslmode=require [/white]"
    )
    print(
        "[white dim]└── https://aws.amazon.com/blogs/big-data/use-the-amazon-redshift-sqlalchemy-dialect-to-interact-with-amazon-redshift/[/white dim]"
    )

    print()
    print(
        "[bold green]Databricks:[/bold green] [white]databricks://token:<access_token>@<server_hostname>?http_path=<http_path>&catalog=<catalog>&schema=<schema>[/white]"
    )
    print("[white dim]└── https://docs.databricks.com/en/dev-tools/sqlalchemy.html")

    print()
    print(
        "[bold green]Microsoft SQL Server:[/bold green] [white]mssql://user:password@host:port/dbname?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes [/white]"
    )
    print(
        "[white dim]└── https://docs.sqlalchemy.org/en/20/core/engines.html#microsoft-sql-server"
    )

    print()
    print(
        "[bold green]MySQL:[/bold green] [white]mysql://user:password@host:port/dbname [/white]"
    )
    print(
        "[white dim]└── https://docs.sqlalchemy.org/en/20/core/engines.html#mysql[/white dim]"
    )

    print()
    print("[bold green]DuckDB:[/bold green] [white]duckdb://path/to/database [/white]")
    print("[white dim]└── https://github.com/Mause/duckdb_engine[/white dim]")

    print()
    print("[bold green]SQLite:[/bold green] [white]sqlite://path/to/database [/white]")
    print(
        "[white dim]└── https://docs.sqlalchemy.org/en/20/core/engines.html#sqlite[/white dim]"
    )

    print()
    typer.echo(
        "These are all coming from SQLAlchemy's URI format, so they should be familiar to most users."
    )

    logger.info("Streaming sources use their own URI schemes:")
    logger.info(
        "mq-bridge: kafka+mqb://localhost:9092?group_id=g "
        "(also nats/amqp/mqtt/zeromq/aws/ibmmq/memory)"
    )
    logger.info("└── https://omniload.readthedocs.io/supported-sources/mqbridge.html")
    # ruff: enable[E501,T201]


@app.command()
def version():
    from omniload import __version__

    print(f"v{__version__}")  # noqa: T201


def main():
    app()


if __name__ == "__main__":
    main()
