import json
import re
import unittest
from abc import abstractmethod
from typing import Any, Type
from unittest.mock import patch

import dlt
import pytest
from dlt.common.destination import Destination
from dlt.common.destination.client import JobClientBase

from omniload.target.athena import AthenaDestination
from omniload.target.bigquery import BigQueryDestination
from omniload.target.clickhouse import ClickhouseDestination
from omniload.target.cratedb import CrateDBDestination
from omniload.target.csv import CsvDestination
from omniload.target.databricks import DatabricksDestination
from omniload.target.duckdb import DuckDBDestination
from omniload.target.elasticsearch.api import ElasticsearchDestination
from omniload.target.mongodb import MongoDBDestination
from omniload.target.motherduck import MotherduckDestination
from omniload.target.mssql import MsSQLDestination
from omniload.target.mysql import MySqlDestination
from omniload.target.postgresql import PostgresDestination
from omniload.target.redshift import RedshiftDestination
from omniload.target.snowflake import SnowflakeDestination
from omniload.target.sqlite import SqliteDestination
from omniload.target.synapse import SynapseDestination
from omniload.target.trino import TrinoDestination
from tests.util.common import get_etc_path


class BigQueryDestinationTest(unittest.TestCase):
    destination = BigQueryDestination()
    abs_path_to_credentials = str(get_etc_path() / "fakebqcredentials.json")
    actual_credentials: dict = {}

    def setUp(self):
        with open(self.abs_path_to_credentials, "r") as f:
            self.actual_credentials = json.load(f)

    def test_bq_destination_simple_uri(self):
        uri = f"bigquery://my-project?credentials_path={self.abs_path_to_credentials}"
        result = self.destination.dlt_dest(uri)

        self.assertTrue(isinstance(result, dlt.destinations.bigquery))
        self.assertEqual(result.config_params["credentials"], self.actual_credentials)
        self.assertTrue("location" not in result.config_params)

    def test_bq_destination_with_location(self):
        uri = f"bigquery://my-project?credentials_path={self.abs_path_to_credentials}&location=EU"
        result = self.destination.dlt_dest(uri)

        self.assertTrue(isinstance(result, dlt.destinations.bigquery))
        self.assertEqual(result.config_params["credentials"], self.actual_credentials)
        self.assertEqual(result.config_params["location"], "EU")

    def test_table_project_overrides_uri_project(self):
        uri = f"bigquery://uri-project?credentials_path={self.abs_path_to_credentials}"

        result = self.destination.dlt_dest(
            uri, dest_table="table-project.dataset.table"
        )

        self.assertEqual(result.config_params["project_id"], "table-project")

    def test_uri_project_is_the_fallback_for_two_part_table(self):
        uri = f"bigquery://uri-project?credentials_path={self.abs_path_to_credentials}"

        result = self.destination.dlt_dest(uri, dest_table="dataset.table")

        self.assertEqual(result.config_params["project_id"], "uri-project")

    def test_bq_destination_run_params_require_two_or_three_fields(self):
        with pytest.raises(ValueError):
            self.destination.dlt_run_params("", "sometable")

        with pytest.raises(ValueError):
            self.destination.dlt_run_params("", "sometable.with.extra.fields")

    def test_bq_destination_run_params_parse_table_names_correctly(self):
        result = self.destination.dlt_run_params("", "dataset.sometable")
        self.assertEqual(result, {"dataset_name": "dataset", "table_name": "sometable"})

        result = self.destination.dlt_run_params("", "project.dataset.sometable")
        self.assertEqual(result, {"dataset_name": "dataset", "table_name": "sometable"})


class GenericSqlDestinationFixture:
    destination: Any
    expected_class: Type[Destination[Any, JobClientBase]]

    @abstractmethod
    def assertEqual(self, first, second, msg=None):
        pass

    @abstractmethod
    def assertTrue(self, expr, msg=None):
        pass

    def test_credentials_are_passed_correctly(self):
        uri = "some-uri"
        result = self.destination.dlt_dest(uri)

        self.assertTrue(isinstance(result, self.expected_class))
        self.assertEqual(result.config_params["credentials"], uri)

    def test_destination_run_params_enforce_capability(self):
        with pytest.raises(ValueError):
            self.destination.dlt_run_params("", "sometable")

        max_components = self.destination.table_capability.max_components
        overqualified = ".".join("part" for _ in range(max_components + 1))
        with pytest.raises(ValueError):
            self.destination.dlt_run_params("", overqualified)

    def test_destination_run_params_parse_table_names_correctly(self):
        result = self.destination.dlt_run_params("", "dataset.sometable")
        self.assertEqual(result, {"dataset_name": "dataset", "table_name": "sometable"})

        if self.destination.table_capability.max_components == 3:
            result = self.destination.dlt_run_params("", "catalog.dataset.sometable")
            self.assertEqual(
                result, {"dataset_name": "dataset", "table_name": "sometable"}
            )


class PostgresDestinationTest(unittest.TestCase, GenericSqlDestinationFixture):
    destination = PostgresDestination()
    expected_class = dlt.destinations.postgres


class SnowflakeDestinationTest(unittest.TestCase, GenericSqlDestinationFixture):
    destination = SnowflakeDestination()
    expected_class = dlt.destinations.snowflake


class RedshiftDestinationTest(GenericSqlDestinationFixture, unittest.TestCase):
    destination = RedshiftDestination()
    expected_class = dlt.destinations.redshift


class DuckDBDestinationTest(GenericSqlDestinationFixture, unittest.TestCase):
    destination = DuckDBDestination()
    expected_class = dlt.destinations.duckdb

    def test_catalog_error_names_destination_format_and_alternative(self):
        with pytest.raises(ValueError) as exc_info:
            self.destination.dlt_run_params("", "catalog.schema.table")

        message = str(exc_info.value)
        self.assertIn("duckdb", message)
        self.assertIn("<schema>.<table>", message)
        self.assertIn("--dest-uri", message)


@pytest.mark.odbc
class MsSQLDestinationTest(GenericSqlDestinationFixture, unittest.TestCase):
    destination = MsSQLDestination()
    expected_class = dlt.destinations.mssql


class SynapseDestinationTest(GenericSqlDestinationFixture, unittest.TestCase):
    destination = SynapseDestination()
    expected_class = dlt.destinations.synapse


class TrinoDestinationTest(GenericSqlDestinationFixture, unittest.TestCase):
    destination = TrinoDestination()

    def test_credentials_are_passed_correctly(self):
        result = self.destination.dlt_dest("trino://user@host/catalog/schema")

        self.assertEqual(
            result.config_params["credentials"],
            "trino://user@host/catalog/schema",
        )


class CrateDBDestinationTest(GenericSqlDestinationFixture, unittest.TestCase):
    destination = CrateDBDestination()
    expected_class = None

    def test_credentials_are_passed_correctly(self):
        # CrateDB rewrites its scheme and builds through a third-party factory, so the
        # generic credential assertion does not apply.
        pass


class DatabricksDestinationTest(unittest.TestCase):
    destination = DatabricksDestination()
    expected_class = dlt.destinations.databricks

    def test_credentials_are_passed_correctly(self):
        uri = (
            "databricks://token:password@hostname?http_path=/path/123&catalog=workspace"
        )
        result = self.destination.dlt_dest(uri)

        self.assertTrue(isinstance(result, self.expected_class))
        # Override the generic test - expect parsed credentials, not raw URI
        creds = result.config_params["credentials"]
        self.assertEqual(creds["access_token"], "password")
        self.assertEqual(creds["server_hostname"], "hostname")
        self.assertEqual(creds["http_path"], "/path/123")
        self.assertEqual(creds["catalog"], "workspace")

    def test_run_params_with_schema_dot_table(self):
        """Test that schema.table format works"""
        uri = (
            "databricks://token:password@hostname?http_path=/path/123&catalog=workspace"
        )
        result = self.destination.dlt_run_params(uri, "myschema.mytable")
        self.assertEqual(result, {"dataset_name": "myschema", "table_name": "mytable"})

    def test_run_params_with_schema_in_uri(self):
        """Test that just table name works when schema is in URI"""
        uri = "databricks://token:password@hostname?http_path=/path/123&catalog=workspace&schema=myschema"
        result = self.destination.dlt_run_params(uri, "mytable")
        self.assertEqual(result, {"dataset_name": "myschema", "table_name": "mytable"})

    def test_run_params_schema_dot_table_overrides_uri_schema(self):
        """Test that schema.table format overrides schema in URI"""
        uri = "databricks://token:password@hostname?http_path=/path/123&catalog=workspace&schema=old_schema"
        result = self.destination.dlt_run_params(uri, "new_schema.mytable")
        self.assertEqual(
            result, {"dataset_name": "new_schema", "table_name": "mytable"}
        )

    def test_table_catalog_overrides_uri_catalog(self):
        uri = "databricks://token:password@hostname?http_path=/path/123&catalog=old_catalog&schema=old_schema"

        result = self.destination.dlt_dest(
            uri, dest_table="new_catalog.new_schema.mytable"
        )

        self.assertEqual(result.config_params["credentials"]["catalog"], "new_catalog")

    def test_run_params_drop_the_catalog_from_a_three_part_name(self):
        """The catalog reaches the credentials, so run params carry schema and table."""
        uri = "databricks://token:password@hostname?http_path=/path/123"

        result = self.destination.dlt_run_params(uri, "new_catalog.new_schema.mytable")

        self.assertEqual(
            result, {"dataset_name": "new_schema", "table_name": "mytable"}
        )

    def test_run_params_requires_schema(self):
        """Test that error is raised when no schema is provided"""
        uri = (
            "databricks://token:password@hostname?http_path=/path/123&catalog=workspace"
        )
        with pytest.raises(ValueError) as exc_info:
            self.destination.dlt_run_params(uri, "mytable")
        self.assertIn("schema", str(exc_info.value).lower())

    @patch("omniload.target.databricks.get_databricks_oauth_token")
    def test_oauth_m2m_credentials(self, mock_get_token):
        """Test that client_id and client_secret trigger OAuth M2M flow"""
        mock_get_token.return_value = "mocked_access_token"

        uri = "databricks://@hostname?http_path=/path/123&catalog=workspace&client_id=my_client_id&client_secret=my_secret"
        result = self.destination.dlt_dest(uri)

        mock_get_token.assert_called_once_with("hostname", "my_client_id", "my_secret")
        creds = result.config_params["credentials"]
        self.assertEqual(creds["access_token"], "mocked_access_token")
        self.assertEqual(creds["server_hostname"], "hostname")

    def test_missing_hostname_raises_error(self):
        """Test that missing hostname raises ValueError"""
        uri = "databricks://?http_path=/path/123&catalog=workspace"
        with pytest.raises(ValueError) as exc_info:
            self.destination.dlt_dest(uri)
        self.assertIn("hostname", str(exc_info.value).lower())

    def test_missing_token_and_oauth_raises_error(self):
        """Test that missing both token and OAuth credentials raises ValueError"""
        uri = "databricks://@hostname?http_path=/path/123&catalog=workspace"
        with pytest.raises(ValueError) as exc_info:
            self.destination.dlt_dest(uri)
        self.assertIn("access token", str(exc_info.value).lower())


class ClickhouseDestinationTest(unittest.TestCase):
    destination = ClickhouseDestination()

    def test_database_and_table_are_parsed_through_the_capability(self):
        self.assertEqual(
            self.destination.dlt_run_params("", "analytics.events"),
            {"table_name": "events"},
        )
        with pytest.raises(ValueError, match="clickhouse"):
            self.destination.dlt_run_params("", "catalog.analytics.events")

    def test_engine_settings_parsed_from_uri(self):
        uri = "clickhouse://user:pass@localhost:9000/mydb?secure=0&engine.index_granularity=8192&engine.storage_policy=default"
        self.assertEqual(
            self.destination.engine_settings(uri),
            {"index_granularity": "8192", "storage_policy": "default"},
        )

    def test_non_engine_params_excluded(self):
        uri = "clickhouse://user:pass@localhost:9000/mydb?secure=0&http_port=8123&engine.index_granularity=8192"
        settings = self.destination.engine_settings(uri)
        self.assertNotIn("secure", settings)
        self.assertNotIn("http_port", settings)
        self.assertEqual(settings, {"index_granularity": "8192"})

    def test_no_engine_settings_returns_empty_dict(self):
        uri = "clickhouse://user:pass@localhost:9000/mydb?secure=0"
        self.assertEqual(self.destination.engine_settings(uri), {})

    def test_engine_type_parsed_from_uri(self):
        uri = "clickhouse://user:pass@localhost:9000/mydb?secure=0&engine=shared_merge_tree"
        self.assertEqual(self.destination.engine_type(uri), "shared_merge_tree")

    def test_engine_type_returns_none_when_absent(self):
        uri = "clickhouse://user:pass@localhost:9000/mydb?secure=0"
        self.assertIsNone(self.destination.engine_type(uri))

    def test_engine_and_engine_settings_together(self):
        uri = "clickhouse://user:pass@localhost:9000/mydb?engine=merge_tree&engine.index_granularity=8192&engine.storage_policy=default"
        self.assertEqual(self.destination.engine_type(uri), "merge_tree")
        self.assertEqual(
            self.destination.engine_settings(uri),
            {"index_granularity": "8192", "storage_policy": "default"},
        )

    def test_engine_not_included_in_engine_settings(self):
        uri = "clickhouse://user:pass@localhost:9000/mydb?engine=shared_merge_tree&engine.index_granularity=8192"
        settings = self.destination.engine_settings(uri)
        self.assertNotIn("", settings)
        self.assertNotIn("shared_merge_tree", settings.values())
        self.assertEqual(settings, {"index_granularity": "8192"})


@pytest.mark.parametrize(
    ("destination", "uri", "expected_uri"),
    [
        (
            PostgresDestination(),
            "postgres://user:pass@host/old_database?sslmode=require",
            "postgres://user:pass@host/new_database?sslmode=require",
        ),
        (
            SnowflakeDestination(),
            "snowflake://user:pass@account/old_database?warehouse=wh",
            "snowflake://user:pass@account/new_database?warehouse=wh",
        ),
        (
            RedshiftDestination(),
            "redshift://user:pass@host/old_database",
            "postgresql://user:pass@host/new_database",
        ),
        (
            MsSQLDestination(),
            "mssql://user:pass@host/old_database?driver=ODBC+Driver+18",
            "mssql://user:pass@host/new_database?driver=ODBC+Driver+18",
        ),
        (
            SynapseDestination(),
            "synapse://user:pass@host/old_database",
            "synapse://user:pass@host/new_database",
        ),
        (
            TrinoDestination(),
            "trino://user:pass@host/old_catalog/uri_schema",
            "trino://user:pass@host/new_database/uri_schema",
        ),
    ],
)
def test_catalog_retargets_connection_database(destination, uri, expected_uri):
    result = destination.dlt_dest(uri, dest_table="new_database.public.customers")

    assert result.config_params["credentials"] == expected_uri


def test_motherduck_catalog_overrides_uri_database():
    result = MotherduckDestination().dlt_dest(
        "md://old_database?token=password",
        dest_table="new_database.main.customers",
    )

    assert result.config_params["credentials"].database == "new_database"


def test_motherduck_uri_database_is_fallback_for_two_part_table():
    result = MotherduckDestination().dlt_dest(
        "md://uri_database?token=password", dest_table="main.customers"
    )

    assert result.config_params["credentials"].database == "uri_database"


def test_athena_routes_catalog_and_uses_schema_for_staging():
    result = AthenaDestination().dlt_dest(
        "athena://?bucket=loads&access_key_id=key&secret_access_key=secret&region_name=us-east-1",
        dest_table="aws_catalog.analytics.events",
    )

    assert result.config_params["aws_data_catalog"] == "aws_catalog"
    assert (
        result.config_params["query_result_bucket"]
        == "s3://loads/analytics_staging/metadata"
    )


@pytest.mark.parametrize(
    ("destination", "uri", "table", "expected"),
    [
        (
            MySqlDestination(),
            "mysql://user:pass@host/uri_database",
            "table_database.customers",
            {"dataset_name": "table_database", "table_name": "customers"},
        ),
        (
            MySqlDestination(),
            "mysql://user:pass@host/uri_database",
            "customers",
            {"dataset_name": "uri_database", "table_name": "customers"},
        ),
        (
            SqliteDestination(),
            "sqlite:///warehouse.db",
            "main.customers",
            {"dataset_name": "main", "table_name": "customers"},
        ),
        (
            SqliteDestination(),
            "sqlite:///warehouse.db",
            "customers",
            {"dataset_name": "main", "table_name": "customers"},
        ),
        (
            CsvDestination(),
            "csv:///tmp/out.csv",
            "analytics.customers",
            {"dataset_name": "analytics", "table_name": "customers"},
        ),
    ],
)
def test_two_level_destinations_route_schema_and_table(
    destination, uri, table, expected
):
    assert destination.dlt_run_params(uri, table) == expected


def test_a_bare_name_with_no_default_schema_says_so_rather_than_restating_the_format():
    """A one-component capability lists the bare name as valid, so the format string
    cannot be the error: it would reject the very spelling it says to use."""
    with pytest.raises(ValueError) as excinfo:
        MySqlDestination().dlt_run_params("mysql://user:pass@host/", "customers")

    message = str(excinfo.value)
    assert "names no database" in message
    assert "supplies no default" in message
    assert "<database>.<table>" in message
    assert "must be in the format" not in message


@pytest.mark.parametrize(
    ("destination", "table"),
    [
        (ElasticsearchDestination(), "filebeat-2026.03.15"),
        (ElasticsearchDestination(), ".kibana"),
        (MongoDBDestination(), "database.collection.with.dots"),
    ],
)
def test_opaque_destinations_preserve_dotted_names(destination, table):
    assert destination.dlt_run_params("", table) == {"table_name": table}


@pytest.mark.parametrize(
    ("table", "expected"),
    [
        ("analytics.customers", {"schema": "analytics", "table": "customers"}),
        # a dot inside a quoted identifier is one component, not a separator (#300)
        ('analytics."order.items"', {"schema": "analytics", "table": "order.items"}),
        ("[my schema].[my table]", {"schema": "my schema", "table": "my table"}),
    ],
)
def test_csv_destination_keeps_its_quote_aware_table_parser(table, expected):
    """The ``csv://`` destination shares the ``file://`` writer (#301) but not its plain
    ``str.split('.')``: it keeps the parser it was given in #300, so a quoted dotted
    component survives convergence."""
    destination = CsvDestination()
    assert destination.dlt_run_params("csv:///tmp/out.csv", table) == {
        "dataset_name": expected["schema"],
        "table_name": expected["table"],
    }
    # post_load() locates dlt's staged output by these, so they are recorded too.
    assert (destination.dataset_name, destination.table_name) == (
        expected["schema"],
        expected["table"],
    )


@pytest.mark.parametrize("table", ["customers", "", "a.b.c"])
def test_csv_destination_still_requires_a_qualified_table(table):
    """Unlike ``file://``, which defaults an omitted table from the URI stem."""
    with pytest.raises(ValueError, match=re.escape("<schema>.<table>")):
        CsvDestination().dlt_run_params("csv:///tmp/out.csv", table)
