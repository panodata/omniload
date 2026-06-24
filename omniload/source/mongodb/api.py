from typing import Callable

from dlt.common.time import ensure_pendulum_datetime_utc

from omniload.core.model import table_string_to_dataclass


class MongoDbSource:
    table_builder: Callable

    def __init__(self, table_builder=None) -> None:
        if table_builder is None:
            from omniload.source.mongodb.adapter import mongodb_collection

            table_builder = mongodb_collection

        self.table_builder = table_builder

    def handles_incrementality(self) -> bool:
        return False

    def dlt_source(self, uri: str, table: str, **kwargs):
        from dlt.extract import Incremental as dlt_incremental

        # Check if this is a custom query format (collection:query)
        if ":" in table:
            collection_name, query_json = table.split(":", 1)

            # Parse the query using MongoDB's extended JSON parser
            # First, convert MongoDB shell syntax to Extended JSON format
            from bson import json_util

            from omniload.source.mongodb.helpers import (
                convert_mongo_shell_to_extended_json,
            )

            # Convert MongoDB shell constructs to Extended JSON v2 format
            converted_query = convert_mongo_shell_to_extended_json(query_json)

            try:
                query = json_util.loads(converted_query)
            except Exception as e:
                raise ValueError(f"Invalid MongoDB query format: {e}")

            # Validate that it's a list for aggregation pipeline
            if not isinstance(query, list):
                raise ValueError(
                    "Query must be a JSON array representing a MongoDB aggregation pipeline"
                )

            # Check for incremental load requirements
            incremental = None
            if kwargs.get("incremental_key"):
                start_value = kwargs.get("interval_start")
                end_value = kwargs.get("interval_end")

                # Validate that incremental key is present in the pipeline
                incremental_key = kwargs.get("incremental_key")
                self._validate_incremental_query(query, str(incremental_key))

                incremental = dlt_incremental(
                    str(incremental_key),
                    initial_value=start_value,
                    end_value=end_value,
                )

                # Substitute interval parameters in the query
                query = self._substitute_interval_params(query, kwargs)

            # Parse collection name to get database and collection
            if "." in collection_name:
                # Handle database.collection format
                table_fields = table_string_to_dataclass(collection_name)
                database = table_fields.dataset
                collection = table_fields.table
            else:
                # Single collection name, use default database
                database = None
                collection = collection_name

            table_instance = self.table_builder(
                connection_url=uri,
                database=database,
                collection=collection,
                parallel=False,
                incremental=incremental,
                custom_query=query,
            )
            table_instance.max_table_nesting = 1
            return table_instance
        else:
            # Default behavior for simple collection names
            table_fields = table_string_to_dataclass(table)

            incremental = None
            if kwargs.get("incremental_key"):
                start_value = kwargs.get("interval_start")
                end_value = kwargs.get("interval_end")

                incremental = dlt_incremental(
                    kwargs.get("incremental_key", ""),
                    initial_value=start_value,
                    end_value=end_value,
                )

            table_instance = self.table_builder(
                connection_url=uri,
                database=table_fields.dataset,
                collection=table_fields.table,
                parallel=False,
                incremental=incremental,
            )
            table_instance.max_table_nesting = 1

            return table_instance

    def _validate_incremental_query(self, query: list, incremental_key: str):
        """Validate that incremental key is projected in the aggregation pipeline"""
        # Check if there's a $project stage and if incremental_key is included
        has_project = False
        incremental_key_projected = False

        for stage in query:
            if "$project" in stage:
                has_project = True
                project_stage = stage["$project"]
                if isinstance(project_stage, dict):
                    # Check if incremental_key is explicitly included
                    if incremental_key in project_stage:
                        if project_stage[incremental_key] not in [0, False]:
                            incremental_key_projected = True
                    # If there are only inclusions (1 or True values) and incremental_key is not included
                    elif any(v in [1, True] for v in project_stage.values()):
                        # This is an inclusion projection, incremental_key must be explicitly included
                        incremental_key_projected = False
                    # If there are only exclusions (0 or False values) and incremental_key is not excluded
                    elif all(
                        v in [0, False]
                        for v in project_stage.values()
                        if v in [0, False, 1, True]
                    ):
                        # This is an exclusion projection, incremental_key is included by default
                        if incremental_key not in project_stage:
                            incremental_key_projected = True
                        else:
                            incremental_key_projected = project_stage[
                                incremental_key
                            ] not in [0, False]
                    else:
                        # Mixed or unclear projection, assume incremental_key needs to be explicit
                        incremental_key_projected = False

        # If there's a $project stage but incremental_key is not projected, raise error
        if has_project and not incremental_key_projected:
            raise ValueError(
                f"Incremental key '{incremental_key}' must be included in the projected fields of the aggregation pipeline"
            )

    def _substitute_interval_params(self, query: list, kwargs: dict):
        """Substitute :interval_start and :interval_end placeholders with actual datetime values"""

        # Get interval values and convert them to datetime objects
        interval_start = kwargs.get("interval_start")
        interval_end = kwargs.get("interval_end")

        # Convert string dates to datetime objects if needed
        if interval_start is not None:
            if isinstance(interval_start, str):
                pendulum_dt = ensure_pendulum_datetime_utc(interval_start)
                interval_start = (
                    pendulum_dt.to_datetime_string()
                    if hasattr(pendulum_dt, "to_datetime_string")
                    else pendulum_dt
                )
            elif hasattr(interval_start, "to_datetime"):
                interval_start = interval_start.to_datetime()

        if interval_end is not None:
            if isinstance(interval_end, str):
                pendulum_dt = ensure_pendulum_datetime_utc(interval_end)
                interval_end = (
                    pendulum_dt.to_datetime_string()
                    if hasattr(pendulum_dt, "to_datetime_string")
                    else pendulum_dt
                )
            elif hasattr(interval_end, "to_datetime"):
                interval_end = interval_end.to_datetime()

        # Deep copy the query and replace placeholders with actual datetime objects
        def replace_placeholders(obj):
            if isinstance(obj, dict):
                result = {}
                for key, value in obj.items():
                    if value == ":interval_start" and interval_start is not None:
                        result[key] = interval_start
                    elif value == ":interval_end" and interval_end is not None:
                        result[key] = interval_end
                    else:
                        result[key] = replace_placeholders(value)
                return result
            elif isinstance(obj, list):
                return [replace_placeholders(item) for item in obj]
            else:
                return obj

        return replace_placeholders(query)
