import os
from typing import Callable


class ArrowMemoryMappedSource:
    table_builder: Callable

    def __init__(self, table_builder=None) -> None:
        if table_builder is None:
            from omniload.source.arrow.adapter import memory_mapped_arrow

            table_builder = memory_mapped_arrow

        self.table_builder = table_builder

    def handles_incrementality(self) -> bool:
        return False

    def dlt_source(self, uri: str, table: str, **kwargs):
        incremental = None
        if kwargs.get("incremental_key"):
            start_value = kwargs.get("interval_start")
            end_value = kwargs.get("interval_end")

            from dlt.extract import Incremental as dlt_incremental

            incremental = dlt_incremental(
                kwargs.get("incremental_key", ""),
                initial_value=start_value,
                end_value=end_value,
                range_end="closed",
                range_start="closed",
            )

        file_path = uri.split("://")[1]
        if not os.path.exists(file_path):
            raise ValueError(f"File at path {file_path} does not exist")

        if os.path.isdir(file_path):
            raise ValueError(
                f"Path {file_path} is a directory, it should be an Arrow memory mapped file"
            )

        primary_key = kwargs.get("primary_key")
        merge_key = kwargs.get("merge_key")

        table_instance = self.table_builder(
            path=file_path,
            incremental=incremental,
            merge_key=merge_key,
            primary_key=primary_key,
        )

        return table_instance
