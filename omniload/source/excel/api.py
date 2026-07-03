import os


class ExcelSource:
    def handles_incrementality(self) -> bool:
        return False

    def dlt_source(self, uri: str, table: str, **kwargs):
        path = uri.split("://", 1)[1]

        if not os.path.exists(path):
            raise ValueError(f"File at path {path} does not exist")

        if os.path.isdir(path):
            raise ValueError(f"Path {path} is a directory, it should be an Excel file")

        from omniload.source.excel.adapter import excel_source

        return excel_source(path, table, **kwargs)
