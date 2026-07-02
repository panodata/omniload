class ExcelSource:
    def handles_incrementality(self) -> bool:
        return False

    def dlt_source(self, uri: str, table: str, **kwargs):
        path = uri.split("://", 1)[1]

        from omniload.source.excel.adapter import excel_source

        return excel_source(path, table, **kwargs)
