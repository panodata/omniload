class ExcelSource:
    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):

        path = uri.replace("excel://", "")

        from omniload.source.excel.adapter import excel_source

        return excel_source(path, table)
