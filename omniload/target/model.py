class GenericSqlDestination:
    def dlt_run_params(self, uri: str, table: str, **kwargs) -> dict:
        table_fields = table.split(".")
        if len(table_fields) != 2:
            raise ValueError("Table name must be in the format <schema>.<table>")

        res = {
            "dataset_name": table_fields[-2],
            "table_name": table_fields[-1],
        }

        return res

    def post_load(self):
        pass
