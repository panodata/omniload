class GenericSqlDestination:
    """Base implementation for SQL destinations that load into schema tables."""

    def dlt_run_params(self, uri: str, table: str, **kwargs) -> dict:
        """Return dlt run parameters derived from a schema-qualified table name."""
        table_fields = table.split(".")
        if len(table_fields) != 2:
            raise ValueError("Table name must be in the format <schema>.<table>")

        res = {
            "dataset_name": table_fields[-2],
            "table_name": table_fields[-1],
        }

        return res

    def post_load(self):
        """Run no destination-specific follow-up work by default."""
        pass
