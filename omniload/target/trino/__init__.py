from typing import Optional, cast

from omniload.target.model import GenericSqlDestination


class TrinoTypeMapper:
    """Custom type mapper for Trino to handle unsupported types."""

    @staticmethod
    def create_type_mapper():
        """Create a custom type mapper for Trino."""
        from dlt.common.destination import PreparedTableSchema
        from dlt.destinations.impl.sqlalchemy.type_mapper import SqlalchemyTypeMapper
        from sqlalchemy import BigInteger, Text
        from sqlalchemy.sql import sqltypes

        class CustomTrinoTypeMapper(SqlalchemyTypeMapper):
            """Custom type mapper that converts unsupported Trino types."""

            def to_destination_type(
                self, column, table: Optional[PreparedTableSchema] = None
            ):
                table: PreparedTableSchema = cast(PreparedTableSchema, table)
                # Handle special cases before calling parent
                data_type = column.get("data_type", "")

                # Convert JSON to VARCHAR for Trino's Iceberg catalog
                if data_type == "json":
                    # Use TEXT (unlimited VARCHAR) for JSON data
                    return Text()

                # Convert BINARY to VARCHAR
                if data_type == "binary":
                    return Text()

                # Handle integer types - always use BIGINT for Trino
                # Note: dlt uses "bigint" internally, not "integer"
                if data_type in ["bigint", "integer", "int"]:
                    return BigInteger()

                # For other types, try parent mapper
                try:
                    type_ = super().to_destination_type(column, table)
                except Exception:
                    # If parent can't handle it, default to TEXT
                    return Text()

                # Convert any INTEGER type to BIGINT
                if isinstance(type_, sqltypes.Integer) and not isinstance(
                    type_, sqltypes.BigInteger
                ):
                    return BigInteger()

                # Ensure VARCHAR types don't have constraints that Trino doesn't support
                if isinstance(type_, sqltypes.String):
                    # Return TEXT for unlimited string
                    return Text()

                return type_

        return CustomTrinoTypeMapper


class TrinoDestination(GenericSqlDestination):
    def dlt_dest(self, uri: str, **kwargs):
        # Import required modules
        from dlt.destinations.impl.sqlalchemy.factory import (
            sqlalchemy as sqlalchemy_factory,
        )

        # Create the destination with custom type mapper
        # We need to use the factory to properly configure the type mapper
        dest = sqlalchemy_factory(
            credentials=uri, type_mapper=TrinoTypeMapper.create_type_mapper(), **kwargs
        )

        return dest
