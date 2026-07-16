from typing import Dict, Optional

from dlt.common.schema.typing import TColumnSchema
from dlt.sources import DltResource, DltSource

import omniload.core.resource as resource


def apply_athena_hints(
    source: DltSource | DltResource,
    partition_column: str,
    additional_hints: Optional[Dict[str, TColumnSchema]] = None,
) -> None:
    from dlt.destinations.adapters import athena_adapter, athena_partition

    additional_hints = additional_hints or {}

    def _apply_partition_hint(resource: DltResource) -> None:
        columns = resource.columns if resource.columns else {}

        partition_hint = (
            columns.get(partition_column)  # ty: ignore[unresolved-attribute]
            or additional_hints.get(partition_column)
        )

        athena_adapter(
            resource,
            athena_partition.day(partition_column)
            if partition_hint
            and partition_hint.get("data_type") in ("timestamp", "date")
            else partition_column,
        )

    resource.for_each(source, _apply_partition_hint)
