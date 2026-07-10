from typing import Callable

from dlt.sources import DltResource, DltSource


def for_each(
    source: DltSource | DltResource, ex: Callable[[DltResource], None | DltResource]
):
    """
    Apply a function to each resource in a source.
    """
    if (
        hasattr(source, "selected_resources")
        and source.selected_resources
        and isinstance(source.selected_resources, dict)
    ):
        resource_names = list(source.selected_resources.keys())
        for res in resource_names:
            ex(
                source.resources[  # ty: ignore[unresolved-attribute,invalid-argument-type]
                    res
                ]
            )
    else:
        ex(source)  # ty: ignore[invalid-argument-type]


class TypeHintMap:
    """Apply inferred JSON type hints for array-like values in a resource item."""

    def __init__(self):
        """Track whether hints have already been applied for this mapper."""
        self.handled_typehints = False

    def type_hint_map(self, item):
        """Mark list and tuple columns as JSON on the current dlt source."""
        if self.handled_typehints:
            return item

        array_cols = []
        for col in item:
            if isinstance(item[col], (list, tuple)):
                array_cols.append(col)
        if array_cols:
            import dlt

            source = dlt.current.source()
            columns = [{"name": col, "data_type": "json"} for col in array_cols]
            for_each(
                source,
                lambda x: x.apply_hints(
                    columns=columns  # ty: ignore[invalid-argument-type]
                ),
            )

        self.handled_typehints = True
        return item
