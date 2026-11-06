"""Reshape transforms for the "fast transformations" harness.

A reshape restructures a source document into a flatter target: flatten nested
objects, coerce types, drop/rename fields, and leave arrays as real lists rather
than opaque JSON. Whether those lists then become child tables is the source's
call, not the reshape's: it needs ``max_table_nesting >= 1``, which the
filesystem readers do not set. There are two execution models:

  - **per-row** (``python``, ``jq``): a ``(doc: dict) -> dict`` mapper, called
    once per document.
  - **batch** (``polars``): a columnar mapper over an Apache Arrow batch,
    applied via dlt ``add_yield_map`` (it yields N dicts per Arrow table). This
    needs the source to deliver Arrow (``data_item_format="arrow"``); only the
    MongoDB source supports that today, so a batch reshape over any other
    source is rejected upstream in :mod:`omniload.api`.

``create_reshape_mapper`` returns a :class:`ReshapeMapper` carrying both the
callable and which model it uses. Both are wired through
``add_yield_map(as_yield_map())``: a per-row mapper cannot go to ``add_map``,
because dlt would hand it a whole Arrow table on a columnar source.

For Mongo sources the caller skips ``TypeHintMap`` while a reshape is active (it
would otherwise json-hint top-level arrays into JSON columns, which prevents
child tables from forming). See :mod:`omniload.api` and
:mod:`omniload.core.resource`.

Spec format mirrors ``--mask``: ``<engine>:<spec>``, split on the first colon
only, so a jq program or a macropipe recipe may itself contain colons.

  - ``python:<module>:<callable>``  resolve a ``(doc: dict) -> dict`` mapper
  - ``jq:<program>``                a jq program, run per document via Tikray
  - ``polars:<recipes>``            macropipe recipes, one per line, run over
    Arrow batches

A ``(doc: dict) -> dict`` callable may also be passed directly via the Python
API.

jq fidelity note: jq operates on JSON, so it cannot serialize BSON
``Decimal128``/``ObjectId`` (the engine coerces them to JSON-native values
first) and it collapses decimal money to a plain number (``120.00`` -> ``120``).
The pure-Python lane preserves ``Decimal``; the jq lane does not. The Polars
lane behaves like jq here (money is cast to ``Float64``, losing ``Decimal``
typing). Cross-lane comparison therefore normalizes numerics rather than
expecting identical column types.
"""

import threading
from dataclasses import dataclass
from typing import Callable

# tikray and macropipe ship in the optional ``[reshape]`` extra, so a default
# install has neither and ty cannot resolve them. Every deferred import of the
# two below carries a `ty: ignore`; the repeated `unused-ignore-comment` code
# keeps that directive quiet in an environment where the extra *is* installed
# and the import does resolve. The imports are parenthesized so the directive
# fits ty's documented trailing placement without `ruff`'s import sorter
# wanting to rewrap the line and carry the comment off it.

#: Guards the check-then-act against macropipe's process-global recipe registry.
_REGISTRY_LOCK = threading.Lock()


@dataclass
class ReshapeMapper:
    """A reshape callable plus how dlt should apply it.

    ``batch`` mappers take a pyarrow Table and yield many dicts; per-row mappers
    take one dict and return one dict. Both are wired through
    ``add_yield_map(as_yield_map())``, which is what lets a per-row mapper survive
    an item that is not a dict.
    """

    fn: Callable
    batch: bool = False

    def as_yield_map(self) -> Callable:
        """Return a generator function suitable for dlt's ``add_yield_map``.

        A batch mapper already has that shape. A per-row mapper does not, and
        cannot simply be handed to ``add_map``: dlt's ``MapItem`` enumerates a
        Python ``list`` and passes anything else through whole, so a source
        yielding Arrow (the default ``pyarrow`` SQL backend, or ``mmap``) would
        hand a ``pyarrow.Table`` to a mapper whose contract is one document. So
        rows are materialized here first.

        That materialization is the cost of running a per-row engine over a
        columnar source; the ``polars`` engine is the one that stays columnar.
        """
        if self.batch:
            return self.fn

        per_row = self.fn

        def apply(item):
            rows = _rows_of(item)
            if rows is None:
                yield per_row(item)
                return
            for row in rows:
                yield per_row(row)

        return apply


def _rows_of(item):
    """Return ``item`` as a list of dicts if it is an Arrow batch, else ``None``.

    Imported lazily and probed by attribute rather than by isinstance, so this
    module stays importable without pyarrow and covers ``Table`` and
    ``RecordBatch`` alike.
    """
    to_pylist = getattr(item, "to_pylist", None)
    if to_pylist is None or isinstance(item, (dict, list)):
        return None
    return to_pylist()


def create_reshape_mapper(
    reshape: "str | Callable[[dict], dict]",
) -> ReshapeMapper:
    """Return a :class:`ReshapeMapper` for ``reshape`` (a spec string or a callable)."""
    if not isinstance(reshape, str):
        # already a (doc: dict) -> dict callable (Python API path)
        return ReshapeMapper(fn=reshape, batch=False)

    if ":" not in reshape:
        raise ValueError(
            "reshape must be a callable or a '<engine>:<spec>' string "
            "(e.g. 'python:my.module:reshape')"
        )

    engine, spec = reshape.split(":", 1)

    if engine == "python":
        return ReshapeMapper(fn=_resolve_python_callable(spec), batch=False)

    if engine == "jq":
        return ReshapeMapper(fn=_create_jq_mapper(spec), batch=False)

    if engine == "polars":
        return ReshapeMapper(fn=_create_polars_mapper(spec), batch=True)

    raise ValueError(f"unknown reshape engine '{engine}' (expected python|jq|polars)")


def _resolve_python_callable(spec: str) -> "Callable[[dict], dict]":
    """Resolve a ``module.path:callable`` spec to a callable."""
    import importlib

    if ":" not in spec:
        raise ValueError(
            f"python reshape spec must be 'module.path:callable', got '{spec}'"
        )
    module_path, _, attr = spec.partition(":")
    module = importlib.import_module(module_path)
    # Default to None rather than letting getattr raise: an absent attribute (or a
    # spec carrying an extra colon, which lands here as one) is a spec error, and
    # the caller only converts ValueError into a user-facing ValidationError.
    fn = getattr(module, attr, None)
    if not callable(fn):
        raise ValueError(f"python reshape target '{spec}' is not callable")
    return fn


def _create_jq_mapper(program: str) -> "Callable[[dict], dict]":
    """Return a per-row mapper running the jq ``program`` per document via Tikray."""
    try:
        from tikray import (  # ty: ignore[import-not-found, unused-ignore-comment, unused-ignore-comment]
            MokshaTransformation,
        )
    except ImportError as exc:  # pragma: no cover - exercised only without the extra
        raise ImportError(
            "the jq reshape engine needs Tikray; install the optional extra: "
            "pip install 'omniload[reshape]'"
        ) from exc

    transformation = MokshaTransformation().jq(program)

    def mapper(doc: dict) -> dict:
        # jq serializes its input as JSON, so BSON/Decimal types are coerced first.
        return transformation.apply(_to_jsonish(doc))

    return mapper


def _create_polars_mapper(spec: str) -> "Callable":
    """Return a batch mapper reshaping an Arrow table via macropipe (compiled Polars).

    ``spec`` is a macropipe recipe list, one recipe per line (blank lines and
    ``#`` comments ignored). Each line is a ``<function>:<arg>:<arg>`` macro (see
    macropipe's expression language); ``omniload`` registers a couple of custom
    recipes (see ``_register_polars_recipes``) for the nested reshapes that
    macropipe's builtins don't cover.

    The returned callable takes one pyarrow Table (an extraction batch) and
    yields the reshaped rows as dicts, so dlt's normalizer builds the child
    tables. It is wired via ``add_yield_map`` (one Arrow table in, N dicts out).

    Per-batch schema assumption: pymongoarrow infers the Arrow schema from each
    batch, so a field that is null in *some* rows is fine (the recipes are
    null-safe), but a field absent from *every* row in a batch produces no
    column, and a recipe referencing it then raises a Polars
    ``ColumnNotFoundError``. A stable Arrow schema would avoid it, but no omniload
    entry point exposes one yet (``pymongoarrow_schema`` reaches the adapter, not
    ``LoadRequest``), so a sparse collection wants a per-row engine. The harness
    fixture keeps all docs in one batch, so its sometimes-absent money/address
    fields stay present-but-null.
    """
    try:
        import polars as pl
        from macropipe.core import (  # ty: ignore[import-not-found, unused-ignore-comment, unused-ignore-comment]
            MacroPipe,
        )
    except ImportError as exc:  # pragma: no cover - exercised only without the extra
        raise ImportError(
            "the polars reshape engine needs macropipe; install the optional extra: "
            "pip install 'omniload[reshape]'"
        ) from exc

    _register_polars_recipes()

    recipes = [
        line.strip()
        for line in spec.splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]
    if not recipes:
        raise ValueError(
            "polars reshape spec is empty; provide one macropipe recipe per line "
            "(e.g. 'select:_id,name')"
        )
    pipeline = MacroPipe.from_recipes(*recipes)

    def mapper(table):
        # table: one pyarrow Table/RecordBatch per extraction batch. from_arrow
        # on a tabular item always yields a DataFrame (the Series arm is for
        # arrays); check it so a misrouted non-batch item fails loudly here
        # rather than mid-pipeline.
        frame = pl.from_arrow(table)
        if not isinstance(frame, pl.DataFrame):
            raise TypeError(
                "polars reshape expected an Arrow table batch, got "
                f"{type(frame).__name__}"
            )
        result = pipeline.apply(frame.lazy()).collect()
        yield from result.to_dicts()

    return mapper


def _register_polars_recipes() -> None:
    """Register omniload's custom macropipe recipes (idempotent).

    macropipe ``0.0.0`` ships only scalar-column builtins
    (cast/select/rename/...), and its ``cast`` is strict. The convoluted Mongo
    reshape needs two things its builtins lack, so we register them as
    ``@recipe`` functions. That a nested reshape has to extend macropipe at all
    is one of the harness findings:

      - ``geojson_point_flatten:<struct_col>:<lng_alias>:<lat_alias>`` flattens
        a GeoJSON ``<struct_col>.location.coordinates`` ``[lng, lat]`` list into
        two typed columns, null-safe when the address or coordinates are absent.
      - ``cast_number:<col,col,...>`` casts columns to ``Float64`` non-strict,
        so the Mongo source's stringified money (absent values arrive as the
        literal ``"None"``) becomes a number or null instead of raising.
    """
    import polars as pl
    from macropipe.registry import (  # ty: ignore[import-not-found, unused-ignore-comment, unused-ignore-comment]
        Registry,
    )

    def geojson_point_flatten(lazy_frame, struct_col, lng_alias, lat_alias):
        coordinates = (
            pl.col(struct_col).struct.field("location").struct.field("coordinates")
        )
        return lazy_frame.with_columns(
            [
                coordinates.list.get(0, null_on_oob=True).alias(lng_alias),
                coordinates.list.get(1, null_on_oob=True).alias(lat_alias),
            ]
        )

    def cast_number(lazy_frame, column_names):
        columns = [name.strip() for name in column_names.split(",")]
        return lazy_frame.with_columns(
            [pl.col(name).cast(pl.Float64, strict=False) for name in columns]
        )

    # Register each recipe independently and only if absent: macropipe's Registry is
    # a process-global class var and Registry.register raises on a duplicate name, so
    # a blanket "register all" would break a re-run (and a one-name guard could leave
    # a partial set). Idempotent per name, and serialised, because the membership
    # check and the register call are two operations against shared state: two
    # threads building their first polars mapper can otherwise both see the name
    # absent and the loser raises.
    with _REGISTRY_LOCK:
        for fn in (geojson_point_flatten, cast_number):
            existing = Registry.r.get(fn.__name__)
            if existing is None:
                Registry.register(fn)
            elif (
                existing is not fn and getattr(existing, "__module__", None) != __name__
            ):
                raise ValueError(
                    f"macropipe recipe '{fn.__name__}' is already registered by "
                    f"{getattr(existing, '__module__', 'an unknown module')}; "
                    "omniload cannot register its own under that name"
                )


def _to_jsonish(value):
    """Recursively coerce BSON / Decimal values into JSON-native ones jq can serialize.

    ``Decimal128``/``Decimal`` -> ``float`` (jq has no decimal type, so money
    loses its ``Decimal`` typing here; see the module docstring), ``ObjectId``
    -> ``str``, ``datetime``/``date`` -> ISO string. Containers recurse;
    JSON-native scalars pass through. An unsupported type raises ``TypeError``
    rather than being silently ``str()``-ed: a harness lane should fail loudly
    instead of corrupting a value (``bytes``, ``bson.Binary``, regex/code BSON).
    Extend the coercion here if a real source needs another type.
    """
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {k: _to_jsonish(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_jsonish(v) for v in value]

    from datetime import date, datetime
    from decimal import Decimal

    from bson import Decimal128, ObjectId

    if isinstance(value, Decimal128):
        return float(value.to_decimal())
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    raise TypeError(
        f"jq reshape cannot serialize {type(value).__name__}; "
        "extend _to_jsonish in omniload.codec.reshape to coerce it"
    )
