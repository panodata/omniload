"""BSON extended-type normalization for the filesystem BSON reader.

Self-contained mirror of ``omniload.source.mongodb.helpers.convert_mongo_objs`` that
deliberately does *not* import the Mongo source: ``mongodb/helpers.py`` imports the
``pymongo`` client classes (``MongoClient``/``Collection``/``Cursor``) at module top,
which would couple this filesystem reader to the Mongo driver. ``bson`` itself ships
with ``pymongo`` (a hard dependency), so decoding needs no extra package.

This module imports the ``bson`` submodules at its own top level rather than lazily,
because it is itself imported lazily (only from ``_read_bson``), so the cost is paid
only when a BSON file is actually read, never at CLI startup or for other formats.

It goes beyond the Mongo helper's coverage: besides the shared ObjectId / Decimal128 /
datetime / Regex / Timestamp cases, it converts ``Binary`` (base64 str) and the extended
types the Mongo helper leaves raw (``DBRef``, ``MinKey``, ``MaxKey``, ``Code``). Those
raw BSON objects are not JSON-serializable, so without conversion a dump containing one
crashes the load (``TypeError: Type is not JSON serializable``); here they become
portable Extended-JSON-shaped values.
"""

import base64
import datetime as _datetime
from typing import Any

from bson.code import Code
from bson.dbref import DBRef
from bson.decimal128 import Decimal128
from bson.max_key import MaxKey
from bson.min_key import MinKey
from bson.objectid import ObjectId
from bson.regex import Regex
from bson.timestamp import Timestamp
from dlt.common.time import ensure_pendulum_datetime_utc
from dlt.common.utils import map_nested_values_in_place


def convert_bson_objs(value: Any) -> Any:
    """Convert a single BSON extended value to a dlt-serializable Python type.

    Applied to leaf values only: ``map_nested_values_in_place`` recurses into nested
    dicts and lists and calls this on the scalars, so this never sees a plain container
    (BSON extended types like ``DBRef`` are objects, not dict/list, so they do reach
    here). Conversions:

    - ``Binary`` (and any raw ``bytes``) -> base64 ``str``
    - ``ObjectId`` / ``Decimal128`` -> ``str``
    - ``datetime`` -> pendulum UTC ``datetime``
    - ``Regex`` -> pattern ``str``
    - ``Timestamp`` -> pendulum UTC ``datetime``
    - ``DBRef`` -> ``{"$ref", "$id" (normalized), "$db"?}``
    - ``MinKey`` / ``MaxKey`` -> ``{"$minKey": 1}`` / ``{"$maxKey": 1}``
    - ``Code`` -> code ``str`` (or ``{"$code", "$scope" (normalized)}`` when it has scope)
    - anything else -> unchanged
    """
    # Binary subclasses bytes, so the bytes branch must come first and covers both. Raw
    # bytes are emitted as base64 str because dlt's CSV/text writer UTF-8-decodes binary
    # values (raises on arbitrary bytes); base64 str is portable across jsonl, file,
    # parquet and warehouse targets. See PLAN Decisions §1.
    if isinstance(value, (bytes, bytearray)):
        return base64.b64encode(bytes(value)).decode("ascii")
    if isinstance(value, (ObjectId, Decimal128)):
        return str(value)
    if isinstance(value, _datetime.datetime):
        return ensure_pendulum_datetime_utc(value)
    if isinstance(value, Regex):
        # value.pattern is the raw pattern string; do NOT try_compile() it (BSON regexes
        # can carry PCRE-only syntax that Python's re rejects, which would crash the read
        # of an otherwise valid dump).
        return value.pattern
    if isinstance(value, Timestamp):
        return ensure_pendulum_datetime_utc(value.as_datetime())
    if isinstance(value, DBRef):
        # $id is normalized recursively (it is typically an ObjectId). The convert()
        # return is used as-is by the caller (map_nested does not re-descend into it), so
        # the id must be converted here rather than relying on outer recursion.
        ref: dict[str, Any] = {
            "$ref": value.collection,
            "$id": convert_bson_objs(value.id),
        }
        if value.database is not None:
            ref["$db"] = value.database
        return ref
    if isinstance(value, MinKey):
        return {"$minKey": 1}
    if isinstance(value, MaxKey):
        return {"$maxKey": 1}
    if isinstance(value, Code):
        # Code subclasses str, so str(value) is the code text. Scope (when present) is a
        # nested doc that needs its own normalization pass.
        if value.scope:
            return {
                "$code": str(value),
                "$scope": map_nested_values_in_place(
                    convert_bson_objs, dict(value.scope)
                ),
            }
        return str(value)

    return value
