"""Generic bridge turning long-tail file formats into dlt filesystem readers.

This wraps per-format decoders into the same ``Iterator[FileItemDict] -> Iterator[TDataItems]``
shape the other filesystem readers use (see ``readers.read_bson`` for the template), so a
curated set of long-tail formats becomes loadable through the existing
``file:// / s3:// / gs:// / az:// / sftp://`` sources without re-plumbing storage.

A format is read one of two ways (chosen per format, see ``FORMAT_TO_ITERABLE``):

- **Streaming via `iterabledata`** (e.g. MessagePack). `iterabledata` (PyPI ``iterabledata``,
  import package ``iterable``) exposes a uniform per-format class that takes a ``stream=`` file
  object and yields record dicts via ``read_bulk(n)``. `dlt-filesystem` feeds it the already-
  authenticated ``file_obj.open()`` handle, so remote sources keep flowing through dlt's fsspec
  layer and iterabledata's own cloud-storage / credential path is never touched. Note a
  streaming wire format like MessagePack carries no length prefix, so a truncated tail reads as
  a clean EOF and its records are dropped without error, this is inherent to the format and not
  detectable here (unlike the whole-file decode below).
- **Whole-file direct decode** (e.g. CBOR). Some formats are whole-file rather than streaming,
  and iterabledata's wrapper for them wraps its decode in a bare ``except Exception`` that
  yields nothing, so a truncated / corrupt file would load as zero rows with **no error**
  (silent data loss). For those, `dlt-filesystem` decodes the bytes with the format's own library
  directly (``eager_decoder``) so decode errors propagate. This also matches the project's
  per-format routing policy: route to iterabledata only where it is the better path.

Design decisions (verified against iterabledata 1.0.15; see GitHub issue #45):

- **Seekable spool.** iterabledata's ctor calls ``reset() -> seek(0)``, which raises on a
  non-seekable handle (pipes, some compressed / SFTP streams). A seekable handle takes the
  direct path; a non-seekable one is spooled into a ``BytesIO`` first. (Whole-file buffering; a
  tempfile spool for very large non-seekable inputs is a follow-up.)
- **Optional per-format normalizer.** Decoders do not normalize extended types; a format that
  decodes to non-dlt-serializable values (msgpack ``bytes`` / ``Timestamp``; cbor ``bytes`` /
  unknown tags) carries a small normalizer, applied per row via ``map_nested_values_in_place``
  exactly like the BSON reader.
- **Lazy imports.** ``iterable.*`` and each per-format decoder (``msgpack``, ``cbor2``, ...) are
  imported only inside the reader, so importing this module (and the CLI) never pulls them.
- **Typed missing-decoder error.** The ``iterable`` extra installs the shipped tranche's
  decoders, but if one is absent the reader raises ``MissingDecoderError`` naming the exact
  ``pip install`` instead of a bare ``ImportError``.
"""

from __future__ import annotations

import base64
import importlib
import importlib.util
import io
from dataclasses import dataclass
from typing import Any, Callable, Iterator, Optional

from dlt.common.time import ensure_pendulum_datetime_utc
from dlt.common.typing import TDataItems
from dlt.common.utils import map_nested_values_in_place
from dlt.sources.filesystem import FileItemDict

from dlt_filesystem.source.error import (
    MissingDecoderError,
    MissingReaderOptionError,
)

Normalizer = Callable[[Any], Any]
EagerDecoder = Callable[[bytes, dict], Iterator[Any]]


@dataclass(frozen=True)
class IterableFormat:
    """One routable long-tail file format.

    A format is read either by streaming through an ``iterabledata`` class (set ``class_path``)
    or by decoding the whole file directly (set ``eager_decoder``); exactly one must be given.

    Attributes:
        decoder_dist: Top-level import name of the decoder package that must be present to read
            the format (``msgpack``, ``cbor2``, ...). Probed with ``find_spec`` both to fail
            fast and to decide whether to advertise the format as supported.
        pip_hint: Exact ``pip install`` target named in the missing-decoder error.
        class_path: Dotted path to the ``iterable.datatypes.*`` class for a streaming format,
            e.g. ``"iterable.datatypes.msgpack.MessagePackIterable"``. The module name inside
            ``iterable.datatypes`` is non-obvious (msgpack lives in ``msgpack``, BSON in
            ``bsonf``), so it is pinned here and covered by an import-path test.
        eager_decoder: For a whole-file format, a callable ``(bytes, options) -> Iterator[record]``
            that decodes the whole file and surfaces decode errors (instead of routing through an
            iterabledata wrapper that would swallow them). ``options`` carries the per-URI reader
            hints forwarded from the ``#key=value`` fragment (e.g. XML's mandatory ``tagname``);
            a format that takes no options ignores it. ``iterable`` is not imported for these
            formats.
        normalizer_factory: Optional builder returning a leaf-normalizer closure applied to
            every row. ``None`` when the format already decodes to dlt-safe values.
    """

    decoder_dist: str
    pip_hint: str
    class_path: Optional[str] = None
    eager_decoder: Optional[EagerDecoder] = None
    normalizer_factory: Optional[Callable[[], Normalizer]] = None

    def __post_init__(self) -> None:
        """Reject a spec that sets neither or both of ``class_path`` / ``eager_decoder``."""
        if bool(self.class_path) == bool(self.eager_decoder):
            raise ValueError(
                "IterableFormat needs exactly one of class_path / eager_decoder"
            )


def _msgpack_normalizer() -> Normalizer:
    """Build the msgpack leaf normalizer.

    iterabledata reads msgpack with a hardcoded ``Unpacker(raw=False, use_list=True)``, so
    ``bytes`` values stay ``bytes`` and timestamp extensions decode to ``msgpack.Timestamp``,
    neither of which is dlt-serializable. Mirror the BSON reader: ``bytes`` -> base64 ``str``
    (portable across text and Parquet loaders), ``Timestamp`` -> pendulum UTC ``datetime``.
    (A custom ``msgpack.ExtType`` is a namedtuple, so ``map_nested_values_in_place`` already
    turns it into a ``{"code", "data"}`` dict and this normalizer base64s its ``data``.)
    ``msgpack`` is imported here (once, when the reader is built) so the module top stays
    decoder-free.
    """
    import msgpack

    timestamp_type = msgpack.Timestamp

    def convert(value: Any) -> Any:
        """Convert one msgpack leaf: ``bytes`` -> base64 str, ``Timestamp`` -> UTC datetime."""
        if isinstance(value, (bytes, bytearray)):
            return base64.b64encode(bytes(value)).decode("ascii")
        if isinstance(value, timestamp_type):
            return ensure_pendulum_datetime_utc(value.to_datetime())
        return value

    return convert


def _cbor_normalizer() -> Normalizer:
    """Build the CBOR leaf normalizer.

    cbor2 already decodes the well-known tags (datetime, bignum, Decimal) into native Python
    types, which are dlt-safe; only ``bytes`` needs the base64 treatment (as for msgpack /
    BSON). A surviving ``CBORTag`` is an *unknown/custom* tag with no native mapping, so it is
    represented portably as ``{"tag": n, "value": ...}`` (its payload normalized too) instead
    of crashing the load on a non-serializable object. ``cbor2`` is imported here (once, when
    the reader is built) so the module top stays decoder-free.
    """
    import cbor2

    tag_type = cbor2.CBORTag

    def convert(value: Any) -> Any:
        """Convert one CBOR leaf: ``bytes`` -> base64 str, unknown ``CBORTag`` -> ``{tag, value}``."""
        if isinstance(value, (bytes, bytearray)):
            return base64.b64encode(bytes(value)).decode("ascii")
        if isinstance(value, tag_type):
            return {"tag": value.tag, "value": _normalize_row(convert, value.value)}
        return value

    return convert


def _cbor_eager_decode(data: bytes, options: dict) -> Iterator[Any]:
    """Decode a whole CBOR file into records, surfacing decode errors.

    A CBOR source must be a single top-level value: a top-level array yields one row per
    element; any other single value yields one row. An empty file yields no rows (matching the
    other readers), but a non-empty **corrupt / truncated** payload raises ``CBORDecodeError``
    rather than silently loading nothing (which is what iterabledata's error-swallowing
    ``CBORIterable`` would do). Files that concatenate several top-level objects are read only
    up to the first (a cbor2 limitation that cannot be detected). CBOR takes no reader options,
    so ``options`` is accepted (for the uniform eager-decoder signature) and ignored.
    """
    import cbor2

    if not data:
        return
    obj = cbor2.loads(data)
    if isinstance(obj, list):
        yield from obj
    else:
        yield obj


def _yaml_normalizer() -> Normalizer:
    """Build the YAML leaf normalizer.

    ``yaml.safe_load`` decodes two tags to types a text / Parquet loader cannot serialize:
    ``!!binary`` -> ``bytes`` and ``!!set`` -> ``set``. Convert ``bytes`` to a base64 string
    (as msgpack / cbor / BSON do) and a ``set`` to a list (JSON / Parquet-safe), normalizing
    each member too. ``!!timestamp`` decodes to ``datetime`` / ``date``, which are already
    dlt-safe and pass through. No decoder import is needed (the values are plain Python types),
    so this normalizer is dependency-free.
    """

    def convert(value: Any) -> Any:
        """Convert one YAML leaf: ``bytes`` -> base64 str, ``set`` -> list of converted members."""
        if isinstance(value, (bytes, bytearray)):
            return base64.b64encode(bytes(value)).decode("ascii")
        if isinstance(value, (set, frozenset)):
            return [convert(member) for member in value]
        return value

    return convert


def _yaml_eager_decode(data: bytes, options: dict) -> Iterator[Any]:
    """Decode a whole YAML file into records, surfacing parse errors.

    Uses ``yaml.safe_load_all`` (never ``load``), so a tag that would construct an arbitrary
    Python object (``!!python/object/...``) is rejected with a ``yaml.YAMLError`` instead of
    executed. Each YAML **document** becomes rows: a document that is a **list** expands to one
    row per element (so a top-level sequence of records loads naturally); any other document
    yields one row. A ``---``-only / empty document parses to ``None`` and is skipped (it
    carries no record). An empty file yields no rows (matching the other readers); a malformed
    document raises ``yaml.YAMLError`` rather than silently loading nothing. YAML takes no
    reader options, so ``options`` is accepted (for the uniform eager-decoder signature) and
    ignored.
    """
    import yaml

    if not data.strip():
        return
    for document in yaml.safe_load_all(data):
        if document is None:
            continue
        if isinstance(document, list):
            yield from document
        else:
            yield document


# Safe lxml parse flags, hardened against the XXE / entity-expansion / external-DTD / encoding
# attack classes (see issue #45): entities are not resolved (``resolve_entities=False``), no DTD
# is loaded (``load_dtd=False``), nothing is fetched over the network (``no_network=True``),
# pathological trees are capped (``huge_tree=False``), and a malformed document raises rather
# than being silently recovered (``recover=False``). So an **external** entity is never read or
# fetched: in element text its reference is dropped, and in an attribute value it raises rather
# than resolving. (An *internal* entity defined inline in the document still expands inside an
# attribute value, because XML normalizes attribute values to strings; that is document-local
# text, and libxml2's amplification cap bounds any expansion.) Passed as kwargs to
# ``etree.iterparse``, which builds its own parser from them.
_XML_PARSER_FLAGS: dict[str, bool] = {
    "resolve_entities": False,
    "load_dtd": False,
    "no_network": True,
    "huge_tree": False,
    "recover": False,
}


def _xml_localname(tag: str) -> str:
    """Return an XML tag's local name, stripping any ``{namespace}`` prefix.

    ``"{http://ex/}item"`` -> ``"item"``; a plain ``"item"`` is returned unchanged. Done with
    ``rpartition`` (not ``etree.QName``) so it stays a cheap string op in the per-element hot
    path with no lxml import.
    """
    return tag.rpartition("}")[2]


def _xml_element_to_record(elem: Any) -> Any:
    """Convert one XML row element to a portable value per the pinned convention.

    - A **leaf** (no attributes, no element children) -> its stripped text, or ``None`` if empty.
    - Otherwise a **dict**: attributes under ``@name``, non-empty leading text under ``#text``,
      and each child element under its local tag name. Repeated child tags collapse into a list
      in document order. The ``@`` / ``#`` prefixes keep attribute and text keys from ever
      colliding with a child element's key.

    Comments, processing instructions and **unexpanded entity references** are skipped by the
    ``isinstance(child.tag, str)`` guard (only real elements carry a ``str`` tag). That guard is
    what keeps an unresolved external entity from leaking its target into a row: with
    ``resolve_entities=False`` an ``&xxe;`` reference is an entity child, never text, so it is
    dropped here. Inter-element tail text is not captured; namespaces are stripped to local
    names (a name clash across namespaces collapses into the same list).
    """
    child_elems = [child for child in elem if isinstance(child.tag, str)]
    if not elem.attrib and not child_elems:
        text = (elem.text or "").strip()
        return text or None

    record: dict[str, Any] = {}
    for name, value in elem.attrib.items():
        record["@" + _xml_localname(name)] = value
    leading_text = (elem.text or "").strip()
    if leading_text:
        record["#text"] = leading_text
    for child in child_elems:
        key = _xml_localname(child.tag)
        value = _xml_element_to_record(child)
        if key not in record:
            record[key] = value
        elif isinstance(record[key], list):
            record[key].append(value)
        else:
            record[key] = [record[key], value]
    return record


def _xml_eager_decode(data: bytes, options: dict) -> Iterator[Any]:
    """Decode a whole XML file into row records with a hardened parser.

    Requires a ``tagname`` reader option (from ``#tagname=<row-tag>``) naming the repeated
    element that is one row; without it a :class:`MissingReaderOptionError` is raised (never a
    bare ``AttributeError``). The row tag is matched both bare and in any namespace
    (``(tagname, "{*}" + tagname)``) so namespaced feeds work without the caller writing Clark
    notation. Parsing uses :data:`_XML_PARSER_FLAGS`, which neutralizes XXE / entity-expansion /
    external-DTD attacks; a corrupt or disallowed document raises ``lxml.etree.XMLSyntaxError``.

    When the row tag nests inside another row tag (recursive / hierarchical XML), only the
    **outermost** match is a row; an inner occurrence is a nested sub-record of its ancestor row,
    not a separate row. This avoids emitting the same element twice and avoids corrupting the
    ancestor row during memory cleanup.

    Memory note: the eager seam hands the whole file's bytes to this decoder, so the file is
    buffered in full (like CBOR / YAML). ``iterparse`` bounds only the parsed **element tree** --
    each outermost row is cleared and its processed siblings dropped after it is yielded -- not
    the raw bytes. True larger-than-memory streaming is a separate follow-up. An empty (or
    whitespace-only) file yields no rows.
    """
    tagname = options.get("tagname")
    if not tagname:
        raise MissingReaderOptionError("tagname", "xml", "file://data.xml#tagname=item")
    if not data.strip():
        return

    from lxml import etree  # ty: ignore[unresolved-import]

    matcher = (tagname, "{*}" + tagname)
    context = etree.iterparse(
        io.BytesIO(data), events=("start", "end"), tag=matcher, **_XML_PARSER_FLAGS
    )
    # Depth of matching (row-tag) elements currently open. A row is only emitted when its `end`
    # brings the depth back to 0, i.e. it is not nested inside another row tag -- so an inner
    # match is left as part of its ancestor row instead of being yielded (and cleared) early,
    # which would delete the ancestor's own not-yet-processed children.
    depth = 0
    for event, elem in context:
        if event == "start":
            depth += 1
            continue
        depth -= 1
        if depth != 0:
            continue  # a nested match; its outermost ancestor row already contains it
        yield _xml_element_to_record(elem)
        # Free the parsed row and any already-processed preceding siblings so the parent does
        # not accumulate the whole tree (bounds the *element tree*, not the buffered bytes).
        elem.clear()
        parent = elem.getparent()
        if parent is not None:
            while elem.getprevious() is not None:
                del parent[0]


# Lexically sorted by format name. XML and YAML both use the whole-file `eager_decoder` seam
# (never an iterabledata class): iterabledata's XML parser resolves entities and can't be locked
# down through its API, and its YAML wrapper is eager and swallows parse errors, so
# `dlt-filesystem` owns both decodes -- a safe lxml parse for XML, `yaml.safe_load_all` for YAML.
# See `docs/getting-started/file-format-routing.md`.
# TODO: Adjust `pip_hint` values after breaking out into dedicated package.
FORMAT_TO_ITERABLE: dict[str, IterableFormat] = {
    "cbor": IterableFormat(
        decoder_dist="cbor2",
        pip_hint="omniload[iterable]",
        eager_decoder=_cbor_eager_decode,
        normalizer_factory=_cbor_normalizer,
    ),
    "msgpack": IterableFormat(
        decoder_dist="msgpack",
        pip_hint="omniload[iterable]",
        class_path="iterable.datatypes.msgpack.MessagePackIterable",
        normalizer_factory=_msgpack_normalizer,
    ),
    "xml": IterableFormat(
        decoder_dist="lxml",
        pip_hint="omniload[iterable]",
        eager_decoder=_xml_eager_decode,
        # XML decodes to str / dict / list / None leaves, all dlt-safe, so no normalizer.
    ),
    "yaml": IterableFormat(
        decoder_dist="yaml",
        pip_hint="omniload[iterable]",
        eager_decoder=_yaml_eager_decode,
        normalizer_factory=_yaml_normalizer,
    ),
}


def installed_iterable_formats() -> tuple[str, ...]:
    """Iterable-backed formats whose decoder is importable, in registry order.

    Used to advertise only the formats a given install can actually read. A streaming format
    additionally needs ``iterabledata`` itself; a whole-file (``eager_decoder``) format needs
    only its decoder. Uses ``importlib.util.find_spec`` so it never imports ``iterable`` or a
    decoder, keeping the CLI import path decoder-free.
    """
    have_iterable = importlib.util.find_spec("iterable") is not None
    available = []
    for name, spec in FORMAT_TO_ITERABLE.items():
        if importlib.util.find_spec(spec.decoder_dist) is None:
            continue
        if spec.class_path is not None and not have_iterable:
            continue
        available.append(name)
    return tuple(available)


def _load_iterable_class(spec: IterableFormat) -> type:
    """Import and return the ``iterable.datatypes.*`` class named by ``spec.class_path``.

    Raises ``MissingDecoderError`` (with the pip hint) if iterabledata can't be imported.
    """
    if spec.class_path is None:
        raise ValueError("IterableFormat has no class_path to load")
    module_path, _, class_name = spec.class_path.rpartition(".")
    try:
        module = importlib.import_module(module_path)
    except ImportError as e:
        raise MissingDecoderError(
            "Reading this format needs the iterabledata package. "
            f"Install it with: pip install '{spec.pip_hint}'"
        ) from e
    return getattr(module, class_name)


def _as_seekable_binary_stream(file_obj: Any) -> Any:
    """Return a seekable binary stream for ``file_obj``.

    iterabledata's ctor rewinds via ``seek(0)`` and raises on a non-seekable handle, so a
    non-seekable stream (pipe, some compressed / SFTP handles) is spooled into ``BytesIO``. A
    seekable handle is passed straight through so the common cloud case stays streaming.
    """
    seekable = getattr(file_obj, "seekable", None)
    if callable(seekable) and seekable():
        return file_obj
    return io.BytesIO(file_obj.read())


def _normalize_row(normalize: Normalizer, row: Any) -> Any:
    """Apply ``normalize`` to every leaf of ``row`` (recursing into dicts/lists/tuples)."""
    # map_nested_values_in_place raises on a non-container; records are normally dicts, but a
    # scalar / top-level value is normalized directly so an odd payload doesn't crash.
    if isinstance(row, (dict, list, tuple)):
        return map_nested_values_in_place(normalize, row)
    return normalize(row)


def _iter_file_records(
    spec: IterableFormat, stream: Any, options: dict[str, Any], bulk_size: int
) -> Iterator[Any]:
    """Yield raw records from one open file.

    Whole-file formats (``eager_decoder``) decode the buffered bytes directly so decode errors
    surface. Streaming formats drive the iterabledata class via ``read_bulk`` until it returns
    ``[]`` (EOF only, for the binary tranche; no mid-file skip path is wired in this PR).
    """
    if spec.eager_decoder is not None:
        yield from spec.eager_decoder(stream.read(), options)
        return
    iterable_class = _load_iterable_class(spec)
    reader = iterable_class(stream=stream, options=options or None)
    while True:
        batch = reader.read_bulk(bulk_size)
        if not batch:
            break
        yield from batch


def read_via_iterable(
    items: Iterator[FileItemDict],
    *,
    file_format: str,
    chunksize: int = 1000,
    **options: Any,
) -> Iterator[TDataItems]:
    """Read ``file_format`` files, yielding row chunks of at most ``chunksize``.

    Mirrors ``readers.read_bson``: opens each file's fsspec handle, produces its records (via
    the format's iterabledata class or its whole-file decoder), normalizes each row, and yields
    chunks, flushing per file so a multi-file glob never drops a partial final chunk.
    ``options`` carries the per-URI reader hints (from the ``#key=value`` fragment) and is
    forwarded to a streaming class's ctor or to the ``eager_decoder`` (e.g. XML's ``tagname``);
    a format that takes no options ignores it.

    Raises:
        MissingDecoderError: if the format's decoder (or iterabledata, for a streaming format)
            is not installed.
    """
    spec = FORMAT_TO_ITERABLE[file_format]
    if importlib.util.find_spec(spec.decoder_dist) is None:
        raise MissingDecoderError(
            f"Reading {file_format} files needs the '{spec.decoder_dist}' package. "
            f"Install it with: pip install '{spec.pip_hint}'"
        )
    normalize = spec.normalizer_factory() if spec.normalizer_factory else None

    for file_obj in items:
        with file_obj.open() as f:
            stream = _as_seekable_binary_stream(f)
            chunk: list = []
            for record in _iter_file_records(spec, stream, options, chunksize):
                chunk.append(
                    _normalize_row(normalize, record)
                    if normalize is not None
                    else record
                )
                if len(chunk) >= chunksize:
                    yield chunk
                    chunk = []
            if chunk:
                yield chunk
