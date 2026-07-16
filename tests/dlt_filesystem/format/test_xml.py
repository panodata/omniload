"""Test the XML filesystem reader on the generic iterabledata harness.

Mock-only unit lane (no Docker, no credentials): XML files are written to ``tmp_path`` and read
back through ``LocalFilesystemSource``. Covers: the mandatory ``#tagname`` reader hint threaded
through all three harness seams, the row-element-to-record convention (``@attr`` / ``#text`` /
repeated-children-as-list / namespaces), and the parser-safety contract -- an XXE / entity-bomb /
external-DTD / bad-encoding input is neutralized, never leaked or expanded.

XML is parsed with a hardened ``lxml`` config directly (iterabledata's XML parser resolves
entities and can't be locked down through its API), so it needs only ``lxml``, which ships in
omniload's ``iterable`` extra.
"""

import importlib.util
import json

import pytest

from dlt_filesystem.source.api import LocalFilesystemSource
from dlt_filesystem.source.error import (
    MissingDecoderError,
    MissingReaderOptionError,
)
from dlt_filesystem.source.format.iterable_codec import (
    FORMAT_TO_ITERABLE,
    read_via_iterable,
)
from dlt_filesystem.source.format.readers import read_xml
from dlt_filesystem.testing.stub import (
    FileItemStub,
    NonSeekableItem,
)
from dlt_filesystem.testing.writer import write_xml

lxml_etree = pytest.importorskip("lxml.etree")


def _read_with_tag(path, tag="item"):
    """Read a local XML file end-to-end through the shared reader with a ``#tagname`` hint."""
    return list(LocalFilesystemSource().dlt_source(f"file://{path}#tagname={tag}", ""))


def _read_direct(path, tag="item"):
    """Drive ``read_xml`` directly (bypassing dlt's error wrapping), flattening its row chunks."""
    chunks = list(read_xml(iter([FileItemStub(path)]), tagname=tag))  # ty: ignore[invalid-argument-type]
    return [row for chunk in chunks for row in chunk]


# --- happy path + row-to-record convention -----------------------------------


def test_reads_rows_by_tagname(tmp_path):
    """Each ``#tagname`` element becomes one row; attributes land under ``@name``."""
    path = write_xml(
        tmp_path / "d.xml",
        '<data><item id="1"><name>alice</name></item>'
        '<item id="2"><name>bob</name></item></data>',
    )
    rows = _read_with_tag(path)
    assert rows == [
        {"@id": "1", "name": "alice"},
        {"@id": "2", "name": "bob"},
    ]


def test_record_shape_attributes_text_repeated_nested(tmp_path):
    """The pinned convention: attributes -> ``@attr``, non-empty leading text -> ``#text``,
    repeated child tags -> a list, nested elements -> a dict, empty element -> ``None``."""
    path = write_xml(
        tmp_path / "shape.xml",
        '<data><item id="5" type="x">lead'
        "<name>foo</name><tag>a</tag><tag>b</tag><empty/>"
        "<nested><deep>1</deep></nested></item></data>",
    )
    (row,) = _read_with_tag(path)
    assert row == {
        "@id": "5",
        "@type": "x",
        "#text": "lead",
        "name": "foo",
        "tag": ["a", "b"],
        "empty": None,
        "nested": {"deep": "1"},
    }


def test_pure_text_rows_become_scalars(tmp_path):
    """A row element with only text (no attributes, no children) yields its text scalar."""
    path = write_xml(
        tmp_path / "text.xml", "<data><item>hello</item><item>world</item></data>"
    )
    assert _read_with_tag(path) == ["hello", "world"]


def test_attribute_and_text_without_children(tmp_path):
    """An element with an attribute and text (no children) keeps text under ``#text``."""
    path = write_xml(
        tmp_path / "at.xml", '<data><item><v unit="kg">5</v></item></data>'
    )
    assert _read_with_tag(path) == [{"v": {"@unit": "kg", "#text": "5"}}]


def test_namespaced_rows_match_and_strip_to_localnames(tmp_path):
    """A namespaced row tag matches ``#tagname=item`` (via the ``{*}`` wildcard) and both element
    and attribute names are stripped to their local names."""
    path = write_xml(
        tmp_path / "ns.xml",
        '<data xmlns:x="http://ex/">'
        '<x:item x:role="lead" id="1"><v>a</v></x:item>'
        '<x:item id="2"><v>b</v></x:item></data>',
    )
    rows = _read_with_tag(path)
    assert rows == [{"@role": "lead", "@id": "1", "v": "a"}, {"@id": "2", "v": "b"}]


def test_repeated_local_names_across_namespaces_collapse_to_list(tmp_path):
    """Two children with the same local name (here across namespaces) collapse into one list,
    the documented key-collision behaviour."""
    path = write_xml(
        tmp_path / "collide.xml",
        '<data xmlns:a="http://a/" xmlns:b="http://b/">'
        "<item><a:v>1</a:v><b:v>2</b:v></item></data>",
    )
    assert _read_with_tag(path) == [{"v": ["1", "2"]}]


def test_nested_same_tagname_yields_outermost_row_with_inner_nested(tmp_path):
    """When the row tag nests inside another row tag, only the outermost element is a row and the
    inner one is a nested sub-record; the outer row keeps all its own children. This guards the
    memory-cleanup from deleting an ancestor row's children (which would drop data)."""
    path = write_xml(
        tmp_path / "nested.xml",
        '<data><item id="outer"><name>parent</name>'
        '<item id="inner"><name>child</name></item></item>'
        '<item id="sibling"><name>sib</name></item></data>',
    )
    assert _read_with_tag(path) == [
        {"@id": "outer", "name": "parent", "item": {"@id": "inner", "name": "child"}},
        {"@id": "sibling", "name": "sib"},
    ]


def test_unknown_tagname_yields_no_rows(tmp_path):
    """A tagname that matches nothing is not an error; it simply loads zero rows."""
    path = write_xml(tmp_path / "d.xml", "<data><item><v>a</v></item></data>")
    assert _read_with_tag(path, tag="row") == []


def test_empty_file_yields_no_rows(tmp_path):
    """An empty (or whitespace-only) file loads as zero rows rather than raising on 'no root'."""
    path = write_xml(tmp_path / "empty.xml", "   \n")
    assert _read_direct(path) == []


def test_extension_and_format_hint_both_resolve(tmp_path):
    """A `.xml` extension and an explicit `#xml&tagname=item` hint both route to the reader."""
    body = '<data><item id="1"/><item id="2"/></data>'
    assert len(_read_with_tag(write_xml(tmp_path / "by_ext.xml", body))) == 2
    hint_path = write_xml(tmp_path / "feed.dat", body)
    rows = list(
        LocalFilesystemSource().dlt_source(f"file://{hint_path}#xml&tagname=item", "")
    )
    assert len(rows) == 2


def test_reads_multiple_files_flushes_each_remainder(tmp_path):
    """Multi-file glob: every row across files loads (each file's remainder is flushed)."""
    write_xml(tmp_path / "a.xml", '<d><item id="1"/><item id="2"/><item id="3"/></d>')
    write_xml(tmp_path / "b.xml", '<d><item id="4"/><item id="5"/></d>')
    rows = list(
        LocalFilesystemSource().dlt_source(f"file://{tmp_path}/*.xml#tagname=item", "")
    )
    assert sorted(r["@id"] for r in rows) == ["1", "2", "3", "4", "5"]


# --- tagname hint threads through all three seams -----------------------------


def test_tagname_hint_threads_end_to_end(tmp_path):
    """The row is only shaped correctly if ``tagname=item`` traversed resource_for_reader ->
    the read_xml wrapper -> read_via_iterable -> _xml_eager_decode. A wrong/missing tagname
    would give zero rows, so a correct load is proof the option crossed all three seams."""
    path = write_xml(tmp_path / "d.xml", "<data><item><n>1</n></item></data>")
    assert _read_with_tag(path) == [{"n": "1"}]


def test_missing_tagname_raises_reader_option_error(tmp_path):
    """No ``#tagname`` -> a clear MissingReaderOptionError (never a bare AttributeError from the
    parser, and not the connection-shaped MissingValueError)."""
    path = write_xml(tmp_path / "d.xml", "<data><item><v>a</v></item></data>")
    with pytest.raises(MissingReaderOptionError) as exc:
        list(read_xml(iter([FileItemStub(path)]), chunksize=10))  # ty: ignore[invalid-argument-type]
    message = str(exc.value)
    assert "tagname" in message
    assert "#tagname=" in message


def test_irrelevant_hint_is_dropped_not_forwarded(tmp_path):
    """A hint the reader doesn't declare (here ``sheet``) is filtered out, so it can't reach the
    decoder; without a ``tagname`` the mandatory-option error still fires."""
    path = write_xml(tmp_path / "d.xml", "<data><item><v>a</v></item></data>")
    with pytest.raises(Exception) as exc:
        list(LocalFilesystemSource().dlt_source(f"file://{path}#sheet=foo", ""))
    assert "tagname" in str(exc.value)


# --- parser-safety attack matrix (driven directly for precise behaviour) ------


def test_internal_entity_is_not_expanded(tmp_path):
    """An internal entity stays unexpanded (resolve_entities=False), so it never reaches a row;
    the element reads as empty rather than leaking the entity's replacement text."""
    path = write_xml(
        tmp_path / "ent.xml",
        '<?xml version="1.0"?><!DOCTYPE d [<!ENTITY s "SECRET-EXPANDED">]>'
        "<d><item><v>&s;</v></item></d>",
    )
    rows = _read_direct(path)
    assert rows == [{"v": None}]
    assert "SECRET-EXPANDED" not in json.dumps(rows)


def test_file_external_entity_is_not_read(tmp_path):
    """A ``file://`` external entity is not resolved, so a local secret never leaks into a row
    (no_network alone would NOT stop a local file entity; resolve_entities=False does)."""
    secret = tmp_path / "secret.txt"
    secret.write_text("TOP-SECRET-XYZ")
    # secret.as_uri() gives a valid file:// URI on every platform (file:///C:/... on Windows),
    # so the entity is a resolvable-looking reference that the parser still refuses to resolve.
    path = write_xml(
        tmp_path / "xxe.xml",
        f'<?xml version="1.0"?><!DOCTYPE d [<!ENTITY xxe SYSTEM "{secret.as_uri()}">]>'
        "<d><item><v>&xxe;</v></item></d>",
    )
    rows = _read_direct(path)
    assert rows == [{"v": None}]
    assert "TOP-SECRET-XYZ" not in json.dumps(rows)


def test_network_external_entity_is_not_fetched(tmp_path):
    """A network external entity is not fetched (no_network=True) and stays unexpanded."""
    path = write_xml(
        tmp_path / "ssrf.xml",
        '<?xml version="1.0"?>'
        '<!DOCTYPE d [<!ENTITY xxe SYSTEM "http://169.254.169.254/latest/meta-data/">]>'
        "<d><item><v>&xxe;</v></item></d>",
    )
    assert _read_direct(path) == [{"v": None}]


def test_external_dtd_is_not_loaded(tmp_path):
    """An external DTD reference is ignored (load_dtd=False, no_network=True); the document
    still parses without fetching it."""
    path = write_xml(
        tmp_path / "dtd.xml",
        '<?xml version="1.0"?><!DOCTYPE data SYSTEM "http://example.invalid/evil.dtd">'
        "<data><item><v>hi</v></item></data>",
    )
    assert _read_direct(path) == [{"v": "hi"}]


def test_parameter_entity_xxe_is_neutralized(tmp_path):
    """The parameter-entity OOB-XXE construct (a `%pe;` that defines a file-external general
    entity) never reads the file: the general entity is not resolved into the row."""
    secret = tmp_path / "secret.txt"
    secret.write_text("TOP-SECRET-PE")
    path = write_xml(
        tmp_path / "pe.xml",
        '<?xml version="1.0"?><!DOCTYPE r [\n'
        f"  <!ENTITY % pe \"<!ENTITY xxe SYSTEM '{secret.as_uri()}'>\">\n"
        "  %pe;\n]>\n<r><item><v>&xxe;</v></item></r>",
    )
    try:
        rows = _read_direct(path)
    except lxml_etree.XMLSyntaxError:
        return  # rejected outright -- safe
    assert "TOP-SECRET-PE" not in json.dumps(rows)


def test_internal_entity_in_attribute_expands_to_document_local_text(tmp_path):
    """An internal entity in an *attribute* value expands to its replacement text: lxml
    normalizes attribute values, so an entity reference there cannot stay unexpanded the way it
    can in element text. This is benign -- the value is defined inline in the same document, not
    fetched from anywhere -- so it is not an exfiltration vector, only worth pinning as behaviour."""
    path = write_xml(
        tmp_path / "attrent.xml",
        '<?xml version="1.0"?><!DOCTYPE r [<!ENTITY s "local-text">]>'
        '<r><item id="&s;"><v>ok</v></item></r>',
    )
    assert _read_direct(path) == [{"@id": "local-text", "v": "ok"}]


def test_external_entity_in_attribute_does_not_read_file(tmp_path):
    """An *external* entity in an attribute value cannot resolve (resolve_entities=False, no DTD
    load, no network), so it raises rather than reading a local file into the attribute -- the
    attribute-value counterpart of the element-text XXE guard."""
    secret = tmp_path / "secret.txt"
    secret.write_text("TOP-SECRET-ATTR")
    path = write_xml(
        tmp_path / "xxeattr.xml",
        f'<?xml version="1.0"?><!DOCTYPE r [<!ENTITY xxe SYSTEM "{secret.as_uri()}">]>'
        '<r><item id="&xxe;"><v>ok</v></item></r>',
    )
    with pytest.raises(lxml_etree.XMLSyntaxError):
        _read_direct(path)


def test_billion_laughs_is_bounded(tmp_path):
    """An entity-expansion bomb never expands: with resolve_entities=False the nested entities
    are left as references, so the load is bounded whether libxml2 rejects the recursive
    definition outright (raises) or parses it without expanding (no huge blob reaches a row).
    Either outcome is safe; the guarantee is that gigabytes are never materialized."""
    path = write_xml(
        tmp_path / "boom.xml",
        '<?xml version="1.0"?><!DOCTYPE lolz [\n'
        ' <!ENTITY lol "lol">\n'
        ' <!ENTITY lol2 "&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;">\n'
        ' <!ENTITY lol3 "&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;">\n'
        ' <!ENTITY lol4 "&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;">\n'
        ' <!ENTITY lol5 "&lol4;&lol4;&lol4;&lol4;&lol4;&lol4;&lol4;&lol4;&lol4;&lol4;">\n'
        ' <!ENTITY lol6 "&lol5;&lol5;&lol5;&lol5;&lol5;&lol5;&lol5;&lol5;&lol5;&lol5;">\n'
        ' <!ENTITY lol7 "&lol6;&lol6;&lol6;&lol6;&lol6;&lol6;&lol6;&lol6;&lol6;&lol6;">\n'
        ' <!ENTITY lol8 "&lol7;&lol7;&lol7;&lol7;&lol7;&lol7;&lol7;&lol7;&lol7;&lol7;">\n'
        ' <!ENTITY lol9 "&lol8;&lol8;&lol8;&lol8;&lol8;&lol8;&lol8;&lol8;&lol8;&lol8;">\n'
        "]>\n<lolz><item>&lol9;</item></lolz>",
    )
    try:
        rows = _read_direct(path)
    except lxml_etree.XMLSyntaxError:
        return  # rejected outright -- safe
    # If it parsed at all, the entities were not expanded, so no "lollol..." blob leaked.
    assert "lollol" not in json.dumps(rows)


def test_deeply_nested_is_bounded(tmp_path):
    """A pathologically deep tree is capped (huge_tree=False) and raises rather than exhausting
    memory / the C stack."""
    depth = 50000
    body = "<data><item>" + "<a>" * depth + "x" + "</a>" * depth + "</item></data>"
    path = write_xml(tmp_path / "deep.xml", body)
    with pytest.raises(lxml_etree.XMLSyntaxError):
        _read_direct(path)


def test_large_text_node_parses(tmp_path):
    """A large (but well-formed) text node is not an attack and loads intact."""
    path = write_xml(
        tmp_path / "big.xml",
        "<data><item><v>" + "A" * 2_000_000 + "</v></item></data>",
    )
    (row,) = _read_direct(path)
    assert len(row["v"]) == 2_000_000


def test_mismatched_encoding_declaration_raises(tmp_path):
    """A document declaring UTF-16 over UTF-8 bytes is rejected with a clear parse error rather
    than mis-decoding or crashing."""
    path = write_xml(
        tmp_path / "enc.xml",
        '<?xml version="1.0" encoding="UTF-16"?><data><item><v>hi</v></item></data>',
    )
    with pytest.raises(lxml_etree.XMLSyntaxError):
        _read_direct(path)


def test_malformed_xml_raises(tmp_path):
    """A malformed document raises (recover=False), not a silent partial load."""
    path = write_xml(tmp_path / "bad.xml", "<data><item><v>oops</vvv></item>")
    with pytest.raises(lxml_etree.XMLSyntaxError):
        _read_direct(path)


# --- chunking contract + non-seekable spool ----------------------------------


def test_chunks_at_boundary_and_flushes_partial(tmp_path):
    """Rows are yielded in chunksize-sized chunks, with the partial final chunk flushed."""
    body = "<data>" + "".join(f"<item><n>{i}</n></item>" for i in range(5)) + "</data>"
    path = write_xml(tmp_path / "five.xml", body)
    chunks = list(read_xml(iter([FileItemStub(path)]), chunksize=2, tagname="item"))  # ty: ignore[invalid-argument-type]
    assert [len(chunk) for chunk in chunks] == [2, 2, 1]
    assert [row["n"] for chunk in chunks for row in chunk] == ["0", "1", "2", "3", "4"]


def test_non_seekable_stream_is_spooled(tmp_path):
    """A non-seekable handle still loads (the harness reads the whole stream before decoding)."""
    data = b"<data><item><n>0</n></item><item><n>1</n></item></data>"
    chunks = list(
        read_via_iterable(
            iter([NonSeekableItem(data)]),  # ty: ignore[invalid-argument-type]
            file_format="xml",
            chunksize=1000,
            tagname="item",
        )
    )
    assert [row["n"] for chunk in chunks for row in chunk] == ["0", "1"]


# --- registry / import-path / error UX ---------------------------------------


def test_xml_uses_a_direct_decoder_not_iterabledata():
    """XML is parsed with lxml directly (not iterabledata's entity-resolving parser), so the
    registry entry carries an eager_decoder and no class_path, and needs no normalizer."""
    spec = FORMAT_TO_ITERABLE["xml"]
    assert spec.eager_decoder is not None
    assert spec.class_path is None
    assert spec.decoder_dist == "lxml"
    assert spec.normalizer_factory is None


def test_missing_decoder_raises_typed_install_hint(tmp_path, monkeypatch):
    """With lxml unavailable, the reader raises a typed error naming the exact `pip install`,
    not a bare ImportError."""
    real_find_spec = importlib.util.find_spec

    def fake_find_spec(name, *args, **kwargs):
        """Report the lxml decoder as absent, delegating other lookups to the real finder."""
        if name == "lxml":
            return None
        return real_find_spec(name, *args, **kwargs)

    monkeypatch.setattr(importlib.util, "find_spec", fake_find_spec)

    path = write_xml(tmp_path / "x.xml", "<data><item/></data>")
    with pytest.raises(MissingDecoderError) as exc:
        list(read_xml(iter([FileItemStub(path)]), chunksize=10, tagname="item"))  # ty: ignore[invalid-argument-type]
    message = str(exc.value)
    assert "lxml" in message
    assert "omniload[iterable]" in message


def test_xml_advertised_only_when_decoder_installed(monkeypatch):
    """A base install must not advertise xml as supported when lxml is absent, even though the
    format stays routable."""
    from dlt_filesystem.source.format.registry import advertised_file_formats

    assert "xml" in advertised_file_formats()

    real_find_spec = importlib.util.find_spec

    def fake_find_spec(name, *args, **kwargs):
        """Report the lxml decoder as absent, delegating other lookups to the real finder."""
        if name == "lxml":
            return None
        return real_find_spec(name, *args, **kwargs)

    monkeypatch.setattr(importlib.util, "find_spec", fake_find_spec)
    assert "xml" not in advertised_file_formats()
