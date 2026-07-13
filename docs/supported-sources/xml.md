(xml)=

# XML

`omniload` reads [XML](https://www.w3.org/XML/) files. Like BSON, MessagePack and CBOR it is a
**read format**: it is decoded through the same filesystem readers as CSV, JSONL and Parquet, so
any source that reads files can read XML.

There is no XML *destination*; `file://` writes `csv`, `jsonl` and `parquet` only.

## Installation

XML support ships in the optional `iterable` extra, so it is not part of the base install:

```sh
pip install 'omniload[iterable]'
```

If a `.xml` file is loaded without the extra installed, `omniload` fails with a clear error
naming the exact `pip install` to run, rather than a bare `ImportError`.

XML is parsed with a hardened `lxml` configuration directly, not through the `iterabledata`
bridge, so it can be locked down against XXE / entity-expansion attacks and so a corrupt file
raises instead of loading partial data; see
[File-format routing](../getting-started/file-format-routing.md) for how omniload chooses a
reader per format.

## The `#tagname` hint is required

XML has no single natural "row" like a JSON array does, so you must tell omniload which repeated
element is one row. Append a `#tagname=<row-tag>` {ref}`reader hint <reader-hint>` to the
source:

```sh
omniload ingest \
    --source-uri 'file://catalog/products.xml#tagname=product' \
    --source-table 'products' \
    --dest-uri duckdb:///local.duckdb \
    --dest-table 'public.products'
```

Every element whose (local) name matches `tagname` becomes one row; everything outside those
elements (the document root, wrapping containers) is ignored. A namespaced row tag matches on its
local name, so `#tagname=product` matches both `<product>` and `<ns:product>`. Without a
`#tagname` hint the load fails with a clear error asking for one.

## Where it works

XML is available on every source that goes through the shared file readers:

- Local files: [`file://`](file.md)
- [`s3://`](s3.md), [`gs://`](google-cloud-storage.md), [Azure blob storage](azure-blob-storage.md)
- [`sftp://`](sftp.md)

Remote reads go through the source's own fsspec handle, so they reuse its existing
authentication (no separate XML storage configuration). A file is read as XML when its extension
is `.xml` (optionally `.xml.gz`) or when an explicit `#xml`
{ref}`format hint <format-hint>` is appended (the `#tagname` hint is still required, e.g.
`#xml&tagname=product`). Gzipped files are decompressed automatically. The whole file is read
into memory and parsed at once; a corrupt or malformed file raises rather than loading partial
data. Character encoding follows the document's `<?xml encoding="..."?>` declaration (lxml's
detection); a declaration that disagrees with the actual bytes is rejected with a parse error.

## Row shape: elements, attributes and text

Each matched row element is turned into a record:

| XML | Record |
| :--- | :--- |
| child element `<name>foo</name>` | `"name": "foo"` |
| repeated child elements `<tag>a</tag><tag>b</tag>` | `"tag": ["a", "b"]` (a list) |
| attribute `id="5"` | `"@id": "5"` |
| element text mixed with attributes/children | `"#text": "..."` |
| empty element `<empty/>` | `"empty": null` |
| nested element | a nested object |
| element with only text (no attributes/children) | the text string directly |

Attribute keys are prefixed with `@` and mixed text with `#text`, so neither can collide with a
child element's key. Namespaces are stripped to their local names (`<ns:name>` becomes `name`);
two children that share a local name across different namespaces collapse into one list. All
values are strings (or nested objects/lists), matching XML's text-only data model; cast them at
the destination if you need typed columns.

## Parser safety

The parser is configured to neutralize the common XML attack classes without any per-file
tuning:

- **External entities (XXE)** are not resolved, so an `&xxe;` reference to `file:///etc/passwd`
  or a URL never reads a local file or makes a network request: in element text the reference is
  dropped from the row, and in an attribute value it raises a parse error rather than resolving.
  (An *internal* entity defined inline in the document does expand inside an attribute value,
  because XML normalizes attributes to strings; that is document-local text, not fetched data.)
- **Entity-expansion bombs** ("billion laughs") cannot expand, because entities are never
  resolved; a pathological document is rejected or read without expansion, never materialized.
- **External DTDs** are not loaded and nothing is fetched over the network.
- **Oversized nodes** are capped: a single text node larger than lxml's built-in limit
  (about 10 MB) is rejected rather than parsed, a denial-of-service guard (`huge_tree=False`).
  Tabular XML stays well under this; a document with a single >10 MB value is not supported.
- A **malformed** document raises rather than being silently half-recovered.

:::{note}
**Known limitation: nested XML is flattened.** The filesystem readers run with
`max_table_nesting=0`, so a deeply nested element does not become a set of related child tables;
nested objects and lists are stored as JSON in a single column (or flattened by the destination's
own rules). For tabular XML with one level of fields per row this is exactly what you want; for
deeply hierarchical XML, expect the nested structure to land as JSON rather than normalized
tables. Making the nesting depth tunable per reader is a planned follow-up.
:::
