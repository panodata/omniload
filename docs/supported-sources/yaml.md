(yaml)=

# YAML

`omniload` reads [YAML](https://yaml.org/) files. Like BSON, MessagePack, CBOR and XML it is a
**read format**: it is decoded through the same filesystem readers as CSV, JSONL and Parquet, so
any source that reads files can read YAML.

There is no YAML *destination*; `file://` writes `csv`, `jsonl` and `parquet` only.

## Installation

YAML support ships in the optional `iterable` extra, so it is not part of the base install:

```sh
pip install 'omniload[iterable]'
```

If a `.yaml` / `.yml` file is loaded without the extra installed, `omniload` fails with a clear
error naming the exact `pip install` to run, rather than a bare `ImportError`.

YAML is parsed with `yaml.safe_load_all` directly, not through the `iterabledata` bridge, so a
malformed file raises instead of silently loading zero rows and an unsafe tag is rejected rather
than executed; see [File-format routing](../getting-started/file-format-routing.md) for how
omniload chooses a reader per format.

## File shape: documents become rows

A YAML file is a stream of one or more `---`-separated **documents**, and each document becomes
rows:

- a document that is a **list** (sequence) expands to **one row per element**, so a plain list of
  records loads naturally;
- any other document (a mapping, a scalar) yields **one row**;
- a `---`-only or empty document parses to null and is **skipped** (it carries no record).

So both of these load three rows:

```yaml
# one document that is a list
- {id: 1}
- {id: 2}
- {id: 3}
```

```yaml
# three documents
id: 1
---
id: 2
---
id: 3
```

An empty file loads zero rows; a malformed document raises rather than loading partial data.

## Where it works

YAML is available on every source that goes through the shared file readers:

- Local files: [`file://`](file.md)
- [`s3://`](s3.md), [`gs://`](google-cloud-storage.md), [Azure blob storage](azure-blob-storage.md)
- [`sftp://`](sftp.md)

Remote reads go through the source's own fsspec handle, so they reuse its existing
authentication (no separate YAML storage configuration). A file is read as YAML when its
extension is `.yaml` or `.yml` (optionally `.gz`) or when an explicit `#yaml`
{ref}`format hint <format-hint>` is appended. Gzipped files are decompressed
automatically. The whole file is read into memory and parsed at once (YAML is not a streaming
format).

## Example: loading a YAML file into DuckDB

```sh
omniload ingest \
    --source-uri 'file://config/records.yaml' \
    --source-table 'records' \
    --dest-uri duckdb:///local.duckdb \
    --dest-table 'public.records'
```

## Safety and extended types

Parsing uses `yaml.safe_load_all`, the safe loader: a tag that would construct an arbitrary
Python object (`!!python/object/...`, `!!python/name:...`) is **rejected** with an error, never
executed, so an untrusted YAML file cannot run code. Anchors and aliases (`&anchor` / `*alias`)
resolve normally.

`safe_load` maps a few YAML tags to Python types a JSON or Parquet loader cannot serialize;
`omniload` converts those to portable values before handing the data to the loader:

| YAML type | Loaded as |
| :--- | :--- |
| `!!binary` (`bytes`) | base64-encoded string |
| `!!set` | list |
| `!!timestamp` (datetime / date) | datetime / date |

`bytes` is base64-encoded (rather than passed through as raw bytes) so the value is portable
across text-based loaders as well as Parquet. Strings, numbers, booleans, null and nested
mappings / sequences load directly. Timestamps are already dlt-safe and pass through; a `!!set`
becomes a list (its order is not significant).

:::{note}
**Known limitation: nested YAML is flattened.** The filesystem readers run with
`max_table_nesting=0`, so a deeply nested mapping does not become a set of related child tables;
nested objects and lists are stored as JSON in a single column (or flattened by the destination's
own rules). For flat, one-level records this is exactly what you want; for deeply nested YAML,
expect the nested structure to land as JSON rather than normalized tables. Making the nesting
depth tunable per reader is a planned follow-up.
:::
