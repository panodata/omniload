---
outline: deep
---

# File-format routing

omniload reads each file format through the **best available path** rather than one generic
reader. This page explains how that routing works and why, so the per-format pages
(BSON, CBOR, MessagePack, XML, YAML) can stay focused on how to *use* each format. It is
background reading: you do not need any of it to load a file, but it explains the behaviour you
will see around extended types, corrupt files, and which optional package a format needs.

## Which path reads which format

| Format | Read path | Why |
| :--- | :--- | :--- |
| CSV, JSONL, Parquet | Native Polars / pyarrow readers | Fast, columnar, already well tested. |
| BSON | Dedicated in-tree codec | Needs extended-type normalization the generic decoders do not do. |
| CBOR | Whole-file decode with `cbor2` directly | Whole-file format; a direct decode surfaces corruption instead of hiding it. |
| MessagePack | Streaming [`iterabledata`](https://github.com/datenoio/iterabledata) bridge | No better native path; streamed record-by-record. |
| XML | Whole-file parse with a hardened `lxml` directly | iterabledata's XML parser resolves entities (an XXE risk) and can't be locked down through its API; a direct, safe parse is a security requirement, not just a speed choice. |
| YAML | Whole-file decode with `yaml.safe_load_all` directly | iterabledata's YAML wrapper is eager and swallows parse errors; a direct safe-load surfaces corruption and rejects code-execution tags. |

The routing policy is deliberately conservative: **fill gaps, don't replace working paths.** A
format goes through `iterabledata` only where that is genuinely the better path; where its
handling would be unsafe or would hide errors (XML entity resolution, YAML's error-swallowing
wrapper), omniload owns the decode instead and rides only the shared harness (the reader
plumbing, chunking, missing-decoder hint, seekable spool). New long-tail formats are added
incrementally behind the optional `iterable` extra, without touching the CSV / JSONL / Parquet
paths.

## Two read mechanisms

Formats that go through the long-tail machinery are read one of two ways, chosen per format.

### Streaming through the iterabledata bridge

MessagePack (and future streaming formats) are read through `iterabledata`, which exposes a
uniform per-format class that yields record dicts from a file object. omniload feeds it the
source's **own already-authenticated `fsspec` handle**, so a file on S3, GCS, Azure or SFTP
keeps flowing through the source's existing credentials. iterabledata's own cloud-storage /
credential layer is never touched, so there is no second authentication surface to configure.

Records are pulled in batches until the reader signals end-of-file, and flushed per file so a
multi-file glob never drops a partial final chunk. iterabledata rewinds the handle on
construction, which fails on a non-seekable stream (a pipe, some compressed or SFTP handles);
such a stream is spooled into memory first, while a seekable handle streams straight through.

### Whole-file decode

Some formats are whole-file rather than streaming, and iterabledata's wrapper for them wraps
its decode in a broad `except` that yields nothing on a bad payload, meaning a truncated or
corrupt file would load as **zero rows with no error** (silent data loss). For those, omniload
decodes the bytes with the format's own library directly (CBOR via `cbor2`, YAML via
`yaml.safe_load_all`, XML via a hardened `lxml` parse) so decode errors propagate instead of
vanishing. This is why these formats need only their own decoder and not `iterabledata` itself,
even though all ship together in the `iterable` extra.

For **XML**, going direct is also a safety requirement, not just an error-surfacing one:
iterabledata's parser resolves entities with no way to turn it off through its API, so an
untrusted file could trigger an XXE read (`&xxe;` pointing at `file:///etc/passwd` or a URL) or
an entity-expansion bomb. omniload's `lxml` parse disables entity resolution, DTD loading and
network access, so those inputs are neutralized: an external entity is simply dropped from the
row, and an expansion bomb never materializes. For **YAML**, omniload uses the *safe* loader, so
a tag that would construct an arbitrary Python object (`!!python/object/...`) is rejected rather
than executed.

### Reader hints

XML has no single natural "row" the way a JSON array does, so its reader needs one piece of
per-file information: which repeated element is a row. That arrives through the URI's
{ref}`reader-hint channel <reader-hints>` as `#tagname=<row-tag>` (e.g.
`file://products.xml#tagname=product`), the first real consumer of that channel. The hint is
threaded from the source URI down to the decoder; a hint the reader doesn't declare is dropped,
and a missing-but-required one (XML without `#tagname`) raises a clear error rather than failing
obscurely.

## Extended-type normalization

Binary formats carry types JSON does not (raw `bytes`, timestamps, tagged values), and the
decoders hand some of those back as native Python objects that a text or Parquet loader cannot
serialize. omniload normalizes rows to portable values before handing data to the loader:

- `bytes` becomes a base64-encoded string (portable across text loaders and Parquet alike). This
  covers a MessagePack / CBOR binary value and a YAML `!!binary`.
- A MessagePack `Timestamp` extension becomes a UTC datetime.
- An unknown CBOR tag becomes a plain `{"tag": ..., "value": ...}` object rather than crashing
  the load.
- A YAML `!!set` becomes a list. XML needs no such normalization: its values are all strings or
  nested objects/lists, which every loader handles.

Some values are made portable by the decoder itself rather than by omniload: `cbor2` decodes
the standard CBOR tags (datetime, big integers, decimals) into native Python types directly.
Those load into Parquet and SQL destinations, but a native decimal cannot be serialized to a
JSONL *file* destination, so use a Parquet or SQL destination for data that carries decimals.
Nested maps and arrays are handled recursively. The exact per-format mapping is on each
format's own page under "Extended-type handling".

## Integrity and truncation

The read mechanism determines how a damaged file behaves, and it is worth knowing which
guarantee you get:

- **Whole-file decode (CBOR, XML, YAML)** raises on a corrupt or malformed file rather than
  loading partial data. CBOR additionally must be a *single* top-level value; files that
  concatenate several top-level objects are read only up to the first, a decoder limitation that
  cannot be detected at read time. XML additionally rejects an entity-expansion bomb and a
  mismatched encoding declaration.
- **Streaming formats (MessagePack)** carry no length prefix, so a truncated tail reads as a
  clean end-of-file: the partial trailing record, and anything after a mid-stream corruption,
  are dropped silently. Validate file integrity upstream if partial loads would be a problem.

## The `iterable` extra

Long-tail format support ships in the optional `iterable` extra (also part of `full`):

```sh
pip install 'omniload[iterable]'
```

It installs `iterabledata` plus the shipped tranche's decoders (`cbor2`, `lxml`, `msgpack`,
`pyyaml`). If a file in one of these formats is loaded without the extra installed, omniload
fails with a clear error naming the exact `pip install` to run, rather than a bare `ImportError`.
