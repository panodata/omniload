---
outline: deep
---

# File-format routing

omniload reads each file format through the **best available path** rather than one generic
reader. This page explains how that routing works and why, so the per-format pages
(MessagePack, CBOR, BSON, ...) can stay focused on how to *use* each format. It is background
reading: you do not need any of it to load a file, but it explains the behaviour you will see
around extended types, corrupt files, and which optional package a format needs.

## Which path reads which format

| Format | Read path | Why |
| :--- | :--- | :--- |
| CSV, JSONL, Parquet | Native Polars / pyarrow readers | Fast, columnar, already well tested. |
| BSON | Dedicated in-tree codec | Needs extended-type normalization the generic decoders do not do. |
| MessagePack | Streaming [`iterabledata`](https://github.com/datenoio/iterabledata) bridge | No better native path; streamed record-by-record. |
| CBOR | Whole-file decode with `cbor2` directly | Whole-file format; a direct decode surfaces corruption instead of hiding it. |

The routing policy is deliberately conservative: **fill gaps, don't replace working paths.** A
format is only routed to the generic bridge when there is no faster, better-tested native
reader for it. New long-tail formats are added incrementally behind the optional `iterable`
extra, without touching the CSV / JSONL / Parquet paths.

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
decodes the bytes with the format's own library directly (CBOR via `cbor2`) so decode errors
propagate instead of vanishing. This is why CBOR needs only the `cbor2` package and not
`iterabledata` itself, even though both ship together in the `iterable` extra.

## Extended-type normalization

Binary formats carry types JSON does not (raw `bytes`, timestamps, tagged values), and the
decoders hand some of those back as native Python objects that a text or Parquet loader cannot
serialize. omniload normalizes rows to portable values before handing data to the loader:

- `bytes` becomes a base64-encoded string (portable across text loaders and Parquet alike).
- A MessagePack `Timestamp` extension becomes a UTC datetime.
- An unknown CBOR tag becomes a plain `{"tag": ..., "value": ...}` object rather than crashing
  the load.

Some values are made portable by the decoder itself rather than by omniload: `cbor2` decodes
the standard CBOR tags (datetime, big integers, decimals) into native Python types directly.
Those load into Parquet and SQL destinations, but a native decimal cannot be serialized to a
JSONL *file* destination, so use a Parquet or SQL destination for data that carries decimals.
Nested maps and arrays are handled recursively. The exact per-format mapping is on each
format's own page under "Extended-type handling".

## Integrity and truncation

The read mechanism determines how a damaged file behaves, and it is worth knowing which
guarantee you get:

- **Whole-file decode (CBOR)** raises on a corrupt or truncated file rather than loading
  partial data. A CBOR source must also be a *single* top-level value; files that concatenate
  several top-level objects are read only up to the first, a decoder limitation that cannot be
  detected at read time.
- **Streaming formats (MessagePack)** carry no length prefix, so a truncated tail reads as a
  clean end-of-file: the partial trailing record, and anything after a mid-stream corruption,
  are dropped silently. Validate file integrity upstream if partial loads would be a problem.

## The `iterable` extra

Long-tail format support ships in the optional `iterable` extra (also part of `full`):

```sh
pip install 'omniload[iterable]'
```

It installs `iterabledata` plus the shipped tranche's decoders (`msgpack`, `cbor2`). If a file
in one of these formats is loaded without the extra installed, omniload fails with a clear
error naming the exact `pip install` to run, rather than a bare `ImportError`.
