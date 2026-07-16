(messagepack)=
(msgpack)=

# MessagePack

`omniload` reads [MessagePack](https://msgpack.org/) files, a compact binary
serialization of JSON-shaped records. Like BSON it is a **read format**: it is decoded
through the shared filesystem readers, so any source that reads
files can read MessagePack.

MessagePack is currently supported for read operations only.

## Installation

MessagePack support ships in the optional `iterable` extra (it is backed by the
[`iterabledata`](https://github.com/datenoio/iterabledata) library plus the `msgpack`
decoder), so it is not part of the base install:

```sh
pip install 'omniload[iterable]'
```

If a `.msgpack` file is loaded without the extra installed, `omniload` fails with a clear
error naming the exact `pip install` to run, rather than a bare `ImportError`.

MessagePack is read through the generic `iterabledata` bridge because it has no faster native
reader; see {ref}`file-format-routing` about how omniload chooses a reader per format.

## Where it works

MessagePack is available on every source that goes through the shared file readers:

- Local files: {ref}`file`
- Remote files: {ref}`s3`, {ref}`gcs`, {ref}`azure-storage`, {ref}`sftp`, ...

Remote reads go through the source's own fsspec handle, so they reuse its existing
authentication (no separate MessagePack storage configuration). A file is read as
MessagePack when its extension is `.msgpack` (optionally `.msgpack.gz`) or when an explicit
{ref}`format hint <format-hint>` is appended. Gzipped files are
decompressed automatically.

The file is expected to be a stream of MessagePack records (maps) written back to back,
the way `msgpack.Packer` emits them. Map keys should be strings; a non-string, non-`bytes`
key (for example an integer) is rejected by the decoder, while a `bytes` key is accepted and
becomes a `bytes` column name.

MessagePack records carry no length prefix, so a truncated file cannot be detected: the
decoder stops cleanly at the point of truncation and the trailing (partial) record and
anything after a mid-stream corruption are dropped silently. Validate file integrity upstream
if partial loads would be a problem.

## Example: loading a MessagePack file into DuckDB

```sh
omniload ingest \
    --source-uri 'file://events/day.msgpack' \
    --source-table 'events' \
    --dest-uri duckdb:///local.duckdb \
    --dest-table 'public.events'
```

The same file read from S3, with a non-standard extension pinned via `#msgpack`:

```sh
omniload ingest \
    --source-uri 's3://' \
    --source-table 'my_bucket/events/day.dat#msgpack' \
    --dest-uri duckdb:///local.duckdb \
    --dest-table 'public.events'
```

## Extended-type handling

MessagePack carries a few types JSON does not. They are converted to portable Python
values before `omniload` hands the data to the loader, so they survive every destination
(including the Parquet loader used for warehouses):

| MessagePack type | Loaded as |
| :--- | :--- |
| binary (`bytes`) | base64-encoded string |
| Timestamp extension | UTC datetime |

`bytes` is base64-encoded (rather than passed through as raw bytes) so the value is
portable across text-based loaders as well as Parquet. Nested maps and arrays are
converted recursively. String, integer, float, boolean, null and nested structures load
directly.
