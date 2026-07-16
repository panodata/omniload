(bson)=

# BSON

`omniload` reads [BSON](https://bsonspec.org/) files, the binary format `mongodump`
writes (one file per collection, documents concatenated back to back). BSON is a **read
format**: it is decoded through the shared filesystem readers,
so any source that reads files can read BSON.

BSON is currently supported for read operations only.

## Where it works

BSON is available on every source that goes through the shared file readers:

- Local files: [`file://`](file.md)
- [`s3://`](s3.md), [`gs://`](google-cloud-storage.md) (blob storage)
- [`sftp://`](sftp.md)

A file is read as BSON when its extension is `.bson` (optionally `.bson.gz`) or when an
explicit `#bson` {ref}`format hint <format-hint>` is appended. Gzipped files are
decompressed automatically.

## Example: loading a BSON dump into DuckDB

```sh
omniload ingest \
    --source-uri 'file://dump/users.bson' \
    --source-table 'users' \
    --dest-uri duckdb:///local.duckdb \
    --dest-table 'public.users'
```

The same file read from S3, with a non-standard extension pinned via `#bson`:

```sh
omniload ingest \
    --source-uri 's3://' \
    --source-table 'my_bucket/dump/users.dat#bson' \
    --dest-uri duckdb:///local.duckdb \
    --dest-table 'public.users'
```

## Extended-type handling

BSON carries types JSON does not. They are converted to portable Python values before
`omniload` hands the data to the loader, so they survive every destination (including the
Parquet loader used for warehouses):

| BSON type | Loaded as |
| :--- | :--- |
| `ObjectId` | string (24-char hex) |
| `Decimal128` | string |
| `Binary` | base64-encoded string |
| `datetime` | UTC datetime |
| `Timestamp` | UTC datetime |
| `Regex` | pattern string |
| `DBRef` | `{"$ref": collection, "$id": id, "$db": database}` object (`$db` only when set) |
| `MinKey` / `MaxKey` | `{"$minKey": 1}` / `{"$maxKey": 1}` |
| `Code` | code string (or `{"$code": source, "$scope": document}` when it carries scope) |

`Binary` is base64-encoded (rather than passed through as raw bytes) so the value is
portable across text-based loaders as well as Parquet. Nested documents and arrays are
converted recursively.
