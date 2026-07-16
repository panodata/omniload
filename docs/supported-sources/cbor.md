(cbor)=

# CBOR

`omniload` reads [CBOR](https://cbor.io/) (Concise Binary Object Representation, RFC 8949)
files. Like BSON and MessagePack it is a **read format**: it is decoded through the shared
filesystem readers, so any source that reads files can read CBOR.

CBOR is currently supported for read operations only.

## Installation

CBOR support ships in the optional `iterable` extra, so it is not part of the base install:

```sh
pip install 'omniload[iterable]'
```

If a `.cbor` file is loaded without the extra installed, `omniload` fails with a clear error
naming the exact `pip install` to run, rather than a bare `ImportError`.

CBOR is decoded directly with `cbor2` rather than through the `iterabledata` bridge, so a
corrupt file raises instead of silently loading zero rows; see
{ref}`file-format-routing` about how omniload chooses a reader per format.

## File shape: a single top-level value

A CBOR source must be a **single top-level CBOR value**:

- a top-level **array** yields one row per element, and
- a single top-level **map** yields one row.

Files that concatenate several top-level CBOR objects back to back are read only up to the
**first** object. This is a limitation of the CBOR decoder that cannot be detected at read
time, so if you control the writer, emit a single top-level array of records.

## Where it works

CBOR is available on every source that goes through the shared file readers:

- Local files: [`file://`](file.md)
- [`s3://`](s3.md), [`gs://`](google-cloud-storage.md), [Azure blob storage](azure-blob-storage.md)
- [`sftp://`](sftp.md)

Remote reads go through the source's own fsspec handle, so they reuse its existing
authentication (no separate CBOR storage configuration). A file is read as CBOR when its
extension is `.cbor` (optionally `.cbor.gz`) or when an explicit `#cbor`
{ref}`format hint <format-hint>` is appended. Gzipped files are decompressed
automatically. The whole file is read into memory and decoded at once (CBOR is not a
streaming format); a corrupt or truncated file raises rather than loading partial data. Map
keys are expected to be strings.

## Example: loading a CBOR file into DuckDB

```sh
omniload ingest \
    --source-uri 'file://events/day.cbor' \
    --source-table 'events' \
    --dest-uri duckdb:///local.duckdb \
    --dest-table 'public.events'
```

## Extended-type handling

CBOR carries a few types JSON does not. The decoder maps the standard tags to native Python
types (datetime, big integers, decimals), and `omniload` converts the rest to portable
values before handing the data to the loader, so they survive every destination (including
the Parquet loader used for warehouses):

| CBOR type | Loaded as |
| :--- | :--- |
| binary (`bytes`) | base64-encoded string |
| datetime (tag 0 / 1) | UTC datetime |
| decimal (tag 4) / bignum (tag 2 / 3) | decimal / integer |
| other tagged value | `{"tag": number, "value": value}` object |

`bytes` is base64-encoded (rather than passed through as raw bytes) so the value is portable
across text-based loaders as well as Parquet. Nested maps and arrays are converted
recursively. String, integer, float, boolean, null and nested structures load directly.

Decimals load as native decimals, which the Parquet loader (used for warehouses) and SQL
destinations handle, but a JSON-based destination (a `jsonl` file target) cannot serialize a
decimal; use a Parquet or SQL destination for CBOR data that carries decimals.
