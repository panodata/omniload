(ods)=

# ODS

`omniload` reads [OpenDocument spreadsheet (ODS)] files,
used by [OpenOffice], [LibreOffice], and other spreadsheet applications.

## Where it works

OpenOffice and LibreOffice ODS files can be accessed on every source that
goes through the shared file readers:

- Local files: {ref}`file`
- Remote files: {ref}`s3`, {ref}`gcs`, {ref}`azure-storage`, {ref}`sftp`, ...

A file is read as ODS when its extension is `.ods` (optionally `.ods.gz`),
or when an explicit `#ods` {ref}`format hint <format-hint>` is appended.
Gzipped files are decompressed automatically.

## How it works

The whole file is read into memory and decoded at once (ODS is not a streaming
format); a corrupt or truncated file raises rather than loading partial data.
Map keys are expected to be strings.

## Options

Options can be defined by using reader hints. The loader is using
[polars.read_ods], please consult its documentation about all available
parameters and their descriptions.

Please note due to introspection and automatic type casting capabilities,
the full set of parameters is only available with Python 3.14 and higher.

## Example: Load ODS file into DuckDB

```sh
omniload ingest \
    --source-uri 'file://path/to/workbook.ods#sheet_name=events' \
    --dest-uri   'duckdb:///local.duckdb' \
    --dest-table 'public.events'
```


[LibreOffice]: https://en.wikipedia.org/wiki/LibreOffice
[OpenDocument spreadsheet (ODS)]: https://en.wikipedia.org/wiki/OpenDocument
[OpenOffice]: https://en.wikipedia.org/wiki/Apache_OpenOffice
[polars.read_ods]: https://docs.pola.rs/api/python/stable/reference/api/polars.read_ods.html
