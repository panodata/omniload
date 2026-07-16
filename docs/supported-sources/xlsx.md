(xlsx)=

# XLSX

`omniload` reads [Excel Workbook] XLSX spreadsheet files.
XLSX is currently supported for read operations only.

## Where it works

Excel XLSX files can be accessed on every source that goes through the shared file readers:

- Local files: [`file://`](file.md)
- [`s3://`](s3.md), [`gs://`](google-cloud-storage.md), [Azure blob storage](azure-blob-storage.md)
- [`sftp://`](sftp.md)

A file is read as XLSX when its extension is `.xlsx` (optionally `.xlsx.gz`),
or when an explicit `#xlsx` {ref}`format hint <format-hint>` is appended.
Gzipped files are decompressed automatically.

## How it works

The whole file is read into memory and decoded at once (XLSX is not a streaming
format); a corrupt or truncated file raises rather than loading partial data.
Map keys are expected to be strings.

## Options

Options can be defined by using reader hints. The loader is using
[polars.read_excel], please consult its documentation about all available
parameters and their descriptions.

Please note due to introspection and automatic type casting capabilities,
the full set of parameters is only available with Python 3.14 and higher.

## Example: Load XLSX file into DuckDB

```sh
omniload ingest \
    --source-uri 'file://path/to/workbook.xlsx#sheet_name=events' \
    --dest-uri   'duckdb:///local.duckdb' \
    --dest-table 'public.events'
```


[Excel Workbook]: https://en.wikipedia.org/wiki/Microsoft_Excel#Current_file_extensions
[polars.read_excel]: https://docs.pola.rs/api/python/stable/reference/api/polars.read_excel.html
