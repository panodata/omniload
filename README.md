<div align="center">

# omniload
<p>Copy data between any source and any destination.</p>
<img src="https://github.com/panodata/omniload/blob/main/resources/demo.gif?raw=true" width="750" />

[![License](https://img.shields.io/github/license/panodata/omniload.svg)](https://github.com/panodata/omniload/blob/main/LICENSE)
[![Downloads / month](https://pepy.tech/badge/omniload/month)](https://pepy.tech/project/omniload/)
[![Python versions](https://img.shields.io/pypi/pyversions/omniload.svg)](https://pypi.org/project/omniload/)

</div>

## About

omniload is a polyglot data loader framework based on [dlt], the
open-source Python library for building data pipelines. dlt does the
heavy lifting (schema inference, normalization, and incremental load
state); omniload wraps it behind a single CLI and a URI scheme, so you
can copy data between any source and any destination without writing
pipeline code, either from your shell or the Python API.

omniload provides the same efficient incremental data loading modes
inherited by dlt: `append`, `merge`, and `delete+insert`.

## Install

### Container

For running `omniload` in a container environment, use the provided OCI
images.

```shell
docker run --rm ghcr.io/panodata/omniload:latest version
```

### Package

For running or installing the `omniload` Python package,
we are recommending to use [uv]. [^1][^2]
```shell
uvx omniload
```

Alternatively, if you like to install it account-wide on your system:
```shell
uv tool install omniload
```

[^1]: You can install `uv` using `pip install uv`, or another method that matches your operating system.
[^2]: While installing `omniload` with vanilla `pip` is possible, it is an order of magnitude slower.

## Synopsis

### CLI

Instruct omniload to read from the PostgreSQL table `public.some_data`,
and write to the BigQuery warehouse table `omniload.some_data`.

```shell
omniload ingest \
    --source-uri 'postgresql://admin:admin@localhost:8837/web?sslmode=disable' \
    --source-table 'public.some_data' \
    --dest-uri 'bigquery://<your-project-name>?credentials_path=/path/to/service/account.json' \
    --dest-table 'omniload.some_data'
```

### Python API

Using `omniload.run_ingest`, you can run the same
ingestion from your own application. The keyword arguments map one-to-one to the
CLI options, and it returns the dlt `LoadInfo` for the run (or `None` for a dry run).

```python
from omniload import run_ingest

info = run_ingest(
    source_uri="sqlite:///./source.db",
    dest_uri="duckdb:///./warehouse.duckdb",
    source_table="main.some_table",
    dest_table="public.some_table",
)
print(info)
```

See the [Python API documentation] for dry runs, error handling, and passing
enum options as strings.

## Handbook

Please visit the [quickstart documentation], or inspect the
list of supported [sources and destinations].

## Project

### Contribute

Contributions are very much welcome. Please visit the [sandbox documentation]
to learn how to spin up a development environment on your workstation and submit
patches, or create a [ticket][Issues] to report a bug or propose a feature.

### Status

Breaking changes should be expected until a 1.0 release, so version pinning is
strongly recommended, especially when using this software as a library.
For example:
```shell
pip install 'omniload[full]==0.0.42'
```

### License

The project is licensed under the MIT License, see the [LICENSE] file for details.
Some components are licensed under the Apache 2.0 license, see the [NOTICE] file for details.

### Acknowledgements

This project would not have been possible without the amazing work by the
authors and contributors to [SQLAlchemy], [dlt], and [ingestr], turtles all
the way down. Kudos.


[dlt]: https://github.com/dlt-hub/dlt
[quickstart documentation]: https://omniload.readthedocs.io/getting-started/quickstart.html
[Python API documentation]: https://omniload.readthedocs.io/getting-started/python-api.html
[ingestr]: https://bruin-data.github.io/ingestr/
[Issues]: https://github.com/panodata/omniload/issues
[LICENSE]: https://github.com/panodata/omniload/blob/main/LICENSE
[NOTICE]: https://github.com/panodata/omniload/blob/main/NOTICE
[sandbox documentation]: https://omniload.readthedocs.io/sandbox.html
[sources and destinations]: https://omniload.readthedocs.io/supported-sources/
[SQLAlchemy]: https://www.sqlalchemy.org/
[uv]: https://docs.astral.sh/uv/
