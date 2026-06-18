<div align="center">

# omniload
<p>Copy data between any source and any destination.</p>
<img src="https://github.com/panodata/omniload/blob/main/resources/demo.gif?raw=true" width="750" />

</div>

## About

omniload is a polyglot data loader framework based on dlt.
It allows you to load data from any source into any destination,
either using a concise CLI from your shell,
or the Python API from your own applications.

omniload provides the same efficient incremental data loading modes
inherited by [dlt]: `append`, `merge`, and `delete+insert`.

## Install
We recommend using [uv] to install or run `omniload`.

```shell
pip install uv
uvx omniload
```

Alternatively, if you'd like to install it globally:
```shell
uv pip install --system omniload
```

While installation with vanilla `pip` is possible, it's an order of magnitude slower.

## Synopsis

The next command instructs omniload to read the table `public.some_data` from
your PostgreSQL instance, and to write the data to your BigQuery warehouse
under the schema `omniload` and table `some_data`.

```shell
omniload ingest \
    --source-uri 'postgresql://admin:admin@localhost:8837/web?sslmode=disable' \
    --source-table 'public.some_data' \
    --dest-uri 'bigquery://<your-project-name>?credentials_path=/path/to/service/account.json' \
    --dest-table 'omniload.some_data'
```

## Handbook

Please visit the [full documentation][Documentation], or inspect the
list of supported [sources and destinations].

## Project

### Contribute

Contributions are very much welcome. Please visit the [Documentation]
to learn how to spin up a sandbox environment on your workstation and submit
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
[Documentation]: https://github.com/panodata/omniload/blob/main/docs/getting-started/quickstart.md
[ingestr]: https://bruin-data.github.io/ingestr/
[Issues]: https://github.com/panodata/omniload/issues
[LICENSE]: https://github.com/panodata/omniload/blob/main/LICENSE
[NOTICE]: https://github.com/panodata/omniload/blob/main/NOTICE
[sources and destinations]: https://github.com/panodata/omniload/blob/main/docs/supported-sources/index.md
[SQLAlchemy]: https://www.sqlalchemy.org/
[uv]: https://docs.astral.sh/uv/
