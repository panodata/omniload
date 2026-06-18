---
outline: deep
---

# Introduction

omniload is a polyglot data loader framework based on dlt.
It allows you to load data from any source into any destination,
either using a concise CLI from your shell,
or the Python API from your own applications.

omniload provides the same efficient incremental data loading modes
inherited by [dlt]: `append`, `merge`, and `delete+insert`.

## Installation

We recommend using [uv](https://github.com/astral-sh/uv) to run `omniload`.

```
pip install uv
uvx omniload
```

Alternatively, if you'd like to install it globally:
```
uv pip install --system omniload
```

While installation with vanilla `pip` is possible, it's an order of magnitude slower.

## Next Steps

Check out the [Quickstart](/getting-started/quickstart.md) guide to get started with omniload.

### License

The project is licensed under the MIT License, see the [LICENSE] file for details.
Some components are licensed under the Apache 2.0 license, see the [NOTICE] file for details.

### Acknowledgements

This project would not have been possible without the amazing work by the
authors and contributors to [SQLAlchemy], [dlt], and [ingestr], turtles all
the way down. Kudos.


```{toctree}
:caption: Commands and adapters
:maxdepth: 1
:hidden:
:glob:
commands/ingest
commands/*
```

```{toctree}
:caption: Handbook
:maxdepth: 1
:hidden:
:glob:
getting-started/core-concepts
getting-started/quickstart
getting-started/incremental-loading
getting-started/data-masking
supported-sources/index
```

```{toctree}
:caption: Tutorials
:maxdepth: 1
:hidden:
:glob:
tutorials/*
```

```{toctree}
:caption: Project
:maxdepth: 1
:hidden:
:glob:
sandbox
changelog
contributors
backlog
```


[dlt]: https://github.com/dlt-hub/dlt
[ingestr]: https://bruin-data.github.io/ingestr/
[LICENSE]: https://github.com/panodata/omniload/blob/main/LICENSE
[NOTICE]: https://github.com/panodata/omniload/blob/main/NOTICE
[SQLAlchemy]: https://www.sqlalchemy.org/
