---
outline: deep
---

# Introduction

omniload is a command-line app that allows you to ingest data from any source into any destination using simple command-line flags, no code necessary.

- ✨ copy data from your database into any destination
- ➕ incremental loading: `append`, `merge` or `delete+insert`
- 🐍 single-command installation

omniload takes away the complexity of managing any backend or writing any code for ingesting data, simply run the command and watch the data land on its destination.

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


```{toctree}
:caption: Commands and adapters
:maxdepth: 1
:hidden:
:glob:
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


[LICENSE]: https://github.com/panodata/omniload/blob/main/LICENSE.md
[NOTICE]: https://github.com/panodata/omniload/blob/main/NOTICE
