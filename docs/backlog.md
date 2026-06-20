# Backlog

## Iteration +0

- Folksonomy -- define categories
  - File formats
  - Filesystems (local vs. remote vs. cloud storage)
  - Databases
  - Data warehouses
  - Streams
  - Services (System vs. Cloud/SaaS?)
  - Open table formats
- Misc
  - Cloud storage
  - Pipeline frameworks
  - Metrics, telemetry, logs

## Iteration +1

- Documentation: Rework section "supported-sources"
- Documentation: Advertise OCI image
- Code layout: Refactor `omniload.src` into `omniload.x.{category}`
- Tutorial "CSV to Elasticsearch"
  https://github.com/bruin-data/ingestr/issues/487
- Tutorial "Parquet to CrateDB"
- Tutorial "HDFS to CrateDB"
- Check other people's dlt component collections
  https://github.com/thealtoclef/dlt-providers
  https://github.com/thealtoclef/dlt/commits/devel

## Iteration +2

- Add Helm chart, and Improve Cloud deployment docs & resources
  https://images.minimus.io/
- Emphasize and improve file-based formats (CSV, JSON, Parquet, BSON, etc.)
- Refurbish `example-uris` subcommand
- Support 3-component table names (`catalog.schema.table`).
  Make catalog/database/project-qualified names parse consistently.
  https://github.com/bruin-data/ingestr/pull/856

## Iteration +3

- Add scheduling unit
- Add notification unit
- Validate support for CrateDB
  https://github.com/bruin-data/ingestr/pull/284
- Support streaming sources
  https://github.com/bruin-data/ingestr/issues/282
