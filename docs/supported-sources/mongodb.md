# MongoDB
MongoDB is a popular, open source NoSQL database known for its flexibility, scalability, and wide adoption in a variety of applications.

omniload supports MongoDB as both a source and destination.

## URI format

MongoDB supports two connection string formats:

### Standard format (local/self-hosted)
```text
mongodb://user:password@host:port
```

URI parameters:
- `user`: the user name to connect to the database
- `password`: the password for the user
- `host`: the host address of the database server
- `port`: the port number the database server is listening on, default is 27017 for MongoDB

### SRV format (MongoDB Atlas)
```text
mongodb+srv://user:password@cluster.xxxxx.mongodb.net/?retryWrites=true&w=majority
```

URI parameters:
- `user`: the user name to connect to the database
- `password`: the password for the user
- `cluster.xxxxx.mongodb.net`: the cluster hostname provided by MongoDB Atlas
- Query parameters like `retryWrites` and `w` are optional but recommended for Atlas connections

:::{caution}
Do not put the database name at the end of the URI for MongoDB, instead make it a part of `--source-table` or `--dest-table` option as `database.collection` format.
:::

The same URI structure can be used both for sources and destinations. You can read more about MongoDB's connection string format [here](https://docs.mongodb.com/manual/reference/connection-string/).

## Source table format

The `--source-table` option for MongoDB supports two formats:

### Basic format
```text
database.collection
```

This performs a simple collection scan, equivalent to `db.collection.find()`.

### Custom aggregation format
```text
database.collection:[aggregation_pipeline]
```

This allows you to specify a custom MongoDB aggregation pipeline as a JSON array.

## Custom aggregations

omniload supports custom MongoDB aggregation pipelines, similar to how SQL sources support custom queries. This allows you to perform complex data transformations, filtering, and projections directly in MongoDB before the data is ingested.

### Basic syntax

Use the following format for custom aggregations:

```bash
omniload ingest \
  --source-uri "mongodb://user:password@host:port" \
  --source-table 'database.collection:[{"$match": {...}}, {"$project": {...}}]'
```

### Examples

#### Simple filtering
```bash
omniload ingest \
  --source-uri "mongodb://localhost:27017" \
  --source-table 'mydb.users:[{"$match": {"status": "active"}}]'
```

#### Complex aggregation with grouping
```bash
omniload ingest \
  --source-uri "mongodb://localhost:27017" \
  --source-table 'mydb.orders:[
    {"$match": {"status": "completed"}},
    {"$group": {
      "_id": "$customer_id",
      "total_orders": {"$sum": 1},
      "total_amount": {"$sum": "$amount"}
    }}
  ]'
```

#### Projection and transformation
```bash
omniload ingest \
  --source-uri "mongodb://localhost:27017" \
  --source-table 'mydb.products:[
    {"$project": {
      "name": 1,
      "price": 1,
      "category": 1,
      "price_usd": {"$multiply": ["$price", 1.1]}
    }}
  ]'
```

### Incremental loads with custom aggregations

Custom aggregations support incremental loading when combined with the `--incremental-key` option. The incremental key must be included in the projected fields of your aggregation pipeline.

#### Using interval placeholders

You can use `:interval_start` and `:interval_end` placeholders in your aggregation pipeline, which will be automatically replaced with the actual datetime values during incremental loads:

```bash
omniload ingest \
  --source-uri "mongodb://localhost:27017" \
  --source-table 'mydb.events:[
    {"$match": {
      "created_at": {
        "$gte": ":interval_start",
        "$lt": ":interval_end"
      }
    }},
    {"$project": {
      "_id": 1,
      "event_type": 1,
      "user_id": 1,
      "created_at": 1
    }}
  ]' \
  --incremental-key "created_at"
```

#### Requirements for incremental loads

When using incremental loads with custom aggregations:

1. **Incremental key projection**: The field specified in `--incremental-key` must be included in your projection
2. **Datetime type**: The incremental key should be a datetime field
3. **Pipeline validation**: omniload validates that your aggregation pipeline properly projects the incremental key

### Validation and error handling

omniload performs several validations on custom aggregation pipelines:

- **JSON validation**: Ensures the aggregation pipeline is valid JSON
- **Array format**: Aggregation pipelines must be JSON arrays
- **Incremental key validation**: When using `--incremental-key`, validates that the key is projected in the pipeline
- **Clear error messages**: Provides specific error messages for common issues

### Limitations

- **Parallel loading**: Custom aggregations don't support parallel loading due to MongoDB cursor limitations. The loader automatically falls back to sequential processing.
- **Arrow format**: When using Arrow data format with custom aggregations, data is converted to Arrow format after loading rather than using native MongoDB Arrow integration.

### Performance considerations

- Use `$match` stages early in your pipeline to filter data as soon as possible
- Add appropriate indexes to support your aggregation pipeline
- Consider using `$limit` to restrict the number of documents processed
- For large datasets, MongoDB's `allowDiskUse: true` option is automatically enabled for aggregation pipelines

## Using MongoDB Atlas as a source

MongoDB Atlas can be used as a source to extract data using the SRV connection string format.

```bash
omniload ingest \
  --source-uri "mongodb+srv://username:password@cluster0.xxxxx.mongodb.net/?retryWrites=true&w=majority" \
  --source-table "mydb.users" \
  --dest-uri "duckdb:///local.duckdb" \
  --dest-table "analytics.users"
```

:::{note}
When using MongoDB Atlas as a source, ensure your IP address is whitelisted in Network Access settings. You can find this under Security > Network Access in your Atlas dashboard.
:::

All the custom aggregation features described above work with MongoDB Atlas as well:

```bash
omniload ingest \
  --source-uri "mongodb+srv://username:password@cluster0.xxxxx.mongodb.net/?retryWrites=true&w=majority" \
  --source-table 'mydb.orders:[{"$match": {"status": "completed"}}]' \
  --dest-uri "duckdb:///local.duckdb" \
  --dest-table "analytics.completed_orders"
```

## Using MongoDB as a destination

MongoDB can be used as a destination to load data from various sources. The `--dest-table` option follows the same format: `database.collection`.

### MongoDB Atlas

```bash
omniload ingest \
  --source-uri "postgres://user:pass@localhost:5432/mydb" \
  --source-table "public.users" \
  --dest-uri "mongodb+srv://username:password@cluster0.xxxxx.mongodb.net/?retryWrites=true&w=majority" \
  --dest-table "mydb.users"
```

:::{note}
When using MongoDB Atlas as a destination, ensure your IP address is whitelisted in Network Access settings.
:::

### Local MongoDB with authentication

```bash
omniload ingest \
  --source-uri "csv:///path/to/data.csv" \
  --source-table "data" \
  --dest-uri "mongodb://username:password@localhost:27017/?authSource=admin" \
  --dest-table "mydb.mycollection"
```

### Local MongoDB without authentication

```bash
omniload ingest \
  --source-uri "csv:///path/to/data.csv" \
  --source-table "data" \
  --dest-uri "mongodb://localhost:27017" \
  --dest-table "mydb.mycollection"
```

:::{tip}
By default, omniload uses a "replace" strategy which deletes existing data in the collection before loading new data. The target database and collection will be created automatically if they don't exist.
:::
