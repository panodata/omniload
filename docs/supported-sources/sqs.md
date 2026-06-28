# Amazon SQS

[Amazon SQS](https://aws.amazon.com/sqs/) is a fully managed message queuing service.

omniload consumes from SQS via [mq-bridge](mqbridge.md).

## URI format

```text
aws+mqb://?region=us-east-1
```

SQS has no separate connection URL: the queue is named by its full `queue_url`, supplied via
`--source-table` (or `?queue_url=...`, percent-encoded). The `region` is discovered from the
queue URL or `?region=`. Common options: `access_key` / `secret_key` / `session_token`,
`endpoint_url` (e.g. LocalStack), `wait_time_seconds` (long-poll). See [mq-bridge](mqbridge.md)
for the full option reference and delivery semantics.

## Sample command

```sh
omniload ingest \
    --source-uri 'aws+mqb://?region=us-east-1' \
    --source-table 'https://sqs.us-east-1.amazonaws.com/123456789012/orders' \
    --dest-uri 'duckdb:///sqs.duckdb' \
    --dest-table 'dest.orders'
```
