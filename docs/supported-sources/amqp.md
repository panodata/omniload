(amqp)=

# AMQP

The Advanced Message Queuing Protocol ([AMQP]) is an open standard application
layer protocol for message-oriented middleware. The defining features of AMQP
are message orientation, queuing, routing (including point-to-point and
publish-and-subscribe), reliability and security.

[RabbitMQ] is an open-source message broker that implements the Advanced
Message Queuing Protocol (AMQP). It is widely used for building distributed
systems, microservices communication, and asynchronous task processing.

omniload uses [mq-bridge] to consume from AMQP brokers.

## URI format

```text
amqp+mqb://localhost:5672/vhost
```

`--source-table` supplies the AMQP **queue**, but an explicit `?queue=` query
parameter overrides it.
Common options: `queue`, `exchange`, `prefetch_count`, `subscribe_mode` (fan-out),
`no_declare_queue`, `no_persistence`, `username` / `password`.
See [mq-bridge] for the full option reference, TLS, and delivery semantics.

## Sample command

```sh
omniload ingest \
    --source-uri 'amqp+mqb://localhost:5672/vhost' \
    --source-table 'jobs' \
    --dest-uri 'duckdb:///amqp.duckdb' \
    --dest-table 'dest.jobs'
```

The result of this command will be a table in the `amqp.duckdb` database with JSON
columns containing the message data and metadata.


[AMQP]: https://en.wikipedia.org/wiki/Advanced_Message_Queuing_Protocol
[RabbitMQ]: https://www.rabbitmq.com/ 
[mq-bridge]: mqbridge.md
