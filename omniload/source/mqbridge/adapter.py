from typing import Any, Callable, Dict, Iterable, NamedTuple, Optional, Tuple
from urllib.parse import ParseResult, parse_qs, urlparse

import dlt
from dlt.common.typing import TDataItem

# mq-bridge takes an endpoint config {<transport>: {...}}; see mq_bridge.config. We address it
# via compound <transport>+mqb schemes, e.g. kafka+mqb://localhost:9092?group_id=g or
# memory+mqb://?topic=orders. --source-table supplies the topic/subject/queue.

# Query params for the resource loop, not the endpoint config.
RESOURCE_PARAMS = frozenset({"max_messages", "idle_timeout_ms", "batch_size", "format"})

# Endpoint fields that parse_qs returns as strings but mq-bridge expects typed.
_INT_FIELDS = frozenset(
    {
        "capacity",
        "prefetch_count",
        "qos",
        "max_inflight",
        "keep_alive_seconds",
        "session_expiry_interval",
        "queue_capacity",
        "request_timeout_ms",
        "stream_max_bytes",
        "stream_max_messages",
        "wait_time_seconds",  # aws (SQS long-poll)
        "internal_buffer_size",  # zeromq, ibmmq
        "max_message_size",  # ibmmq
        "wait_timeout_ms",  # ibmmq (consumer poll timeout)
    }
)
_BOOL_FIELDS = frozenset(
    {
        "delayed_ack",
        "no_jetstream",
        "request_reply",
        "subscribe_mode",
        "subscriber_mode",
        "clean_session",
        "no_declare_queue",
        "no_persistence",
        "enable_nack",
        "shared",
        "binary_payload_mode",  # aws
        "bind",  # zeromq
        "disable_status_inq",  # ibmmq
        "required",  # tls.required
        "accept_invalid_certs",  # tls.accept_invalid_certs
    }
)


class _Transport(NamedTuple):
    """How a transport turns a ``<transport>+mqb://`` URI into its mq-bridge config.

    ``table_field`` is the field ``--source-table`` fills (topic/subject/queue/queue_url).
    ``authority_field`` is the field the URI authority fills (usually ``url``), or ``None`` when
    the transport has no connection URL — SQS names the queue through ``table_field`` instead.
    ``render`` turns the parsed URI into the authority value, prefixing the broker scheme.

    For memory, ``authority_field`` and ``table_field`` are the same slot (``url``/``topic`` are
    aliases), which makes "either the authority or ``?topic=``, not both" fall out for free.
    """

    table_field: str
    authority_field: Optional[str] = None
    render: Optional[Callable[[ParseResult], str]] = None


def _ibmmq_conn_name(parsed: ParseResult) -> str:
    """Render the authority as an IBM MQ connection name list (``host(port)``).

    IBM MQ addresses queue managers as ``host(port)`` rather than ``host:port``, and accepts a
    comma-separated list for failover. We accept the familiar ``host:port[,host:port]`` authority
    (consistent with kafka/nats) and translate each entry; a bare host is passed through as-is.
    """
    names = []
    for hostport in parsed.netloc.split(","):
        hostport = hostport.strip()
        if not hostport:
            continue
        if ":" in hostport:
            host, _, port = hostport.rpartition(":")
            names.append(f"{host}({port})")
        else:
            names.append(hostport)
    return ",".join(names)


# kafka/nats take a comma-separated host list in the authority (cluster/replica); mqtt and amqp
# are single-host (front them with a load balancer / DNS for HA). zeromq also accepts ipc:// —
# pass ?url= to override the tcp:// default. ibmmq needs queue_manager + channel as query params
# and consumes from a queue (?topic= switches to pub/sub subscriber mode).
_TRANSPORTS: Dict[str, _Transport] = {
    "kafka": _Transport(
        "topic", "url", lambda p: p.netloc
    ),  # bare broker list, no scheme
    "nats": _Transport("subject", "url", lambda p: f"nats://{p.netloc}"),
    "amqp": _Transport("queue", "url", lambda p: f"amqp://{p.netloc}{p.path}"),
    "mqtt": _Transport("topic", "url", lambda p: f"tcp://{p.netloc}"),
    "zeromq": _Transport("topic", "url", lambda p: f"tcp://{p.netloc}"),
    "aws": _Transport(
        "queue_url"
    ),  # SQS: queue_url is itself the URL, no connection url
    "ibmmq": _Transport("queue", "url", _ibmmq_conn_name),
    "memory": _Transport("topic", "topic", lambda p: f"{p.netloc}{p.path}"),
}


def _coerce(key: str, value: str) -> Any:
    if key in _INT_FIELDS:
        try:
            return int(value)
        except ValueError:
            raise ValueError(
                f"Query parameter {key!r} expects an integer, got {value!r}"
            )
    if key in _BOOL_FIELDS:
        return value.strip().lower() in ("1", "true", "yes", "on")
    return value


def _assign(config: Dict[str, Any], key: str, value: str) -> None:
    """Set ``key`` on ``config``, expanding dotted keys into nested config blocks.

    ``tls.required=true`` becomes ``{"tls": {"required": True}}``, so nested mq-bridge blocks
    (TLS/mTLS settings) are expressible as flat query parameters. Coercion keys off the leaf
    segment, so ``tls.required`` is still treated as a boolean.
    """
    *parents, leaf = key.split(".")
    node = config
    for part in parents:
        child = node.setdefault(part, {})
        if not isinstance(child, dict):
            raise ValueError(
                f"Conflicting query parameter {key!r}: {part!r} is set both as a value "
                "and as a nested block"
            )
        node = child
    node[leaf] = _coerce(leaf, value)


def endpoint_from_uri(uri: str, table: str) -> Tuple[str, Dict[str, Dict[str, Any]]]:
    """Translate a ``<transport>+mqb://`` URI into ``(transport, Consumer.from_config arg)``."""
    parsed = urlparse(uri)
    scheme = parsed.scheme
    if not scheme.endswith("+mqb"):
        raise ValueError(f"Not an mq-bridge URI scheme: {scheme!r}")

    transport = scheme[: -len("+mqb")]
    spec = _TRANSPORTS.get(transport)
    if spec is None:
        supported = ", ".join(sorted(_TRANSPORTS))
        raise ValueError(
            f"Unsupported mq-bridge transport: {transport!r}. Supported: {supported}"
        )

    config: Dict[str, Any] = {}
    for key, values in parse_qs(parsed.query).items():
        if key in RESOURCE_PARAMS or not values:
            continue
        _assign(config, key, values[-1])

    # The authority fills its field unless an explicit ?url= / ?topic= already did; --source-table
    # fills the topic-like field likewise. Explicit query params always win over both.
    if spec.authority_field and spec.render and spec.authority_field not in config:
        value = spec.render(parsed)
        if value:
            config[spec.authority_field] = value
    if table and spec.table_field not in config:
        config[spec.table_field] = table

    return transport, {transport: config}


def mqbridge_resource(
    consumer: Any,
    *,
    name: str,
    max_messages: int,
    idle_timeout_ms: int,
    batch_size: int,
    fmt: str,
    record_batch: Callable[[Any], None],
):
    """Bounded merge-mode resource draining ``consumer`` for one pipeline run.

    Records carry ``_mqb_id`` (the merge key) and ``_mqb_metadata``. Nothing is acked here;
    each fully-yielded batch's token is handed to ``record_batch`` so
    ``MqBridgeSource.post_load`` can ack exactly those batches after the load commits.
    """

    def reader() -> Iterable[TDataItem]:
        drained = 0
        while drained < max_messages:
            messages, token = consumer.poll_batch(
                max=min(batch_size, max_messages - drained),
                timeout_ms=idle_timeout_ms,
            )
            if not messages:
                break  # idle or end-of-stream
            for message in messages:
                if fmt == "text":
                    payload: Any = {"value": message.text()}
                else:
                    payload = message.json()
                    if not isinstance(payload, dict):
                        payload = {"value": payload}
                yield {
                    "_mqb_id": message.id,
                    "_mqb_metadata": dict(message.metadata),
                    **payload,
                }
            # Every message in this batch has now been yielded (so it is in the load
            # package); record its token for post_load to ack. If a --yield-limit
            # (dlt add_limit) truncates the stream mid-batch, control never reaches here
            # for that batch, so its token stays outstanding and the batch is redelivered
            # next run and deduped on _mqb_id — we never ack messages we didn't load.
            record_batch(token)
            drained += len(messages)

    return dlt.resource(
        reader,
        name=name,
        write_disposition="merge",
        primary_key="_mqb_id",
    )()
