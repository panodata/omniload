from typing import Any, Optional
from urllib.parse import parse_qs, urlparse


class MqBridgeSource:
    """Consume from a broker via ``mq_bridge.Consumer`` and load through dlt.

    Delivery is at-least-once: each fully-yielded batch's token is acked in ``post_load``,
    after the load commits. A failed load — or a ``--yield-limit`` that truncates a batch
    mid-stream — leaves that batch un-acked, so it is redelivered and the resource merges on
    ``_mqb_id`` to dedup.
    """

    def __init__(self) -> None:
        self._consumer: Optional[Any] = None
        # Tokens of batches fully handed to the load package, to ack in post_load.
        self._pending_batches: list[Any] = []

    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        # mq-bridge owns both keys, so reject conflicting flags loudly instead of letting them
        # silently subvert the delivery guarantee. It manages offsets itself (incrementality),
        # and the resource merges on ``_mqb_id`` for effectively-once delivery. We read the user's
        # *original* request via the ``requested_*`` kwargs because run_ingest nulls
        # ``incremental_key`` for ``handles_incrementality`` sources before this runs.
        if kwargs.get("requested_incremental_key"):
            raise ValueError(
                "mq-bridge takes care of incrementality on its own, "
                "you should not provide --incremental-key"
            )
        if kwargs.get("requested_primary_key"):
            raise ValueError(
                "mq-bridge dedups on its own message id (_mqb_id); do not pass --primary-key, "
                "which would override the merge key and break exactly-once delivery"
            )

        from omniload.source.mqbridge.adapter import (
            endpoint_from_uri,
            mqbridge_resource,
        )

        transport, config = endpoint_from_uri(uri, table)

        params = parse_qs(urlparse(uri).query)

        def _param(key: str, default: Any, cast):
            return cast(params[key][-1]) if params.get(key) else default

        max_messages = _param("max_messages", 100_000, int)
        idle_timeout_ms = _param("idle_timeout_ms", 2_000, int)
        batch_size = _param("batch_size", 500, int)
        fmt = _param("format", "json", str)
        if fmt not in ("json", "text"):
            raise ValueError(
                f"Unsupported mq-bridge format {fmt!r}; expected 'json' or 'text'"
            )

        # Imported lazily (and after the cheap input validation above) so a bad URI or flag fails
        # fast without requiring the native mq-bridge wheel to be importable.
        from mq_bridge import Consumer

        self._consumer = Consumer.from_config(config)
        self._pending_batches = []
        name = table.split(".")[-1] if table else transport
        return mqbridge_resource(
            self._consumer,
            name=name,
            max_messages=max_messages,
            idle_timeout_ms=idle_timeout_ms,
            batch_size=batch_size,
            fmt=fmt,
            record_batch=self._pending_batches.append,
        )

    def post_load(self) -> None:
        # Ack exactly the batches that made it into the load package, then close. Batches
        # left un-acked (e.g. truncated by --yield-limit) stay outstanding and redeliver.
        if self._consumer is not None:
            try:
                for token in self._pending_batches:
                    self._consumer.ack(token)
            finally:
                self._pending_batches = []
                self._consumer.close()
                self._consumer = None

    def release(self) -> None:
        # Close without acking, so the whole run's batches are redelivered.
        if self._consumer is not None:
            self._pending_batches = []
            self._consumer.close()
            self._consumer = None
