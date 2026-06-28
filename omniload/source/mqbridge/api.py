from typing import Any, Optional
from urllib.parse import parse_qs, urlparse


class MqBridgeSource:
    """Consume from a broker via ``mq_bridge.Consumer`` and load through dlt.

    Delivery is at-least-once: the consumer is committed in ``post_load``, after the load
    commits. A failed load is redelivered, so the resource merges on ``_mqb_id`` to dedup.
    """

    def __init__(self) -> None:
        self._consumer: Optional[Any] = None

    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key"):
            raise ValueError(
                "mq-bridge takes care of incrementality on its own, "
                "you should not provide incremental_key"
            )

        from mq_bridge import Consumer

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

        self._consumer = Consumer.from_config(config)
        name = table.split(".")[-1] if table else transport
        return mqbridge_resource(
            self._consumer,
            name=name,
            max_messages=max_messages,
            idle_timeout_ms=idle_timeout_ms,
            batch_size=batch_size,
            fmt=fmt,
        )

    def post_load(self) -> None:
        # Ack what we polled this run, then close.
        if self._consumer is not None:
            try:
                self._consumer.commit()
            finally:
                self._consumer.close()
                self._consumer = None

    def release(self) -> None:
        # Close without committing, so the batch is redelivered.
        if self._consumer is not None:
            self._consumer.close()
            self._consumer = None
