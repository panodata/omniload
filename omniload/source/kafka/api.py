from urllib.parse import parse_qs, urlparse


class KafkaSource:
    def handles_incrementality(self) -> bool:
        return False

    def dlt_source(self, uri: str, table: str, **kwargs):
        # kafka://?bootstrap_servers=localhost:9092&group_id=test_group&security_protocol=SASL_SSL&sasl_mechanisms=PLAIN&sasl_username=example_username&sasl_password=example_secret
        source_fields = urlparse(uri)
        source_params = parse_qs(source_fields.query)

        bootstrap_servers = source_params.get("bootstrap_servers")
        group_id = source_params.get("group_id")
        security_protocol = source_params.get("security_protocol", [])
        sasl_mechanisms = source_params.get("sasl_mechanisms", [])
        sasl_username = source_params.get("sasl_username", [])
        sasl_password = source_params.get("sasl_password", [])
        batch_size = source_params.get("batch_size", [3000])
        batch_timeout = source_params.get("batch_timeout", [3])

        # Decoding options.
        key_type = source_params.get("key_type", [])
        value_type = source_params.get("value_type", [])
        output_format = source_params.get("format", [])
        include = source_params.get("include", [])
        select = source_params.get("select", [])

        if not bootstrap_servers:
            raise ValueError(
                "bootstrap_servers in the URI is required to connect to kafka"
            )

        if not group_id:
            raise ValueError("group_id in the URI is required to connect to kafka")

        start_date = kwargs.get("interval_start")
        from omniload.source.kafka.adapter import (
            KafkaCredentials,
            KafkaEventProcessor,
            kafka_consumer,
        )
        from omniload.source.kafka.model import KafkaDecodingOptions

        options = KafkaDecodingOptions.from_params(
            key_type=key_type,
            value_type=value_type,
            format=output_format,
            include=include,
            select=select,
        )

        return kafka_consumer(
            topics=[table],
            credentials=KafkaCredentials(
                bootstrap_servers=bootstrap_servers[0],
                group_id=group_id[0],
                security_protocol=(
                    security_protocol[0] if len(security_protocol) > 0 else None
                ),
                sasl_mechanisms=(
                    sasl_mechanisms[0] if len(sasl_mechanisms) > 0 else None
                ),
                sasl_username=sasl_username[0] if len(sasl_username) > 0 else None,
                sasl_password=sasl_password[0] if len(sasl_password) > 0 else None,
            ),
            msg_processor=KafkaEventProcessor(options=options).process,
            start_from=start_date,
            batch_size=int(batch_size[0]),
            batch_timeout=int(batch_timeout[0]),
        )
