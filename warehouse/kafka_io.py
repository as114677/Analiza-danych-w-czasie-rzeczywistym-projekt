from __future__ import annotations

import json
from typing import Iterator

try:
    from kafka import KafkaConsumer, KafkaProducer
except ImportError:
    KafkaConsumer = None
    KafkaProducer = None


class WarehouseConsumer:

    def __init__(self, bootstrap_servers: str, group_id: str = "warehouse-processor") -> None:
        if KafkaConsumer is None:
            raise RuntimeError("Brak biblioteki kafka-python. Zainstaluj: pip install -r requirements.txt")

        self._consumer = KafkaConsumer(
            "products",
            "sales",
            bootstrap_servers=bootstrap_servers.split(","),
            group_id=group_id,
            auto_offset_reset="earliest",      
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            consumer_timeout_ms=-1,               
        )

    def poll_events(self) -> Iterator[tuple[str, dict]]:
        for message in self._consumer:
            yield message.topic, message.value

    def close(self) -> None:
        self._consumer.close()


class WarehouseProducer:

    def __init__(self, bootstrap_servers: str) -> None:
        if KafkaProducer is None:
            raise RuntimeError("Brak biblioteki kafka-python. Zainstaluj: pip install -r requirements.txt")

        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers.split(","),
            key_serializer=lambda v: v.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            acks="all",
            retries=3,
        )

    def send(self, topic: str, key: str, value: dict) -> None:
        self._producer.send(topic, key=key, value=value)

    def flush(self) -> None:
        self._producer.flush()

    def close(self) -> None:
        self._producer.flush()
        self._producer.close()
