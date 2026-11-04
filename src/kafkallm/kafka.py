from __future__ import annotations

import logging
import time

from confluent_kafka import KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from pydantic import BaseModel

from kafkallm.models import to_json_bytes
from kafkallm.settings import Settings


logger = logging.getLogger(__name__)


def _check_broker(settings: Settings) -> None:
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    admin.list_topics(timeout=5)


def ensure_topics(settings: Settings) -> None:
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    topics = {
        name: NewTopic(
            name,
            num_partitions=settings.kafka_topic_partitions,
            replication_factor=1,
            config={"retention.ms": str(settings.kafka_retention_ms)},
        )
        for name in {
            settings.jobs_topic,
            settings.results_topic,
            settings.events_topic,
        }
    }
    topics[settings.state_topic] = NewTopic(
        settings.state_topic,
        num_partitions=settings.kafka_topic_partitions,
        replication_factor=1,
        config={
            "cleanup.policy": "compact",
            "retention.ms": "-1",
            "min.cleanable.dirty.ratio": "0.01",
        },
    )
    futures = admin.create_topics(list(topics.values()))
    for name, future in futures.items():
        try:
            future.result(timeout=10)
            logger.info("Created topic: %s", name)
        except KafkaException as exc:
            error = exc.args[0]
            if error.code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise


def wait_for_kafka(settings: Settings) -> None:
    deadline = time.monotonic() + max(0, settings.kafka_startup_timeout_seconds)
    delay = 1.0
    last_error: Exception | None = None

    while time.monotonic() < deadline:
        try:
            _check_broker(settings)
            ensure_topics(settings)
            logger.info("Redpanda is reachable and topics are ready")
            return
        except (KafkaException, TimeoutError, OSError) as exc:
            last_error = exc
            remaining = max(0.0, deadline - time.monotonic())
            logger.warning(
                "Redpanda not ready yet (%s); retrying in %.1fs",
                exc,
                min(delay, remaining),
            )
        time.sleep(min(delay, max(0.0, deadline - time.monotonic())))
        delay = min(delay * 2, 10.0)

    message = "Redpanda did not become available within the startup timeout"
    if last_error is not None:
        raise RuntimeError(message) from last_error
    raise RuntimeError(message)


class KafkaProducer:
    def __init__(self, settings: Settings) -> None:
        self.producer = Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "acks": "all",
                "enable.idempotence": True,
            }
        )

    @staticmethod
    def _log_delivery_error(error: KafkaError | None, message: object) -> None:
        del message
        if error is not None:
            logger.warning("Kafka delivery failed: %s", error)

    def send(
        self,
        topic: str,
        key: str,
        model: BaseModel,
        headers: list[tuple[str, bytes]] | None = None,
    ) -> None:
        errors: list[Exception] = []

        def delivered(error: KafkaError | None, message: object) -> None:
            del message
            if error is not None:
                errors.append(KafkaException(error))

        self.producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=to_json_bytes(model),
            headers=headers or [],
            on_delivery=delivered,
        )
        remaining = self.producer.flush(timeout=30)
        if remaining:
            raise TimeoutError("Kafka message could not be confirmed.")
        if errors:
            raise errors[0]

    def send_async(
        self,
        topic: str,
        key: str,
        model: BaseModel,
        headers: list[tuple[str, bytes]] | None = None,
    ) -> None:
        self.producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=to_json_bytes(model),
            headers=headers or [],
            on_delivery=self._log_delivery_error,
        )

    def close(self) -> None:
        self.producer.flush(timeout=10)
