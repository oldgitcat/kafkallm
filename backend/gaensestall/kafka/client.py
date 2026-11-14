from __future__ import annotations

import logging
import time

from confluent_kafka import KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from gaensestall.core.models import to_json_bytes
from gaensestall.core.settings import Settings
from gaensestall.worker.retry import RetryTimeout, retry_until
from pydantic import BaseModel

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
            settings.control_topic,
            settings.logs_topic,
        }
    }
    for name in {
        settings.state_topic,
        settings.agents_topic,
        settings.sandboxes_topic,
    }:
        topics[name] = NewTopic(
            name,
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
    try:
        retry_until(
            deadline,
            lambda: (_check_broker(settings), ensure_topics(settings)),
            description="Kafka",
            logger=logger,
        )
    except RetryTimeout as exc:
        raise RuntimeError(
            "Kafka did not become available within the startup timeout"
        ) from exc
    logger.info("Kafka is reachable and topics are ready")


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

    def send_tombstones(self, topic: str, keys: list[str]) -> None:
        """Delete compacted records by writing null values for their keys."""
        if not keys:
            return
        for key in keys:
            self.producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=None,
                on_delivery=self._log_delivery_error,
            )
        remaining = self.producer.flush(timeout=30)
        if remaining:
            raise TimeoutError("Kafka tombstones could not be confirmed.")

    def close(self) -> None:
        self.producer.flush(timeout=10)
