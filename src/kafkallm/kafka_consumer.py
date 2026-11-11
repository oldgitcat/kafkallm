"""Shared Kafka consumer primitives.

The GUI and worker used to open a nearly identical consumer loop for every
topic. These helpers keep the reconnect, EOF, tombstone, and decoding rules in
one place so a fix applies everywhere.
"""

from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Message
from pydantic import BaseModel

from kafkallm.settings import Settings

logger = logging.getLogger(__name__)


def consumer_config(
    settings: Settings,
    group_id: str,
    *,
    session_timeout_ms: int = 15000,
    offset_reset: str = "earliest",
) -> dict[str, Any]:
    return {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": offset_reset,
        "enable.auto.commit": False,
        "session.timeout.ms": session_timeout_ms,
    }


def iter_messages(
    settings: Settings,
    topics: list[str],
    group_prefix: str,
    stop: threading.Event,
    *,
    session_timeout_ms: int = 15000,
    offset_reset: str = "earliest",
    on_idle: Callable[[], bool] | None = None,
) -> Iterator[Message]:
    """Yield messages until *stop* is set, reconnecting after consumer errors.

    ``on_idle`` runs after every poll that returned no message and may request
    shutdown by returning True.
    """
    description = ",".join(topics)
    while not stop.is_set():
        consumer = Consumer(
            consumer_config(
                settings,
                f"{group_prefix}-{uuid.uuid4()}",
                session_timeout_ms=session_timeout_ms,
                offset_reset=offset_reset,
            )
        )
        consumer.subscribe(topics)
        try:
            while not stop.is_set():
                message = consumer.poll(timeout=1.0)
                if message is None:
                    if on_idle is not None and on_idle():
                        return
                    continue
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(message.error())
                yield message
        except Exception:
            logger.exception(
                "Kafka consumer for %s failed; reconnecting",
                description,
            )
            stop.wait(2)
        finally:
            consumer.close()


def message_key(message: Message) -> str | None:
    key = message.key()
    if not isinstance(key, bytes) or not key:
        return None
    try:
        return key.decode("utf-8")
    except UnicodeDecodeError:
        logger.warning("Skipping Kafka record with a non-UTF-8 key")
        return None


def decode_json_value(message: Message) -> Any | None:
    value = message.value()
    if value is None:
        return None
    try:
        return json.loads(value.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        logger.warning("Skipping malformed Kafka record")
        return None


def read_compacted[JSON](
    settings: Settings,
    topic: str,
    parse: Callable[[bytes], JSON],
    *,
    timeout: float = 2.0,
) -> dict[str, JSON]:
    """Read a compacted topic from the beginning and return its latest records.

    This is a bounded snapshot read, not a long-lived store. It is used for
    seeding and for one-off CLI lookups.
    """
    consumer = Consumer(consumer_config(settings, f"kafkallm-read-{uuid.uuid4()}"))
    consumer.subscribe([topic])
    found: dict[str, JSON] = {}
    deadline = time.monotonic() + timeout
    try:
        while time.monotonic() < deadline:
            message = consumer.poll(timeout=0.25)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(message.error())
            key = message_key(message)
            if key is None:
                continue
            value = message.value()
            if value is None:
                found.pop(key, None)
                continue
            try:
                found[key] = parse(value)
            except ValueError:
                logger.warning("Skipping malformed record for %s in %s", key, topic)
    finally:
        consumer.close()
    return found


class CompactedStore[T: BaseModel]:
    """In-memory view of one compacted registry topic."""

    def __init__(
        self,
        settings: Settings,
        topic: str,
        model: type[T],
        *,
        group_prefix: str,
        key_attr: str,
        label: str,
    ) -> None:
        self.settings = settings
        self.topic = topic
        self.model = model
        self.key_attr = key_attr
        self.label = label
        self.group_prefix = group_prefix
        self._items: dict[str, T] = {}
        self._condition = threading.Condition()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._consume,
            name=f"kafkallm-{self.group_prefix}",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def values(self) -> list[T]:
        with self._condition:
            return list(self._items.values())

    def get(self, item_id: str) -> T | None:
        with self._condition:
            return self._items.get(item_id)

    def wait_for(self, item_id: str, timeout: float = 3.0) -> T | None:
        with self._condition:
            self._condition.wait_for(
                lambda: item_id in self._items,
                timeout=timeout,
            )
            return self._items.get(item_id)

    def put(self, item: T) -> None:
        item_id = getattr(item, self.key_attr)
        with self._condition:
            self._items[item_id] = item
            self._condition.notify_all()

    def remove(self, item_id: str) -> None:
        with self._condition:
            self._items.pop(item_id, None)
            self._condition.notify_all()

    def _consume(self) -> None:
        for message in iter_messages(
            self.settings,
            [self.topic],
            self.group_prefix,
            self._stop,
        ):
            item_id = message_key(message)
            if item_id is None:
                continue
            value = message.value()
            if value is None:
                self.remove(item_id)
                continue
            try:
                item = self.model.model_validate_json(value)
            except ValueError:
                logger.warning("Skipping malformed %s record", self.label)
                continue
            self.put(item)
