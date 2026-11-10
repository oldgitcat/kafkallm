from __future__ import annotations

import json
import logging
import threading
import uuid
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException

from kafkallm.models import JobCancel
from kafkallm.settings import Settings

logger = logging.getLogger(__name__)


class CancelStore:
    """In-memory set of job IDs that have been asked to stop."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._cancelled: set[str] = set()
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._consume,
            name="kafkallm-cancels",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def is_cancelled(self, job_id: str) -> bool:
        with self._lock:
            return job_id in self._cancelled

    def add(self, job_id: str) -> None:
        with self._lock:
            self._cancelled.add(job_id)

    def _consume(self) -> None:
        while not self._stop.is_set():
            consumer = Consumer(
                {
                    "bootstrap.servers": self.settings.kafka_bootstrap_servers,
                    "group.id": f"kafkallm-cancels-{uuid.uuid4()}",
                    "auto.offset.reset": "earliest",
                    "enable.auto.commit": False,
                    "session.timeout.ms": 15000,
                }
            )
            consumer.subscribe([self.settings.cancels_topic])
            try:
                while not self._stop.is_set():
                    message = consumer.poll(timeout=1.0)
                    if message is None:
                        continue
                    if message.error():
                        if message.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        raise KafkaException(message.error())
                    value = message.value()
                    if value is None:
                        continue
                    try:
                        payload: Any = json.loads(value.decode("utf-8"))
                        cancel = JobCancel.model_validate(payload)
                    except (UnicodeDecodeError, ValueError):
                        logger.warning("Skipping malformed cancel record")
                        continue
                    self.add(cancel.job_id)
            except Exception:
                logger.exception("Cancel consumer failed; reconnecting")
                self._stop.wait(2)
            finally:
                consumer.close()
