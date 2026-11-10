from __future__ import annotations

import logging
import threading

from kafkallm.kafka import KafkaProducer
from kafkallm.models import JobLog
from kafkallm.settings import Settings

logger = logging.getLogger(__name__)

_MAX_LOG_TEXT = 20_000


class LogPublisher:
    """Publish live job activity without blocking the job on Kafka flushes."""

    def __init__(self, settings: Settings, producer: KafkaProducer) -> None:
        self.settings = settings
        self.producer = producer
        self._sequences: dict[str, int] = {}
        self._lock = threading.Lock()

    def publish(self, job_id: str, kind: str, text: str) -> None:
        text = text.strip("\r\n")
        if not text:
            return
        if len(text) > _MAX_LOG_TEXT:
            text = text[: _MAX_LOG_TEXT - 1] + "…"
        with self._lock:
            sequence = self._sequences.get(job_id, 0)
            self._sequences[job_id] = sequence + 1
        try:
            self.producer.send_async(
                self.settings.logs_topic,
                job_id,
                JobLog(
                    job_id=job_id,
                    sequence=sequence,
                    kind=kind,
                    text=text,
                ),
            )
        except Exception:
            logger.warning("Could not publish a log record for job %s", job_id)
