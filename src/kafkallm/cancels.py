from __future__ import annotations

import logging
import threading
from typing import Any

from kafkallm.kafka_consumer import decode_json_value, iter_messages
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
        for message in iter_messages(
            self.settings,
            [self.settings.cancels_topic],
            "kafkallm-cancels",
            self._stop,
        ):
            payload: Any = decode_json_value(message)
            if payload is None:
                continue
            try:
                cancel = JobCancel.model_validate(payload)
            except ValueError:
                logger.warning("Skipping malformed cancel record")
                continue
            self.add(cancel.job_id)
