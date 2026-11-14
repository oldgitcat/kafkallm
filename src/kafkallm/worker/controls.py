from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from typing import Any

from kafkallm.core.models import JobControl
from kafkallm.core.settings import Settings
from kafkallm.kafka.consumer import decode_json_value, iter_messages

logger = logging.getLogger(__name__)

_MAX_SEEN_REQUESTS = 1000


class ControlStore:
    """Consume worker control requests from Kafka.

    Cancellation is broadcast: each worker uses a private consumer group and
    marks the matching run so its heartbeat can stop it. Retry uses one shared
    consumer group, so exactly one worker handles a retry request.
    """

    def __init__(
        self,
        settings: Settings,
        *,
        on_retry: Callable[[JobControl], None] | None = None,
    ) -> None:
        self.settings = settings
        self.on_retry = on_retry
        self._cancelled: set[tuple[str, str]] = set()
        self._cancelled_jobs: set[str] = set()
        self._seen_requests: set[str] = set()
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []

    def start(self) -> None:
        self._threads = [
            threading.Thread(
                target=self._consume,
                kwargs={
                    "name": "kafkallm-controls-cancel",
                    "group": "kafkallm-controls-cancel",
                    "unique_group": True,
                    "actions": {"cancel"},
                },
                daemon=True,
            ),
            threading.Thread(
                target=self._consume,
                kwargs={
                    "name": "kafkallm-controls-retry",
                    "group": "kafkallm-controls-retry",
                    "unique_group": False,
                    "actions": {"retry"},
                },
                daemon=True,
            ),
        ]
        for thread in self._threads:
            thread.start()

    def stop(self) -> None:
        self._stop.set()
        for thread in self._threads:
            thread.join(timeout=5)

    def is_cancelled(self, job_id: str, run_id: str | None = None) -> bool:
        with self._lock:
            if job_id in self._cancelled_jobs:
                return True
            return run_id is not None and (job_id, run_id) in self._cancelled

    def add_cancel(self, job_id: str, run_id: str | None = None) -> None:
        with self._lock:
            if run_id is None:
                self._cancelled_jobs.add(job_id)
            else:
                self._cancelled.add((job_id, run_id))

    def _consume(
        self,
        *,
        name: str,
        group: str,
        unique_group: bool,
        actions: set[str],
    ) -> None:
        for message in iter_messages(
            self.settings,
            [self.settings.control_topic],
            group,
            self._stop,
            unique_group=unique_group,
            offset_reset="latest",
        ):
            payload: Any = decode_json_value(message)
            if payload is None:
                continue
            try:
                control = JobControl.model_validate(payload)
            except ValueError:
                logger.warning("Skipping malformed control record")
                continue
            if control.action not in actions:
                continue
            if not self._mark_seen(control.request_id):
                continue
            if control.action == "cancel":
                self.add_cancel(control.job_id, control.run_id)
                logger.info(
                    "Cancellation requested for job %s / run %s",
                    control.job_id,
                    control.run_id or "*",
                )
            elif control.action == "retry":
                if self.on_retry is None:
                    logger.warning("Retry request received without a handler")
                    continue
                try:
                    self.on_retry(control)
                except Exception:
                    logger.exception(
                        "Retry request failed for job %s / run %s",
                        control.job_id,
                        control.run_id or "*",
                    )

    def _mark_seen(self, request_id: str) -> bool:
        with self._lock:
            if request_id in self._seen_requests:
                return False
            self._seen_requests.add(request_id)
            if len(self._seen_requests) > _MAX_SEEN_REQUESTS:
                self._seen_requests.pop()
            return True
