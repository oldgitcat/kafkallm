from __future__ import annotations

import json
import logging
import threading
import uuid
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException

from kafkallm.settings import Settings

logger = logging.getLogger(__name__)

_MAX_EVENTS_PER_JOB = 200
_MAX_LOGS_PER_JOB = 3000
_MAX_LOG_JOBS = 200


class JobStore:
    """In-memory projection of the compacted state topic and the event stream.

    Kafka is the database. This projection is rebuilt from the beginning of the
    topics whenever the GUI restarts, so there is no local state to back up.
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._states: dict[str, dict[str, Any]] = {}
        self._events: dict[str, list[dict[str, Any]]] = {}
        self._logs: dict[str, list[dict[str, Any]]] = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []

    def start(self) -> None:
        for target, name in (
            (self._consume_states, "kafkallm-gui-state"),
            (self._consume_events, "kafkallm-gui-events"),
            (self._consume_logs, "kafkallm-gui-logs"),
        ):
            thread = threading.Thread(target=target, name=name, daemon=True)
            thread.start()
            self._threads.append(thread)

    def stop(self) -> None:
        self._stop.set()
        for thread in self._threads:
            thread.join(timeout=5)

    def jobs(self) -> list[dict[str, Any]]:
        with self._lock:
            states = list(self._states.values())
        states.sort(key=lambda state: state.get("submitted_at") or "", reverse=True)
        return states

    def job(self, job_id: str) -> dict[str, Any] | None:
        with self._lock:
            state = self._states.get(job_id)
            events = list(self._events.get(job_id, []))
        if state is None:
            return None
        return {**state, "events": events}

    def put_state(self, state: dict[str, Any]) -> None:
        job_id = state.get("job_id")
        if not isinstance(job_id, str):
            return
        with self._lock:
            self._states[job_id] = state

    def put_event(self, event: dict[str, Any]) -> None:
        job_id = event.get("job_id")
        if not isinstance(job_id, str):
            return
        with self._lock:
            events = self._events.setdefault(job_id, [])
            events.append(event)
            if len(events) > _MAX_EVENTS_PER_JOB:
                del events[: len(events) - _MAX_EVENTS_PER_JOB]

    def put_log(self, record: dict[str, Any]) -> None:
        job_id = record.get("job_id")
        if not isinstance(job_id, str):
            return
        with self._lock:
            if job_id not in self._logs and len(self._logs) >= _MAX_LOG_JOBS:
                self._logs.pop(next(iter(self._logs)), None)
            records = self._logs.setdefault(job_id, [])
            records.append(record)
            if len(records) > _MAX_LOGS_PER_JOB:
                del records[: len(records) - _MAX_LOGS_PER_JOB]

    def logs(self, job_id: str, after: int = 0) -> list[dict[str, Any]]:
        with self._lock:
            records = list(self._logs.get(job_id, []))
        return [
            record
            for record in records
            if isinstance(record.get("sequence"), int) and record["sequence"] > after
        ]

    def _consume_states(self) -> None:
        self._consume_topic(self.settings.state_topic, self.put_state)

    def _consume_events(self) -> None:
        self._consume_topic(self.settings.events_topic, self.put_event)

    def _consume_logs(self) -> None:
        self._consume_topic(self.settings.logs_topic, self.put_log)

    def _consume_topic(self, topic: str, handler: Any) -> None:
        while not self._stop.is_set():
            consumer = Consumer(
                {
                    "bootstrap.servers": self.settings.kafka_bootstrap_servers,
                    "group.id": f"kafkallm-gui-{topic}-{uuid.uuid4()}",
                    "auto.offset.reset": "earliest",
                    "enable.auto.commit": False,
                    "session.timeout.ms": 15000,
                }
            )
            consumer.subscribe([topic])
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
                        payload = json.loads(value.decode("utf-8"))
                    except (UnicodeDecodeError, ValueError):
                        logger.warning("Skipping malformed message in %s", topic)
                        continue
                    if isinstance(payload, dict):
                        handler(payload)
            except Exception:
                logger.exception("GUI consumer for %s failed; reconnecting", topic)
                self._stop.wait(2)
            finally:
                consumer.close()
