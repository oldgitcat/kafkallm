from __future__ import annotations

import threading
from typing import Any

from kafkallm.kafka_consumer import decode_json_value, iter_messages
from kafkallm.settings import Settings

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

    def remove_job(self, job_id: str) -> None:
        """Forget a job and its events and logs after its state was deleted."""
        with self._lock:
            self._states.pop(job_id, None)
            self._events.pop(job_id, None)
            self._logs.pop(job_id, None)

    def _consume_states(self) -> None:
        self._consume_topic(self.settings.state_topic, self.put_state)

    def _consume_events(self) -> None:
        self._consume_topic(self.settings.events_topic, self.put_event)

    def _consume_logs(self) -> None:
        self._consume_topic(self.settings.logs_topic, self.put_log)

    def _consume_topic(self, topic: str, handler: Any) -> None:
        for message in iter_messages(
            self.settings,
            [topic],
            f"kafkallm-gui-{topic}",
            self._stop,
        ):
            payload = decode_json_value(message)
            if isinstance(payload, dict):
                handler(payload)
