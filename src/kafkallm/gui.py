from __future__ import annotations

import json
import logging
import signal
import threading
import uuid
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException
from pydantic import ValidationError

from kafkallm.kafka import KafkaProducer, ensure_topics, wait_for_kafka
from kafkallm.models import Job
from kafkallm.settings import Settings
from kafkallm.state import StatePublisher
from kafkallm.telemetry import (
    current_trace_id,
    inject_trace_headers,
    new_trace_id,
)


logger = logging.getLogger(__name__)

_MAX_EVENTS_PER_JOB = 200


class JobStore:
    """In-memory projection of the compacted state topic and the event stream.

    Kafka is the database. This projection is rebuilt from the beginning of the
    topics whenever the GUI restarts, so there is no local state to back up.
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._states: dict[str, dict[str, Any]] = {}
        self._events: dict[str, list[dict[str, Any]]] = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []

    def start(self) -> None:
        for target, name in (
            (self._consume_states, "kafkallm-gui-state"),
            (self._consume_events, "kafkallm-gui-events"),
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

    def _consume_states(self) -> None:
        self._consume_topic(self.settings.state_topic, self.put_state)

    def _consume_events(self) -> None:
        self._consume_topic(self.settings.events_topic, self.put_event)

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


class JobServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self,
        address: tuple[str, int],
        store: JobStore,
        producer: KafkaProducer,
        settings: Settings,
    ) -> None:
        super().__init__(address, JobHandler)
        self.store = store
        self.producer = producer
        self.settings = settings
        self.producer_lock = threading.Lock()


class JobHandler(BaseHTTPRequestHandler):
    server: JobServer

    def log_message(self, format: str, *args: Any) -> None:
        logger.info("%s - %s", self.address_string(), format % args)

    def do_GET(self) -> None:  # noqa: N802
        if self.path in {"/", "/index.html"}:
            self._send_file()
        elif self.path == "/healthz":
            self._send_json({"status": "ok"})
        elif self.path == "/api/jobs":
            self._send_json({"jobs": self.server.store.jobs()})
        elif self.path.startswith("/api/jobs/"):
            job_id = self.path.rsplit("/", 1)[-1]
            job = self.server.store.job(job_id)
            if job is None:
                self._send_json(
                    {"error": "job not found"}, status=HTTPStatus.NOT_FOUND
                )
            else:
                self._send_json(job)
        else:
            self._send_json(
                {"error": "not found"}, status=HTTPStatus.NOT_FOUND
            )

    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/api/jobs":
            self._send_json(
                {"error": "not found"}, status=HTTPStatus.NOT_FOUND
            )
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(length) or b"{}")
            if not isinstance(payload, dict):
                raise ValueError("The request body must be a JSON object.")
            job = Job.model_validate(
                {
                    **payload,
                    "trace_id": payload.get("trace_id")
                    or current_trace_id()
                    or new_trace_id(),
                }
            )
        except (ValueError, ValidationError) as exc:
            self._send_json(
                {"error": f"Invalid job: {exc}"},
                status=HTTPStatus.BAD_REQUEST,
            )
            return

        try:
            with self.server.producer_lock:
                self.server.producer.send(
                    self.server.settings.jobs_topic,
                    job.job_id,
                    job,
                    headers=inject_trace_headers(),
                )
                StatePublisher(self.server.settings, self.server.producer).queued(
                    job, worker_id=None
                )
        except Exception as exc:
            logger.exception("Could not submit job")
            self._send_json(
                {"error": f"Could not submit job: {exc}"},
                status=HTTPStatus.SERVICE_UNAVAILABLE,
            )
            return
        self._send_json(job.model_dump(mode="json"), status=HTTPStatus.CREATED)

    def _send_file(self) -> None:
        path = Path(__file__).parent / "static" / "index.html"
        try:
            body = path.read_bytes()
        except OSError:
            self._send_json(
                {"error": "GUI assets are missing"},
                status=HTTPStatus.INTERNAL_SERVER_ERROR,
            )
            return
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_json(
        self,
        payload: Any,
        status: HTTPStatus = HTTPStatus.OK,
    ) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)


def run_gui(settings: Settings) -> None:
    wait_for_kafka(settings)
    ensure_topics(settings)
    store = JobStore(settings)
    store.start()
    producer = KafkaProducer(settings)
    server = JobServer(
        (settings.gui_host, settings.gui_port), store, producer, settings
    )

    def shutdown(signum: int, frame: Any) -> None:
        del frame
        logger.info("Received signal %s; shutting down GUI", signum)
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    logger.info(
        "Kafkallm GUI listening on http://%s:%s",
        settings.gui_host,
        settings.gui_port,
    )
    try:
        server.serve_forever(poll_interval=0.5)
    finally:
        server.server_close()
        store.stop()
        producer.close()
