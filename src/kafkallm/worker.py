from __future__ import annotations

import json
import logging
import re
import signal
import time
from types import FrameType

from confluent_kafka import Consumer, KafkaError, KafkaException, Message
from opentelemetry.trace import Status, StatusCode
from pydantic import ValidationError

from kafkallm.audit import AuditLogger
from kafkallm.harness import Harness
from kafkallm.kafka import KafkaProducer, wait_for_kafka
from kafkallm.llm import build_llm
from kafkallm.models import ID_PATTERN, Job, JobResult
from kafkallm.settings import Settings
from kafkallm.telemetry import (
    current_trace_id,
    extract_trace_context,
    get_tracer,
    inject_trace_headers,
    new_trace_id,
    set_content,
)


logger = logging.getLogger(__name__)


class Worker:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.running = True
        logger.info(
            "Warte bis zu %ss auf Redpanda unter %s",
            settings.kafka_startup_timeout_seconds,
            settings.kafka_bootstrap_servers,
        )
        wait_for_kafka(settings)
        self.producer = KafkaProducer(settings)
        self.audit = AuditLogger(settings, self.producer)
        self.harness = Harness(settings, build_llm(settings), audit=self.audit)
        self.consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "group.id": settings.kafka_group_id,
                "client.id": settings.worker_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "session.timeout.ms": 15000,
                "heartbeat.interval.ms": 3000,
                "partition.assignment.strategy": "cooperative-sticky",
                "max.poll.interval.ms": (
                    settings.max_job_timeout_seconds + 120
                )
                * 1000,
            }
        )

    def stop(self, signum: int, frame: FrameType | None) -> None:
        del frame
        logger.info("Signal %s empfangen; Worker wird beendet", signum)
        self.running = False

    def _invalid_job_result(self, raw: object, error: str) -> JobResult:
        result = JobResult.failure(error, self.settings.worker_id)
        updates: dict[str, str] = {"trace_id": current_trace_id() or new_trace_id()}
        if isinstance(raw, dict):
            for field in ("job_id", "run_id"):
                value = raw.get(field)
                if isinstance(value, str) and re.fullmatch(ID_PATTERN, value):
                    updates[field] = value
            trace_id = raw.get("trace_id")
            if isinstance(trace_id, str) and 0 < len(trace_id) <= 128:
                updates["trace_id"] = trace_id
        return result.model_copy(update=updates)

    def handle_message(self, message: Message) -> None:
        parent_context = extract_trace_context(message.headers())
        with get_tracer().start_as_current_span(
            "job.handle",
            context=parent_context,
            attributes={
                "openinference.span.kind": "CHAIN",
                "messaging.system": "kafka",
                "messaging.destination": message.topic(),
                "messaging.kafka.partition": message.partition(),
                "messaging.kafka.offset": message.offset(),
                "worker.id": self.settings.worker_id,
            },
        ) as span:
            handle_started = time.perf_counter()
            raw: object = None
            try:
                value = message.value()
                if value is None:
                    raise ValueError("Leere Kafka-Nachrichten sind keine gültigen Jobs.")
                raw = json.loads(value.decode("utf-8"))
                job = Job.model_validate(raw)
                trace_id = job.trace_id or current_trace_id() or new_trace_id()
                job = job.model_copy(update={"trace_id": trace_id})
                span.set_attribute("job.id", job.job_id)
                span.set_attribute("job.run_id", job.run_id)
                span.set_attribute("kafkallm.trace_id", trace_id)
                set_content(span, "input.value", job.instruction)

                job_audit = self.audit.for_job(job)
                job_audit.emit(
                    "job.received",
                    attributes={
                        "input_files": len(job.input_files),
                        "timeout_seconds": job.timeout_seconds,
                        "max_steps": job.max_steps,
                    },
                )
                logger.info("Starte Job %s / Run %s", job.job_id, job.run_id)
                result = self.harness.process(job, job_audit=job_audit)
                job_audit.emit(
                    "job.completed",
                    result.status,
                    duration_ms=round((time.perf_counter() - handle_started) * 1000, 3),
                    attributes={
                        "steps": result.steps,
                        "usage": result.usage,
                        "artifact_count": len(result.artifacts),
                        "artifact_error": result.artifact_error,
                    },
                    error=result.error,
                )
            except (
                json.JSONDecodeError,
                UnicodeDecodeError,
                ValidationError,
                ValueError,
            ) as exc:
                logger.warning("Ungültige Job-Nachricht: %s", exc)
                result = self._invalid_job_result(raw, f"Ungültiger Job: {exc}")

            span.set_attribute("job.status", result.status)
            set_content(span, "output.value", result.result)
            if result.status == "failed":
                span.set_status(Status(StatusCode.ERROR, result.error or "Job failed"))

            self.producer.send(
                topic=self.settings.results_topic,
                key=result.job_id,
                model=result,
                headers=inject_trace_headers(),
            )
            self.consumer.commit(message=message, asynchronous=False)
            logger.info(
                "Beendet: Job %s / Run %s mit Status %s",
                result.job_id,
                result.run_id,
                result.status,
            )

    def run(self) -> None:
        signal.signal(signal.SIGTERM, self.stop)
        signal.signal(signal.SIGINT, self.stop)
        self.consumer.subscribe([self.settings.jobs_topic])
        logger.info(
            "Worker %s wartet auf Jobs in %s",
            self.settings.worker_id,
            self.settings.jobs_topic,
        )
        try:
            while self.running:
                message = self.consumer.poll(timeout=1.0)
                if message is None:
                    continue
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(message.error())
                self.handle_message(message)
        finally:
            self.consumer.close()
            self.producer.close()
            logger.info("Worker beendet")
