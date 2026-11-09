from __future__ import annotations

import json
import logging
import re
import signal
import threading
import time
from dataclasses import replace
from types import FrameType

from confluent_kafka import Consumer, KafkaError, KafkaException, Message
from opentelemetry.trace import Status, StatusCode
from pydantic import ValidationError

from kafkallm.audit import AuditLogger
from kafkallm.cancels import CancelStore
from kafkallm.goose_job_runtime import WorkerShutdown
from kafkallm.harness import Harness
from kafkallm.kafka import KafkaProducer, wait_for_kafka
from kafkallm.logs import LogPublisher
from kafkallm.models import ID_PATTERN, Job, JobResult
from kafkallm.sandboxes import SandboxStore
from kafkallm.settings import Settings
from kafkallm.state import StatePublisher
from kafkallm.telemetry import (
    current_trace_id,
    extract_trace_context,
    get_tracer,
    inject_trace_headers,
    new_trace_id,
    set_content,
    set_session,
)

logger = logging.getLogger(__name__)


class Worker:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.running = True
        self._pending_messages: list[Message] = []
        logger.info(
            "Waiting up to %ss for Kafka at %s",
            settings.kafka_startup_timeout_seconds,
            settings.kafka_bootstrap_servers,
        )
        wait_for_kafka(settings)
        self.producer = KafkaProducer(settings)
        self.audit = AuditLogger(settings, self.producer)
        self.state = StatePublisher(settings, self.producer)
        self.logs = LogPublisher(settings, self.producer)
        self.sandboxes = SandboxStore(settings)
        self.sandboxes.start()
        self.cancels = CancelStore(settings)
        self.cancels.start()
        self.harness = Harness(settings, audit=self.audit, sandboxes=self.sandboxes)
        self.harness.wait_ready()
        max_poll_ms = (settings.max_job_timeout_seconds + 120) * 1000
        if settings.allow_unlimited_jobs:
            max_poll_ms = max(max_poll_ms, settings.unlimited_max_poll_interval_ms)
        # librdkafka rejects values above 24 hours.
        max_poll_ms = min(max_poll_ms, 86_400_000)
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
                "max.poll.interval.ms": max_poll_ms,
            }
        )

    def stop(self, signum: int, frame: FrameType | None) -> None:
        del frame
        logger.info("Received signal %s; shutting down worker", signum)
        self.running = False
        self.harness.cancel()

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

    def _heartbeat(self, job_id: str) -> bool:
        """Poll the consumer while a long job runs and report cancellation."""
        if not self.running:
            raise WorkerShutdown("Worker is shutting down; job interrupted.")
        try:
            message = self.consumer.poll(0)
        except KafkaException as exc:
            logger.warning("Kafka heartbeat poll failed: %s", exc)
        else:
            if message is not None:
                if message.error():
                    if message.error().code() != KafkaError._PARTITION_EOF:
                        logger.warning(
                            "Kafka heartbeat poll returned: %s", message.error()
                        )
                else:
                    self._pending_messages.append(message)
        return self.cancels.is_cancelled(job_id)

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
                    raise ValueError("Empty Kafka messages are not valid jobs.")
                raw = json.loads(value.decode("utf-8"))
                job = Job.model_validate(raw)
                trace_id = job.trace_id or current_trace_id() or new_trace_id()
                job = job.model_copy(update={"trace_id": trace_id})
                span.set_attribute("job.id", job.job_id)
                span.set_attribute("job.run_id", job.run_id)
                span.set_attribute("kafkallm.trace_id", trace_id)
                set_session(span, job.job_id)
                set_content(span, "input.value", job.instruction)

                state, _ = self.state.running(job, worker_id=self.settings.worker_id)
                job_audit = self.audit.for_job(job)
                job_audit.emit(
                    "job.received",
                    attributes={
                        "input_files": len(job.input_files),
                        "timeout_seconds": job.timeout_seconds,
                        "max_steps": job.max_steps,
                    },
                )
                logger.info("Starting job %s / run %s", job.job_id, job.run_id)
                result = self.harness.process(
                    job,
                    job_audit=job_audit,
                    heartbeat=lambda: self._heartbeat(job.job_id),
                    log=lambda kind, text: self.logs.publish(job.job_id, kind, text),
                )
                if not self.state.completed(state, result):
                    job_audit.emit(
                        "state.failed",
                        "failed",
                        attributes={"topic": self.settings.state_topic},
                        error="The final job state could not be published.",
                    )
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
                logger.warning("Invalid job message: %s", exc)
                result = self._invalid_job_result(raw, f"Invalid job: {exc}")
                self.state.invalid(result)

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
                "Finished: job %s / run %s with status %s",
                result.job_id,
                result.run_id,
                result.status,
            )

    def run(self, install_signals: bool = True) -> None:
        if install_signals:
            signal.signal(signal.SIGTERM, self.stop)
            signal.signal(signal.SIGINT, self.stop)
        self.consumer.subscribe([self.settings.jobs_topic])
        logger.info(
            "Worker %s is waiting for jobs in %s",
            self.settings.worker_id,
            self.settings.jobs_topic,
        )
        try:
            while self.running:
                if self._pending_messages:
                    message = self._pending_messages.pop(0)
                else:
                    message = self.consumer.poll(timeout=1.0)
                if message is None:
                    continue
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(message.error())
                assignment = self.consumer.assignment()
                if assignment:
                    self.consumer.pause(assignment)
                try:
                    self.handle_message(message)
                finally:
                    current = self.consumer.assignment()
                    if current:
                        self.consumer.resume(current)
        finally:
            self.consumer.close()
            self.cancels.stop()
            self.sandboxes.stop()
            self.producer.close()
            self.harness.close()
            logger.info("Worker stopped")


class WorkerPool:
    """Run several workers in one process so jobs can execute in parallel.

    Each worker owns its own Kafka consumer and goose runtime. The Kafka group
    distributes partitions across them; the effective parallelism is min(
    WORKER_CONCURRENCY, KAFKA_TOPIC_PARTITIONS).
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.concurrency = max(1, settings.worker_concurrency)
        self._shutting_down = False
        self.workers = [
            Worker(
                settings
                if self.concurrency == 1
                else replace(settings, worker_id=f"{settings.worker_id}-{index + 1}")
            )
            for index in range(self.concurrency)
        ]

    def run(self) -> None:
        def shutdown(signum: int, frame: FrameType | None) -> None:
            self._shutting_down = True
            logger.info(
                "Received signal %s; stopping %s workers", signum, len(self.workers)
            )
            for worker in self.workers:
                worker.stop(signum, frame)

        signal.signal(signal.SIGTERM, shutdown)
        signal.signal(signal.SIGINT, shutdown)
        threads = []
        for index, worker in enumerate(self.workers, start=1):
            thread = threading.Thread(
                target=worker.run,
                kwargs={"install_signals": False},
                name=f"kafkallm-worker-{index}",
            )
            thread.start()
            threads.append(thread)
        while any(thread.is_alive() for thread in threads):
            for thread in threads:
                thread.join(timeout=1.0)
            if any(not thread.is_alive() for thread in threads):
                # One worker stopped unexpectedly. Stop the rest and let the
                # container supervisor restart a clean pool.
                if not self._shutting_down:
                    logger.error("A worker thread stopped; shutting down the pool")
                for worker in self.workers:
                    worker.stop(signal.SIGTERM, None)
                break
        for thread in threads:
            thread.join()


def run_workers(settings: Settings) -> None:
    WorkerPool(settings).run()
