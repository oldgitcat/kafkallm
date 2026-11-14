from __future__ import annotations

import json
import logging
import re
import signal
import threading
import time
from dataclasses import replace
from pathlib import Path
from types import FrameType

from confluent_kafka import Consumer, KafkaError, KafkaException, Message
from gaensestall.core.models import ID_PATTERN, Job, JobControl, JobResult, JobState
from gaensestall.core.settings import Settings
from gaensestall.core.telemetry import (
    current_trace_id,
    extract_trace_context,
    get_tracer,
    inject_trace_headers,
    new_trace_id,
    set_content,
    set_session,
)
from gaensestall.kafka.client import KafkaProducer, wait_for_kafka
from gaensestall.kafka.consumer import read_compacted
from gaensestall.kafka.job_submission import job_from_state, submit_job
from gaensestall.kafka.logs import LogPublisher
from gaensestall.kafka.state import StatePublisher
from gaensestall.sandbox.job_runtime import WorkerShutdown
from gaensestall.sandbox.registry import SandboxStore
from gaensestall.worker.audit import AuditLogger
from gaensestall.worker.controls import ControlStore
from gaensestall.worker.job_runner import Harness
from opentelemetry.trace import Status, StatusCode
from pydantic import ValidationError

logger = logging.getLogger(__name__)

# Written once the runtime is ready; the Compose healthcheck looks for it.
WORKER_READY_FILE = Path("/tmp/gaensestall-worker-ready")


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
        self.controls = ControlStore(settings, on_retry=self._retry_job)
        self.controls.start()
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

    def _heartbeat(self, job_id: str, run_id: str) -> bool:
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
        return self.controls.is_cancelled(job_id, run_id)

    def _retry_job(self, control: JobControl) -> None:
        """Create a fresh run from the stored state of a finished job."""
        if not self.running:
            return
        states = read_compacted(
            self.settings,
            self.settings.state_topic,
            JobState.model_validate_json,
            timeout=3.0,
        )
        state = states.get(control.job_id)
        if state is None:
            logger.warning("Retry requested for unknown job %s", control.job_id)
            return
        if control.run_id is not None and state.run_id != control.run_id:
            logger.warning(
                "Retry requested for stale run %s of job %s (current run %s)",
                control.run_id,
                control.job_id,
                state.run_id,
            )
            return
        if state.status in {"queued", "running"}:
            logger.warning(
                "Retry requested while job %s is still %s",
                control.job_id,
                state.status,
            )
            return
        job = job_from_state(
            self.settings,
            state,
            job_id=control.new_job_id,
            run_id=control.new_run_id,
        )
        submit_job(self.settings, self.producer, job)
        logger.info(
            "Retry requested for job %s: queued new job %s / run %s",
            control.job_id,
            job.job_id,
            job.run_id,
        )

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
                span.set_attribute("gaensestall.trace_id", trace_id)
                set_session(span, job.job_id)
                set_content(span, "input.value", job.instruction)

                state, _ = self.state.running(job, worker_id=self.settings.worker_id)
                job_audit = self.audit.for_job(job)
                job_audit.emit(
                    "job.received",
                    attributes={
                        "input_files": len(job.input_files),
                        "timeout_seconds": job.timeout_seconds,
                    },
                )
                logger.info("Starting job %s / run %s", job.job_id, job.run_id)
                result = self.harness.process(
                    job,
                    job_audit=job_audit,
                    heartbeat=lambda: self._heartbeat(job.job_id, job.run_id),
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
            self.controls.stop()
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
                name=f"gaensestall-worker-{index}",
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
    pool = WorkerPool(settings)
    WORKER_READY_FILE.touch()
    try:
        pool.run()
    finally:
        WORKER_READY_FILE.unlink(missing_ok=True)
