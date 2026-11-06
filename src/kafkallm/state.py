from __future__ import annotations

import logging

from kafkallm.kafka import KafkaProducer
from kafkallm.models import Job, JobResult, JobState, utc_now
from kafkallm.settings import Settings

logger = logging.getLogger(__name__)


class StatePublisher:
    """Publish the materialized job state to a compacted Kafka topic.

    The state topic is the GUI's database. Every transition is keyed by
    `job_id`, so compaction keeps only the latest state per job.
    """

    def __init__(self, settings: Settings, producer: KafkaProducer) -> None:
        self.settings = settings
        self.producer = producer

    def queued(self, job: Job, worker_id: str | None = None) -> None:
        self._send(JobState.queued(job, worker_id=worker_id))

    def running(self, job: Job, worker_id: str) -> JobState:
        state = JobState.queued(job, worker_id=worker_id).model_copy(
            update={"status": "running", "started_at": utc_now()}
        )
        self._send(state)
        return state

    def completed(self, state: JobState, result: JobResult) -> None:
        self._send(
            state.model_copy(
                update={
                    "status": "succeeded" if result.status == "succeeded" else "failed",
                    "cancelled": result.cancelled,
                    "finished_at": result.finished_at,
                    "worker_id": result.worker_id,
                    "result": result.result,
                    "error": result.error,
                    "artifacts": result.artifacts,
                    "artifact_error": result.artifact_error,
                    "steps": result.steps,
                    "usage": result.usage,
                }
            )
        )

    def invalid(self, result: JobResult) -> None:
        self._send(
            JobState(
                job_id=result.job_id,
                run_id=result.run_id,
                instruction="",
                status="invalid",
                trace_id=result.trace_id,
                submitted_at=result.started_at,
                finished_at=result.finished_at,
                worker_id=result.worker_id,
                error=result.error,
            )
        )

    def _send(self, state: JobState) -> None:
        try:
            self.producer.send(
                topic=self.settings.state_topic,
                key=state.job_id,
                model=state,
            )
        except Exception:
            logger.exception("Could not publish state for job %s", state.job_id)
