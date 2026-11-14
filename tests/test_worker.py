from dataclasses import replace

import pytest

from kafkallm.core.models import Job, JobControl, JobState
from kafkallm.core.settings import Settings
from kafkallm.sandbox.runtime import WorkerShutdown
from kafkallm.worker.worker import Worker


class FakeMessage:
    def error(self):
        return None


class FakeConsumer:
    def __init__(self, message):
        self.message = message
        self.polls = 0

    def poll(self, timeout):
        del timeout
        self.polls += 1
        return self.message


class FakeControlStore:
    def __init__(self, cancelled: bool = False) -> None:
        self.cancelled = cancelled

    def is_cancelled(self, job_id: str, run_id: str | None = None) -> bool:
        del job_id, run_id
        return self.cancelled


def test_invalid_job_result_preserves_payload_trace_id() -> None:
    worker = object.__new__(Worker)
    worker.settings = replace(Settings.from_env(), worker_id="test-worker")

    result = worker._invalid_job_result({"trace_id": "payload-trace"}, "Invalid job")

    assert result.trace_id == "payload-trace"
    assert result.status == "failed"


def test_heartbeat_buffers_unexpected_messages() -> None:
    worker = object.__new__(Worker)
    worker.running = True
    message = FakeMessage()
    worker.consumer = FakeConsumer(message)
    worker._pending_messages = []
    worker.controls = FakeControlStore()

    assert worker._heartbeat("job-1", "run-1") is False
    assert worker._pending_messages == [message]
    assert worker.consumer.polls == 1


def test_heartbeat_reports_cancellation() -> None:
    worker = object.__new__(Worker)
    worker.running = True
    worker.consumer = FakeConsumer(None)
    worker._pending_messages = []
    worker.controls = FakeControlStore(cancelled=True)

    assert worker._heartbeat("job-1", "run-1") is True


def test_heartbeat_reports_worker_shutdown() -> None:
    worker = object.__new__(Worker)
    worker.running = False
    worker.consumer = FakeConsumer(None)
    worker._pending_messages = []
    worker.controls = FakeControlStore()

    with pytest.raises(WorkerShutdown, match="shutting down"):
        worker._heartbeat("job-1", "run-1")


def test_retry_job_submits_a_new_run(monkeypatch) -> None:
    worker = object.__new__(Worker)
    worker.settings = replace(Settings.from_env(), worker_id="test-worker")
    worker.running = True
    worker.producer = object()
    state = JobState.queued(
        Job(job_id="job-1", run_id="run-1", instruction="Retry me")
    ).model_copy(update={"status": "succeeded"})
    monkeypatch.setattr(
        "kafkallm.worker.worker.read_compacted",
        lambda *args, **kwargs: {"job-1": state},
    )
    submitted: list[Job] = []
    monkeypatch.setattr(
        "kafkallm.worker.worker.submit_job",
        lambda settings, producer, job: submitted.append(job),
    )

    worker._retry_job(
        JobControl(
            action="retry",
            job_id="job-1",
            run_id="run-1",
            new_job_id="job-2",
            new_run_id="run-2",
        )
    )

    assert len(submitted) == 1
    assert submitted[0].job_id == "job-2"
    assert submitted[0].run_id == "run-2"
    assert submitted[0].instruction == "Retry me"
