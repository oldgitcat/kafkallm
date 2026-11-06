from dataclasses import replace

import pytest

from kafkallm.goose_job_runtime import WorkerShutdown
from kafkallm.settings import Settings
from kafkallm.worker import Worker


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


class FakeCancelStore:
    def __init__(self, cancelled: bool = False) -> None:
        self.cancelled = cancelled

    def is_cancelled(self, job_id: str) -> bool:
        del job_id
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
    worker.cancels = FakeCancelStore()

    assert worker._heartbeat("job-1") is False
    assert worker._pending_messages == [message]
    assert worker.consumer.polls == 1


def test_heartbeat_reports_cancellation() -> None:
    worker = object.__new__(Worker)
    worker.running = True
    worker.consumer = FakeConsumer(None)
    worker._pending_messages = []
    worker.cancels = FakeCancelStore(cancelled=True)

    assert worker._heartbeat("job-1") is True


def test_heartbeat_reports_worker_shutdown() -> None:
    worker = object.__new__(Worker)
    worker.running = False
    worker.consumer = FakeConsumer(None)
    worker._pending_messages = []
    worker.cancels = FakeCancelStore()

    with pytest.raises(WorkerShutdown, match="shutting down"):
        worker._heartbeat("job-1")
