from dataclasses import replace

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

    worker._heartbeat()

    assert worker._pending_messages == [message]
    assert worker.consumer.polls == 1
