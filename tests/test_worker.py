from dataclasses import replace

from hermes.settings import Settings
from hermes.worker import Worker


def test_invalid_job_result_preserves_payload_trace_id() -> None:
    worker = object.__new__(Worker)
    worker.settings = replace(Settings.from_env(), worker_id="test-worker")

    result = worker._invalid_job_result(
        {"trace_id": "payload-trace"}, "Invalid job"
    )

    assert result.trace_id == "payload-trace"
    assert result.status == "failed"
