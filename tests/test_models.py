import json

import pytest
from pydantic import ValidationError

from kafkallm.models import Job, JobState, to_json_bytes


def test_job_defaults_and_serialization() -> None:
    job = Job(instruction="Hello")

    assert job.type == "llm_task"
    assert job.job_id != job.run_id
    assert json.loads(to_json_bytes(job))["instruction"] == "Hello"


def test_job_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        Job.model_validate({"instruction": "Hello", "unexpected": True})


@pytest.mark.parametrize("run_id", ["../escape", "/tmp/escape", "a/b"])
def test_job_rejects_unsafe_run_ids(run_id: str) -> None:
    with pytest.raises(ValidationError):
        Job.model_validate({"instruction": "Hello", "run_id": run_id})


def test_job_rejects_overlong_trace_id() -> None:
    with pytest.raises(ValidationError):
        Job.model_validate({"instruction": "Hello", "trace_id": "x" * 129})


def test_job_state_is_the_gui_projection() -> None:
    job = Job(instruction="Hello", trace_id="trace-1")
    state = JobState.queued(job, worker_id="worker-1")

    assert state.job_id == job.job_id
    assert state.status == "queued"
    assert state.worker_id == "worker-1"
    assert state.submitted_at is not None
    assert state.started_at is None
