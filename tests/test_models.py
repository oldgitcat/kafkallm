import json

import pytest
from pydantic import ValidationError

from kafkallm.models import Job, to_json_bytes


def test_job_defaults_and_serialization() -> None:
    job = Job(instruction="Hallo")

    assert job.type == "llm_task"
    assert job.job_id != job.run_id
    assert json.loads(to_json_bytes(job))["instruction"] == "Hallo"


def test_job_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        Job.model_validate({"instruction": "Hallo", "unexpected": True})


@pytest.mark.parametrize("run_id", ["../escape", "/tmp/escape", "a/b"])
def test_job_rejects_unsafe_run_ids(run_id: str) -> None:
    with pytest.raises(ValidationError):
        Job.model_validate({"instruction": "Hallo", "run_id": run_id})


def test_job_rejects_overlong_trace_id() -> None:
    with pytest.raises(ValidationError):
        Job.model_validate({"instruction": "Hallo", "trace_id": "x" * 129})
