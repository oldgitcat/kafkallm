from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class FileRef(BaseModel):
    model_config = ConfigDict(extra="forbid")

    bucket: str = Field(min_length=1)
    key: str = Field(min_length=1)


class Job(BaseModel):
    model_config = ConfigDict(extra="forbid")

    job_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()), pattern=ID_PATTERN
    )
    run_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()), pattern=ID_PATTERN
    )
    type: Literal["llm_task"] = "llm_task"
    trace_id: str | None = Field(default=None, min_length=1, max_length=128)
    instruction: str = Field(min_length=1, max_length=100_000)
    input: dict = Field(default_factory=dict)
    input_files: list[FileRef] = Field(default_factory=list, max_length=32)
    timeout_seconds: int = Field(default=300, ge=1, le=3600)
    max_steps: int = Field(default=6, ge=1, le=50)


class JobResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    job_id: str = Field(pattern=ID_PATTERN)
    run_id: str = Field(pattern=ID_PATTERN)
    status: Literal["succeeded", "failed"]
    trace_id: str | None = Field(default=None, min_length=1, max_length=128)
    result: str | None = None
    artifacts: list[FileRef] = Field(default_factory=list)
    artifact_error: str | None = None
    error: str | None = None
    worker_id: str
    started_at: datetime
    finished_at: datetime
    steps: int = 0
    usage: dict[str, int] = Field(default_factory=dict)

    @classmethod
    def failure(cls, error: str, worker_id: str) -> "JobResult":
        now = utc_now()
        return cls(
            job_id=str(uuid.uuid4()),
            run_id=str(uuid.uuid4()),
            status="failed",
            error=error[:4000],
            worker_id=worker_id,
            started_at=now,
            finished_at=now,
        )


class AgentEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    trace_id: str = Field(min_length=1, max_length=128)
    job_id: str = Field(pattern=ID_PATTERN)
    run_id: str = Field(pattern=ID_PATTERN)
    parent_event_id: str | None = None
    type: str = Field(min_length=1)
    agent: str = Field(min_length=1)
    status: Literal["info", "succeeded", "failed"]
    timestamp: datetime = Field(default_factory=utc_now)
    duration_ms: float | None = Field(default=None, ge=0)
    attributes: dict = Field(default_factory=dict)
    error: str | None = None


def to_json_bytes(model: BaseModel) -> bytes:
    return model.model_dump_json().encode("utf-8")
