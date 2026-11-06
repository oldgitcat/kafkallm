from __future__ import annotations

import re
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$"

_ENV_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_EXTENSION_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")
_RESERVED_ENV_NAMES = {
    "BASH_ENV",
    "ENV",
    "HOME",
    "LD_LIBRARY_PATH",
    "LD_PRELOAD",
    "NODE_OPTIONS",
    "PATH",
    "PYTHONHOME",
    "PYTHONPATH",
    "SHELL",
    "TMPDIR",
    "USER",
}
_RESERVED_ENV_PREFIXES = ("GOOSE_", "KAFKALLM_", "OPENAI_")


def utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(slots=True)
class AgentResult:
    text: str
    steps: int
    usage: dict[str, int] = field(default_factory=dict)


class FileRef(BaseModel):
    model_config = ConfigDict(extra="forbid")

    bucket: str = Field(min_length=1)
    key: str = Field(min_length=1)


class AgentSpec(BaseModel):
    """A reusable goosetown role that the orchestrator can delegate to."""

    model_config = ConfigDict(extra="forbid")

    agent_id: str = Field(pattern=ID_PATTERN)
    name: str = Field(min_length=1, max_length=80)
    role: str = Field(default="custom", min_length=1, max_length=40)
    description: str = Field(default="", max_length=4_000)
    instructions: str = Field(default="", max_length=20_000)
    skills: list[str] = Field(default_factory=list, max_length=20)
    enabled: bool = True
    updated_at: datetime = Field(default_factory=utc_now)


class SandboxSpec(BaseModel):
    """A selectable job environment backed by an OCI image.

    The registry is stored in a compacted Kafka topic and managed by the GUI.
    Operators can add new environments without changing the worker image or
    redeploying the control plane.
    """

    model_config = ConfigDict(extra="forbid")

    sandbox_id: str = Field(pattern=ID_PATTERN)
    name: str = Field(min_length=1, max_length=80)
    description: str = Field(default="", max_length=4_000)
    image: str = Field(min_length=1, max_length=255, pattern=r"^\S+$")
    instructions: str = Field(default="", max_length=20_000)
    env: dict[str, str] = Field(default_factory=dict, max_length=64)
    extensions: list[str] = Field(default_factory=list, max_length=20)
    enabled: bool = True
    updated_at: datetime = Field(default_factory=utc_now)

    @field_validator("env")
    @classmethod
    def _validate_env(cls, value: dict[str, str]) -> dict[str, str]:
        for key, item in value.items():
            if not _ENV_NAME_PATTERN.fullmatch(key):
                raise ValueError(f"Invalid environment variable name: {key!r}")
            if key in _RESERVED_ENV_NAMES or key.startswith(_RESERVED_ENV_PREFIXES):
                raise ValueError(f"Environment variable {key!r} is reserved.")
            if len(item) > 8_000:
                raise ValueError(f"Environment variable {key!r} is too long.")
        return value

    @field_validator("extensions")
    @classmethod
    def _validate_extensions(cls, value: list[str]) -> list[str]:
        for extension in value:
            name, separator, path = extension.partition(":")
            if not separator or not _EXTENSION_NAME_PATTERN.fullmatch(name):
                raise ValueError(f"Invalid goose extension: {extension!r}")
            if not path.startswith("/") or any(char in path for char in ", \t\r\n"):
                raise ValueError(f"Invalid goose extension path: {extension!r}")
        return value


class JobCancel(BaseModel):
    """A request to stop a queued or running job."""

    model_config = ConfigDict(extra="forbid")

    job_id: str = Field(pattern=ID_PATTERN)
    requested_at: datetime = Field(default_factory=utc_now)


class Job(BaseModel):
    model_config = ConfigDict(extra="forbid")

    job_id: str = Field(default_factory=lambda: str(uuid.uuid4()), pattern=ID_PATTERN)
    run_id: str = Field(default_factory=lambda: str(uuid.uuid4()), pattern=ID_PATTERN)
    type: Literal["llm_task"] = "llm_task"
    trace_id: str | None = Field(default=None, min_length=1, max_length=128)
    instruction: str = Field(min_length=1, max_length=100_000)
    input: dict = Field(default_factory=dict)
    input_files: list[FileRef] = Field(default_factory=list, max_length=32)
    sandbox: str = Field(default="default", pattern=ID_PATTERN)
    multi_agent: bool = True
    agents: list[AgentSpec] = Field(default_factory=list, max_length=20)
    unlimited: bool = False
    timeout_seconds: int = Field(default=300, ge=1, le=3600)
    max_steps: int = Field(default=6, ge=1, le=50)


class JobResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    job_id: str = Field(pattern=ID_PATTERN)
    run_id: str = Field(pattern=ID_PATTERN)
    status: Literal["succeeded", "failed"]
    trace_id: str | None = Field(default=None, min_length=1, max_length=128)
    cancelled: bool = False
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
    def failure(cls, error: str, worker_id: str) -> JobResult:
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


class JobState(BaseModel):
    """Materialized job state published to the compacted state topic.

    Kafka is the only store. The GUI rebuilds its view by replaying this topic
    from the beginning, so the payload carries everything the UI needs.
    """

    model_config = ConfigDict(extra="forbid")

    job_id: str = Field(pattern=ID_PATTERN)
    run_id: str = Field(pattern=ID_PATTERN)
    instruction: str
    status: Literal["queued", "running", "succeeded", "failed", "invalid"]
    trace_id: str | None = Field(default=None, min_length=1, max_length=128)
    cancelled: bool = False
    input_files: list[FileRef] = Field(default_factory=list, max_length=32)
    sandbox: str = Field(default="default", pattern=ID_PATTERN)
    multi_agent: bool = True
    agent_names: list[str] = Field(default_factory=list, max_length=20)
    unlimited: bool = False
    timeout_seconds: int = Field(default=300, ge=1, le=3600)
    max_steps: int = Field(default=6, ge=1, le=50)
    submitted_at: datetime = Field(default_factory=utc_now)
    started_at: datetime | None = None
    finished_at: datetime | None = None
    worker_id: str | None = None
    result: str | None = None
    error: str | None = None
    artifacts: list[FileRef] = Field(default_factory=list)
    artifact_error: str | None = None
    steps: int = 0
    usage: dict[str, int] = Field(default_factory=dict)

    @classmethod
    def queued(cls, job: Job, worker_id: str | None = None) -> JobState:
        return cls(
            job_id=job.job_id,
            run_id=job.run_id,
            instruction=job.instruction,
            status="queued",
            trace_id=job.trace_id,
            input_files=job.input_files,
            sandbox=job.sandbox,
            multi_agent=job.multi_agent,
            agent_names=[agent.name for agent in job.agents] if job.multi_agent else [],
            unlimited=job.unlimited,
            timeout_seconds=job.timeout_seconds,
            max_steps=job.max_steps,
            worker_id=worker_id,
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
