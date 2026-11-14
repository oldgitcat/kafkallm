"""Job preparation and submission shared by the CLI and the GUI."""

from __future__ import annotations

from typing import Any, Protocol

from gaensestall.core.models import AgentSpec, Job, JobSpec, JobState
from gaensestall.core.settings import Settings
from gaensestall.core.telemetry import (
    current_trace_id,
    inject_trace_headers,
    new_trace_id,
)
from gaensestall.kafka.client import KafkaProducer
from gaensestall.kafka.state import StatePublisher


class AgentLookup(Protocol):
    def get(self, agent_id: str) -> AgentSpec | None: ...


def select_agents(payload: dict[str, Any], agents: AgentLookup) -> None:
    """Resolve GUI ``agent_ids`` into the snapshot stored in the Job message."""
    agent_ids = payload.pop("agent_ids", None)
    multi_agent = bool(payload.get("multi_agent", True))
    if multi_agent and agent_ids is not None:
        if not isinstance(agent_ids, list):
            raise ValueError("agent_ids must be a list.")
        selected: list[AgentSpec] = []
        for raw_id in agent_ids:
            agent = agents.get(str(raw_id))
            if agent is None:
                raise ValueError(f"Unknown agent: {raw_id}")
            if agent.enabled:
                selected.append(agent)
        payload["agents"] = [agent.model_dump(mode="json") for agent in selected]
    elif not multi_agent:
        payload["agents"] = []
    payload["multi_agent"] = multi_agent


def build_job(settings: Settings, payload: dict[str, Any]) -> Job:
    """Validate a prepared payload and apply the common submit-time rules."""
    if payload.get("unlimited") and not settings.allow_unlimited_jobs:
        raise ValueError("Unlimited jobs are disabled. Set ALLOW_UNLIMITED_JOBS=true.")
    if not payload.get("trace_id"):
        payload["trace_id"] = current_trace_id() or new_trace_id()
    return Job.model_validate(payload)


def submit_job(
    settings: Settings,
    producer: KafkaProducer,
    job: Job,
    *,
    worker_id: str | None = None,
) -> Job:
    """Publish the job and its queued state."""
    producer.send(
        settings.jobs_topic,
        job.job_id,
        job,
        headers=inject_trace_headers(),
    )
    StatePublisher(settings, producer).queued(job, worker_id=worker_id)
    return job


def job_from_state(
    settings: Settings,
    state: JobState,
    *,
    job_id: str | None = None,
    run_id: str | None = None,
) -> Job:
    """Build a fresh run from a finished job's materialized state."""
    if not state.instruction.strip():
        raise ValueError("This job has no instruction to retry.")
    if state.unlimited and not settings.allow_unlimited_jobs:
        raise ValueError("Unlimited jobs are disabled. Set ALLOW_UNLIMITED_JOBS=true.")
    values = state.model_dump(include=set(JobSpec.model_fields))
    values["trace_id"] = current_trace_id() or new_trace_id()
    if job_id is not None:
        values["job_id"] = job_id
    if run_id is not None:
        values["run_id"] = run_id
    return Job(**values)
