from __future__ import annotations

import logging
import tempfile
import time
from pathlib import Path

from hermes.agent import AgentResult, AgentRunner
from hermes.audit import AuditLogger, JobAudit
from hermes.llm import LLM
from hermes.models import FileRef, Job, JobResult, utc_now
from hermes.settings import Settings
from hermes.storage import S3Storage
from hermes.telemetry import (
    current_trace_id,
    get_tracer,
    new_trace_id,
    set_content,
    set_error,
)
from hermes.tools import ToolRegistry, Workspace


logger = logging.getLogger(__name__)


class Harness:
    def __init__(
        self,
        settings: Settings,
        llm: LLM,
        audit: AuditLogger | None = None,
    ) -> None:
        self.settings = settings
        self.llm = llm
        self.audit = audit
        self._storage: S3Storage | None = None

    def _get_storage(self) -> S3Storage:
        if self._storage is None:
            self._storage = S3Storage(self.settings)
        return self._storage

    def process(self, job: Job, job_audit: JobAudit | None = None) -> JobResult:
        trace_id = job.trace_id or current_trace_id() or new_trace_id()
        job = job.model_copy(update={"trace_id": trace_id})
        if job_audit is None and self.audit is not None:
            job_audit = self.audit.for_job(job)
        started_at = utc_now()
        run_started = time.perf_counter()
        timeout = min(job.timeout_seconds, self.settings.max_job_timeout_seconds)
        deadline = time.monotonic() + timeout
        workspace_root = Path(self.settings.workspace_root)
        workspace_root.mkdir(parents=True, exist_ok=True)
        steps = 0
        usage: dict[str, int] = {}

        with get_tracer().start_as_current_span(
            "agent.run",
            attributes={
                "openinference.span.kind": "AGENT",
                "agent.name": self.settings.agent_name,
                "job.id": job.job_id,
                "job.run_id": job.run_id,
                "hermes.trace_id": trace_id,
                "worker.id": self.settings.worker_id,
            },
        ) as span:
            set_content(span, "input.value", job.instruction)
            try:
                with tempfile.TemporaryDirectory(
                    prefix=f"{job.run_id}-", dir=workspace_root
                ) as temporary_directory:
                    root = Path(temporary_directory)
                    inputs = root / "inputs"
                    outputs = root / "outputs"
                    inputs.mkdir()
                    outputs.mkdir()

                    if job.input_files:
                        storage = self._get_storage()
                        for index, reference in enumerate(job.input_files):
                            if time.monotonic() > deadline:
                                raise TimeoutError(
                                    "The job timeout was reached while downloading."
                                )
                            filename = Path(reference.key).name or f"input-{index}"
                            storage.download(
                                reference, inputs / f"{index:02d}-{filename}"
                            )

                    agent = AgentRunner(
                        llm=self.llm,
                        tools=ToolRegistry(Workspace(root)),
                        audit=job_audit,
                        provider_name=self.settings.llm_provider,
                        model_name=self.settings.llm_model or self.settings.llm_provider,
                    )
                    agent_result: AgentResult = agent.run(
                        instruction=job.instruction,
                        job_input=job.input,
                        deadline=deadline,
                        max_steps=min(job.max_steps, self.settings.max_agent_steps),
                    )
                    steps = agent_result.steps
                    usage = agent_result.usage

                    output_files = [
                        path for path in outputs.rglob("*") if path.is_file()
                    ]
                    artifacts: list[FileRef] = []
                    artifact_error: str | None = None
                    if output_files:
                        if not self.settings.storage_configured:
                            artifact_error = (
                                f"{len(output_files)} result file(s) were not stored "
                                "because no S3 endpoint is configured."
                            )
                        else:
                            try:
                                artifacts = self._get_storage().upload_outputs(
                                    outputs=outputs,
                                    job_id=job.job_id,
                                    run_id=job.run_id,
                                    deadline=deadline,
                                )
                            except Exception as exc:
                                logger.exception(
                                    "Artifact upload failed for job %s",
                                    job.job_id,
                                )
                                artifact_error = str(exc)[:4000]

                        if artifact_error:
                            if job_audit:
                                job_audit.emit(
                                    "artifact.failed",
                                    "failed",
                                    attributes={"file_count": len(output_files)},
                                    error=artifact_error,
                                )
                        else:
                            for artifact in artifacts:
                                if job_audit:
                                    job_audit.emit(
                                        "artifact.stored",
                                        "succeeded",
                                        attributes={
                                            "bucket": artifact.bucket,
                                            "key": artifact.key,
                                        },
                                    )

                span.set_attribute("agent.steps", steps)
                span.set_attribute("agent.artifact_count", len(artifacts))
                if artifact_error:
                    span.set_attribute("agent.artifact_error", artifact_error)
                for key, value in usage.items():
                    metric = key.replace("_tokens", "")
                    span.set_attribute(f"llm.token_count.{metric}", value)
                set_content(span, "output.value", agent_result.text)
                if job_audit:
                    job_audit.emit(
                        "agent.completed",
                        "succeeded",
                        duration_ms=round((time.perf_counter() - run_started) * 1000, 3),
                        attributes={
                            "steps": steps,
                            "usage": usage,
                            "artifact_count": len(artifacts),
                            "artifact_error": artifact_error,
                        },
                    )
                return JobResult(
                    job_id=job.job_id,
                    run_id=job.run_id,
                    status="succeeded",
                    trace_id=trace_id,
                    result=agent_result.text,
                    artifacts=artifacts,
                    artifact_error=artifact_error,
                    worker_id=self.settings.worker_id,
                    started_at=started_at,
                    finished_at=utc_now(),
                    steps=steps,
                    usage=usage,
                )
            except Exception as exc:
                set_error(span, exc)
                logger.exception(
                    "Job %s / run %s failed", job.job_id, job.run_id
                )
                if job_audit:
                    job_audit.emit(
                        "agent.completed",
                        "failed",
                        duration_ms=round((time.perf_counter() - run_started) * 1000, 3),
                        attributes={"steps": steps, "usage": usage},
                        error=str(exc),
                    )
                return JobResult(
                    job_id=job.job_id,
                    run_id=job.run_id,
                    status="failed",
                    trace_id=trace_id,
                    error=str(exc)[:4000],
                    worker_id=self.settings.worker_id,
                    started_at=started_at,
                    finished_at=utc_now(),
                    steps=steps,
                    usage=usage,
                )
