from __future__ import annotations

import logging
import threading
import time
from pathlib import Path
from typing import Any

import httpx

from hermes.agent import AgentResult
from hermes.audit import JobAudit
from hermes.models import Job
from hermes.settings import Settings
from hermes.telemetry import get_tracer, inject_trace_headers, set_content, set_error


logger = logging.getLogger(__name__)

_TERMINAL_STATUSES = {"completed", "failed", "cancelled", "interrupted"}
_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class HermesRuntimeError(RuntimeError):
    pass


class HermesRunner:
    def __init__(
        self,
        settings: Settings,
        client: httpx.Client | None = None,
    ) -> None:
        if not settings.hermes_api_url:
            raise ValueError("HERMES_API_URL must be configured.")
        if not settings.hermes_api_key:
            raise ValueError("HERMES_API_KEY must be configured.")
        self.settings = settings
        self.client = client or httpx.Client(
            base_url=settings.hermes_api_url,
            headers={"Authorization": f"Bearer {settings.hermes_api_key}"},
            timeout=httpx.Timeout(connect=10.0, read=60.0, write=30.0, pool=10.0),
            transport=httpx.HTTPTransport(retries=2),
        )
        self._active_run_id: str | None = None
        self._active_lock = threading.Lock()

    def wait_ready(self) -> None:
        deadline = time.monotonic() + self.settings.hermes_startup_timeout_seconds
        delay = 1.0
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            try:
                response = self.client.get("/health/detailed", timeout=10.0)
                if response.status_code != 200:
                    raise HermesRuntimeError(
                        f"Hermes readiness check returned HTTP {response.status_code}"
                    )
                readiness = response.json()
                status = str(readiness.get("status", "")).lower()
                if status not in {"ok", "ready", "healthy"}:
                    raise HermesRuntimeError(
                        f"Hermes readiness is {status or 'unknown'}"
                    )
                capabilities = self.client.get("/v1/capabilities", timeout=10.0)
                if capabilities.status_code != 200:
                    raise HermesRuntimeError(
                        "Hermes capabilities check returned HTTP "
                        f"{capabilities.status_code}"
                    )
                features = capabilities.json().get("features", {})
                if not features.get("run_submission"):
                    raise HermesRuntimeError(
                        "Hermes API does not advertise run submission support."
                    )
                logger.info(
                    "Hermes Agent is ready at %s", self.settings.hermes_api_url
                )
                return
            except (httpx.HTTPError, ValueError) as exc:
                last_error = exc
            except HermesRuntimeError as exc:
                last_error = exc
            remaining = max(0.0, deadline - time.monotonic())
            logger.warning(
                "Hermes Agent not ready yet (%s); retrying in %.1fs",
                last_error,
                min(delay, remaining),
            )
            time.sleep(min(delay, remaining))
            delay = min(delay * 2, 10.0)
        raise HermesRuntimeError(
            "Hermes Agent did not become ready within the startup timeout."
        ) from last_error

    def close(self) -> None:
        self.cancel_active()
        self.client.close()

    def cancel_active(self) -> None:
        with self._active_lock:
            remote_run_id = self._active_run_id
        if remote_run_id:
            logger.info("Stopping active Hermes run %s", remote_run_id)
            self._stop(remote_run_id)

    def run(
        self,
        job: Job,
        workspace: Path,
        deadline: float,
        max_steps: int,
        audit: JobAudit | None = None,
    ) -> AgentResult:
        del max_steps
        prompt = self._build_prompt(job, workspace)
        attributes = {
            "openinference.span.kind": "AGENT",
            "agent.runtime": "hermes",
            "hermes.model": self.settings.hermes_model or "configured-default",
            "hermes.provider": (
                self.settings.hermes_provider or "configured-default"
            ),
            "job.id": job.job_id,
            "job.run_id": job.run_id,
        }
        remote_run_id: str | None = None
        audit_completed = False
        with get_tracer().start_as_current_span(
            "hermes.run", attributes=attributes
        ) as span:
            set_content(span, "input.value", prompt)
            try:
                remote_run_id = self._submit(job, prompt, deadline)
                with self._active_lock:
                    self._active_run_id = remote_run_id
                span.set_attribute("hermes.run_id", remote_run_id)
                if audit:
                    audit.emit(
                        "hermes.started",
                        attributes={
                            "remote_run_id": remote_run_id,
                            "workspace": str(workspace),
                            "model": self.settings.hermes_model or None,
                            "provider": self.settings.hermes_provider or None,
                        },
                    )

                status = self._poll(remote_run_id, deadline)
                if status.get("status") != "completed":
                    error = self._status_error(status)
                    if audit:
                        audit.emit(
                            "hermes.completed",
                            "failed",
                            attributes={"remote_run_id": remote_run_id},
                            error=error,
                        )
                        audit_completed = True
                    raise HermesRuntimeError(error)

                output = status.get("output")
                if not isinstance(output, str) or not output.strip():
                    raise HermesRuntimeError(
                        "Hermes completed without a text response."
                    )
                if len(output.encode("utf-8")) > 500_000:
                    raise HermesRuntimeError(
                        "Hermes response is too large for jobs.out."
                    )
                usage = self._usage(status.get("usage"))
                if audit:
                    audit.emit(
                        "hermes.completed",
                        "succeeded",
                        attributes={
                            "remote_run_id": remote_run_id,
                            "usage": usage,
                        },
                    )
                    audit_completed = True
                set_content(span, "output.value", output)
                return AgentResult(text=output, steps=0, usage=usage)
            except Exception as exc:
                set_error(span, exc)
                if audit and not audit_completed:
                    audit.emit(
                        "hermes.completed",
                        "failed",
                        attributes={"remote_run_id": remote_run_id},
                        error=str(exc),
                    )
                raise
            finally:
                with self._active_lock:
                    if self._active_run_id == remote_run_id:
                        self._active_run_id = None

    def _submit(self, job: Job, prompt: str, deadline: float) -> str:
        payload: dict[str, Any] = {
            "input": prompt,
            "instructions": (
                "You are Hermes Agent executing one automated Kafka job. Work inside "
                "the provided job workspace and return only the completed result."
            ),
        }
        if self.settings.hermes_model:
            payload["model"] = self.settings.hermes_model
        if self.settings.hermes_provider:
            payload["provider"] = self.settings.hermes_provider
        headers = {
            "Idempotency-Key": job.run_id,
            **dict(inject_trace_headers()),
        }
        delay = 0.5

        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("The job timeout was reached before submission.")
            try:
                response = self.client.post(
                    "/v1/runs",
                    json=payload,
                    headers=headers,
                    timeout=min(30.0, remaining),
                )
            except httpx.HTTPError as exc:
                logger.warning("Submitting Hermes run failed: %s", exc)
                last_error: Exception = exc
            else:
                if response.status_code in {200, 202}:
                    data = response.json()
                    remote_run_id = data.get("run_id")
                    if not isinstance(remote_run_id, str) or not remote_run_id:
                        raise HermesRuntimeError(
                            "Hermes did not return a run_id."
                        )
                    return remote_run_id
                if response.status_code not in _RETRYABLE_STATUS_CODES:
                    raise HermesRuntimeError(self._response_error(response))
                last_error = HermesRuntimeError(self._response_error(response))
                delay = self._retry_delay(response, delay)

            logger.warning("Retrying Hermes submission in %.1fs", delay)
            time.sleep(min(delay, max(0.0, deadline - time.monotonic())))
            delay = min(delay * 2, 10.0)

    def _poll(self, remote_run_id: str, deadline: float) -> dict[str, Any]:
        delay = 1.0
        while time.monotonic() < deadline:
            try:
                response = self.client.get(
                    f"/v1/runs/{remote_run_id}",
                    timeout=min(15.0, max(1.0, deadline - time.monotonic())),
                )
                if response.status_code == 200:
                    status = response.json()
                    run_status = status.get("status")
                    if run_status == "waiting_for_approval":
                        self._stop(remote_run_id)
                        raise HermesRuntimeError(
                            "Hermes run requires human approval; configure the "
                            "Hermes approval policy for unattended jobs."
                        )
                    if run_status in _TERMINAL_STATUSES:
                        return status
                elif response.status_code not in _RETRYABLE_STATUS_CODES:
                    raise HermesRuntimeError(self._response_error(response))
                else:
                    logger.warning(
                        "Hermes run poll returned HTTP %s",
                        response.status_code,
                    )
                    delay = self._retry_delay(response, delay)
            except (httpx.HTTPError, ValueError) as exc:
                logger.warning("Polling Hermes run failed: %s", exc)

            time.sleep(min(delay, max(0.0, deadline - time.monotonic())))
            delay = min(delay * 1.5, 5.0)

        self._stop(remote_run_id)
        raise TimeoutError("The Hermes run exceeded the job timeout.")

    def _stop(self, remote_run_id: str) -> None:
        try:
            response = self.client.post(
                f"/v1/runs/{remote_run_id}/stop", timeout=5.0
            )
            if response.status_code not in {200, 202, 409}:
                logger.warning(
                    "Stopping Hermes run %s returned HTTP %s",
                    remote_run_id,
                    response.status_code,
                )
        except httpx.HTTPError:
            logger.warning("Could not stop Hermes run %s", remote_run_id)

    @staticmethod
    def _retry_delay(response: httpx.Response, fallback: float) -> float:
        raw = response.headers.get("Retry-After")
        if raw:
            try:
                return min(max(float(raw), 0.0), 30.0)
            except ValueError:
                pass
        return fallback

    def _build_prompt(self, job: Job, workspace: Path) -> str:
        inputs = workspace / "inputs"
        outputs = workspace / "outputs"
        return (
            "Execute this job inside the shared job workspace.\n"
            f"Workspace root: {workspace}\n"
            f"Input files: {inputs}\n"
            f"Write all generated result files to: {outputs}\n"
            "Do not modify files outside the workspace.\n\n"
            f"Instruction:\n{job.instruction}\n\n"
            "Structured job input:\n"
            f"{job.input}"
        )

    @staticmethod
    def _usage(raw: Any) -> dict[str, int]:
        if not isinstance(raw, dict):
            return {}
        return {
            key: int(value)
            for key, value in raw.items()
            if isinstance(value, int) and not isinstance(value, bool)
        }

    @staticmethod
    def _status_error(status: dict[str, Any]) -> str:
        error = status.get("error") or status.get("turn_exit_reason")
        if error:
            return str(error)
        output = status.get("output")
        if isinstance(output, str) and output.strip():
            return f"Hermes run failed: {output.strip()[:4000]}"
        return f"Hermes run ended with status {status.get('status', 'unknown')}."

    @staticmethod
    def _response_error(response: httpx.Response) -> str:
        try:
            payload = response.json()
        except ValueError:
            payload = None
        if isinstance(payload, dict):
            error = payload.get("error")
            if isinstance(error, dict) and error.get("message"):
                return f"Hermes API error: {error['message']}"
            if isinstance(error, str):
                return f"Hermes API error: {error}"
        return (
            f"Hermes API returned HTTP {response.status_code}: "
            f"{response.text[:1000]}"
        )
