import json
import time
from dataclasses import replace
from pathlib import Path

import httpx
import pytest

from hermes.hermes_runtime import HermesRunner, HermesRuntimeError
from hermes.models import Job
from hermes.settings import Settings


def _settings() -> Settings:
    return replace(
        Settings.from_env(),
        hermes_api_url="http://hermes.test",
        hermes_api_key="test-key",
        hermes_model="test-model",
        hermes_provider="test-provider",
        hermes_startup_timeout_seconds=1,
    )


def test_hermes_runner_submits_and_polls_completed_run(tmp_path: Path) -> None:
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path == "/v1/runs":
            captured["body"] = json.loads(request.content)
            captured["idempotency"] = request.headers["Idempotency-Key"]
            return httpx.Response(202, json={"run_id": "run_123", "status": "started"})
        if request.method == "GET" and request.url.path == "/v1/runs/run_123":
            return httpx.Response(
                200,
                json={
                    "status": "completed",
                    "output": "Hermes result",
                    "usage": {"input_tokens": 4, "output_tokens": 3},
                },
            )
        return httpx.Response(404)

    client = httpx.Client(
        transport=httpx.MockTransport(handler),
        base_url="http://hermes.test",
    )
    runner = HermesRunner(_settings(), client=client)
    job = Job(instruction="Do the thing", run_id="run-local")

    result = runner.run(
        job=job,
        workspace=tmp_path,
        deadline=time.monotonic() + 5,
        max_steps=3,
    )

    assert result.text == "Hermes result"
    assert result.usage == {"input_tokens": 4, "output_tokens": 3}
    assert captured["idempotency"] == "run-local"
    assert captured["body"]["model"] == "test-model"
    assert captured["body"]["provider"] == "test-provider"
    assert str(tmp_path) in captured["body"]["input"]
    assert "Do the thing" in captured["body"]["input"]


def test_hermes_runner_surfaces_failed_run(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/runs":
            return httpx.Response(202, json={"run_id": "run_fail"})
        if request.url.path == "/v1/runs/run_fail":
            return httpx.Response(
                200, json={"status": "failed", "error": "provider unavailable"}
            )
        return httpx.Response(404)

    client = httpx.Client(
        transport=httpx.MockTransport(handler),
        base_url="http://hermes.test",
    )
    runner = HermesRunner(_settings(), client=client)

    with pytest.raises(HermesRuntimeError, match="provider unavailable"):
        runner.run(
            job=Job(instruction="Fail"),
            workspace=tmp_path,
            deadline=time.monotonic() + 5,
            max_steps=3,
        )


def test_hermes_runner_surfaces_failure_output(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/runs":
            return httpx.Response(202, json={"run_id": "run_no_provider"})
        if request.url.path == "/v1/runs/run_no_provider":
            return httpx.Response(
                200,
                json={
                    "status": "failed",
                    "output": "No AI provider is configured.",
                },
            )
        return httpx.Response(404)

    client = httpx.Client(
        transport=httpx.MockTransport(handler),
        base_url="http://hermes.test",
    )
    runner = HermesRunner(_settings(), client=client)

    with pytest.raises(HermesRuntimeError, match="No AI provider is configured"):
        runner.run(
            job=Job(instruction="Fail without provider"),
            workspace=tmp_path,
            deadline=time.monotonic() + 5,
            max_steps=3,
        )


def test_hermes_readiness_check() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health/detailed":
            return httpx.Response(200, json={"status": "ok"})
        if request.url.path == "/v1/capabilities":
            return httpx.Response(
                200, json={"features": {"run_submission": True}}
            )
        return httpx.Response(404)

    client = httpx.Client(
        transport=httpx.MockTransport(handler),
        base_url="http://hermes.test",
    )

    HermesRunner(_settings(), client=client).wait_ready()


def test_hermes_runner_retries_temporary_submit_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("hermes.hermes_runtime.time.sleep", lambda _: None)
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        if request.method == "POST" and request.url.path == "/v1/runs":
            attempts += 1
            if attempts == 1:
                return httpx.Response(
                    503,
                    headers={"Retry-After": "0"},
                    json={"error": {"message": "storage_unavailable"}},
                )
            return httpx.Response(202, json={"run_id": "run_retry"})
        if request.url.path == "/v1/runs/run_retry":
            return httpx.Response(
                200, json={"status": "completed", "output": "Recovered"}
            )
        return httpx.Response(404)

    client = httpx.Client(
        transport=httpx.MockTransport(handler),
        base_url="http://hermes.test",
    )
    runner = HermesRunner(_settings(), client=client)

    result = runner.run(
        job=Job(instruction="Retry"),
        workspace=tmp_path,
        deadline=time.monotonic() + 5,
        max_steps=3,
    )

    assert attempts == 2
    assert result.text == "Recovered"


def test_hermes_unknown_status_is_not_terminal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("hermes.hermes_runtime.time.sleep", lambda _: None)
    polls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal polls
        if request.url.path == "/v1/runs":
            return httpx.Response(202, json={"run_id": "run_unknown"})
        if request.url.path == "/v1/runs/run_unknown":
            polls += 1
            if polls == 1:
                return httpx.Response(200, json={"status": "unknown"})
            return httpx.Response(
                200, json={"status": "completed", "output": "Recovered"}
            )
        return httpx.Response(404)

    client = httpx.Client(
        transport=httpx.MockTransport(handler),
        base_url="http://hermes.test",
    )
    runner = HermesRunner(_settings(), client=client)

    result = runner.run(
        job=Job(instruction="Recover"),
        workspace=tmp_path,
        deadline=time.monotonic() + 5,
        max_steps=3,
    )

    assert polls == 2
    assert result.text == "Recovered"
