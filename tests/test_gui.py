from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from dataclasses import replace
from typing import Any

from kafkallm.gui import JobServer, JobStore
from kafkallm.models import Job, JobState
from kafkallm.settings import Settings


class RecordingProducer:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str, Any]] = []

    def send(self, topic: str, key: str, model: Any, headers=None) -> None:
        del headers
        self.messages.append((topic, key, model))


def _settings() -> Settings:
    return replace(
        Settings.from_env(),
        jobs_topic="jobs.in",
        results_topic="jobs.out",
        state_topic="jobs.state",
        events_topic="agent.events",
        gui_port=0,
    )


def _request(url: str, payload: dict[str, Any] | None = None) -> tuple[int, Any]:
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    request = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST" if data is not None else "GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def test_store_keeps_latest_state_and_events() -> None:
    store = JobStore(_settings())
    job = Job(instruction="Hello")
    store.put_state(JobState.queued(job).model_dump(mode="json"))
    store.put_state(
        JobState.queued(job)
        .model_copy(update={"status": "succeeded", "result": "done"})
        .model_dump(mode="json")
    )
    store.put_event(
        {
            "job_id": job.job_id,
            "type": "job.received",
            "status": "info",
            "timestamp": "2026-01-01T00:00:00Z",
        }
    )

    detail = store.job(job.job_id)

    assert detail is not None
    assert detail["status"] == "succeeded"
    assert detail["result"] == "done"
    assert detail["events"][0]["type"] == "job.received"


def test_gui_submits_a_job_and_serves_state() -> None:
    settings = _settings()
    producer = RecordingProducer()
    store = JobStore(settings)
    server = JobServer(("127.0.0.1", 0), store, producer, settings)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        status, job = _request(
            f"{base}/api/jobs",
            {
                "instruction": "Write a file",
                "input": {"name": "world"},
                "timeout_seconds": 120,
                "max_steps": 4,
            },
        )
        assert status == 201
        assert job["instruction"] == "Write a file"
        assert [message[0] for message in producer.messages] == [
            "jobs.in",
            "jobs.state",
        ]

        store.put_state(
            JobState.queued(Job.model_validate(job))
            .model_copy(update={"status": "running"})
            .model_dump(mode="json")
        )
        status, listing = _request(f"{base}/api/jobs")
        assert status == 200
        assert listing["jobs"][0]["status"] == "running"

        status, detail = _request(f"{base}/api/jobs/{job['job_id']}")
        assert status == 200
        assert detail["job_id"] == job["job_id"]

        status, error = _request(
            f"{base}/api/jobs", {"instruction": "", "unexpected": True}
        )
        assert status == 400
        assert "Invalid job" in error["error"]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
