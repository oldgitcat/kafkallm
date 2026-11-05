from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from dataclasses import replace
from typing import Any

from kafkallm.agents import AgentStore, default_agents
from kafkallm.gui import JobServer, JobStore
from kafkallm.models import FileRef, Job, JobState
from kafkallm.settings import Settings


class FakeStorage:
    def __init__(self) -> None:
        self.uploads: list[tuple[str, bytes]] = []

    def upload_input(self, filename: str, data: bytes) -> FileRef:
        self.uploads.append((filename, data))
        return FileRef(bucket="jobs", key=f"uploads/{filename}")


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


def _delete(url: str) -> tuple[int, Any]:
    request = urllib.request.Request(url, method="DELETE")
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def _post_multipart(
    url: str, filename: str, data: bytes, boundary: str = "kafkallm-test"
) -> tuple[int, Any]:
    body = (
        (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="files"; filename="{filename}"\r\n'
            "Content-Type: application/octet-stream\r\n\r\n"
        ).encode()
        + data
        + f"\r\n--{boundary}--\r\n".encode()
    )
    request = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        method="POST",
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


def test_gui_uploads_binary_inputs_and_attaches_them_to_the_job() -> None:
    settings = replace(
        _settings(),
        s3_endpoint_url="http://fake-s3",
        upload_max_files=4,
        upload_max_file_bytes=1024,
        upload_max_total_bytes=4096,
    )
    producer = RecordingProducer()
    storage = FakeStorage()
    store = JobStore(settings)
    server = JobServer(
        ("127.0.0.1", 0),
        store,
        producer,
        settings,
        storage_factory=lambda _settings: storage,
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    payload = b"\x00\x01binary\r\nwith\xffbytes"
    try:
        status, uploaded = _post_multipart(
            f"{base}/api/uploads", "payload.bin", payload
        )
        assert status == 201
        assert storage.uploads == [("payload.bin", payload)]
        assert uploaded["input_files"] == [
            {"bucket": "jobs", "key": "uploads/payload.bin"}
        ]

        status, job = _request(
            f"{base}/api/jobs",
            {
                "instruction": "Use the uploaded file",
                "input_files": uploaded["input_files"],
            },
        )
        assert status == 201
        assert job["input_files"] == uploaded["input_files"]
        assert producer.messages[0][2].input_files[0].key == ("uploads/payload.bin")
        queued_state = producer.messages[1][2]
        assert queued_state.input_files[0].key == "uploads/payload.bin"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_gui_manages_agents_and_attaches_the_selected_flock() -> None:
    settings = _settings()
    producer = RecordingProducer()
    agents = AgentStore(settings)
    for agent in default_agents():
        agents.put(agent)
    server = JobServer(
        ("127.0.0.1", 0),
        JobStore(settings),
        producer,
        settings,
        agents=agents,
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        status, data = _request(f"{base}/api/agents")
        assert status == 200
        assert {agent["agent_id"] for agent in data["agents"]} >= {
            "worker",
            "reviewer",
        }

        status, created = _request(
            f"{base}/api/agents",
            {
                "name": "Security reviewer",
                "role": "reviewer",
                "description": "Checks security-sensitive outputs.",
                "skills": ["goosetown-reviewer"],
            },
        )
        assert status == 201
        assert created["agent_id"] == "security-reviewer"
        assert any(topic == "agents.state" for topic, _, _ in producer.messages)

        status, job = _request(
            f"{base}/api/jobs",
            {
                "instruction": "Review the design",
                "agent_ids": ["worker", "security-reviewer"],
            },
        )
        assert status == 201
        assert [agent["agent_id"] for agent in job["agents"]] == [
            "worker",
            "security-reviewer",
        ]
        job_message = next(
            model for topic, _, model in producer.messages if topic == "jobs.in"
        )
        assert [agent.agent_id for agent in job_message.agents] == [
            "worker",
            "security-reviewer",
        ]
        queued = next(
            model for topic, _, model in producer.messages if topic == "jobs.state"
        )
        assert queued.agent_names == ["Worker", "Security reviewer"]

        status, disabled = _delete(f"{base}/api/agents/security-reviewer")
        assert status == 200
        assert disabled["enabled"] is False
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_gui_rejects_oversized_uploads() -> None:
    settings = replace(
        _settings(),
        s3_endpoint_url="http://fake-s3",
        upload_max_file_bytes=4,
    )
    storage = FakeStorage()
    server = JobServer(
        ("127.0.0.1", 0),
        JobStore(settings),
        RecordingProducer(),
        settings,
        storage_factory=lambda _settings: storage,
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        status, error = _post_multipart(f"{base}/api/uploads", "too-big.bin", b"12345")
        assert status == 413
        assert "size limit" in error["error"]
        assert storage.uploads == []
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
