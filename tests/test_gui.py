from __future__ import annotations

import io
import json
import threading
import urllib.error
import urllib.request
from dataclasses import replace
from pathlib import Path
from typing import Any

from kafkallm.agents import AgentStore, default_agents
from kafkallm.gui import JobServer, JobStore
from kafkallm.models import FileRef, Job, JobState
from kafkallm.sandboxes import SandboxStore, default_sandboxes
from kafkallm.settings import Settings


class FakeStorage:
    def __init__(self) -> None:
        self.uploads: list[tuple[str, bytes]] = []
        self.downloads: list[FileRef] = []

    def upload_input(self, filename: str, data: bytes) -> FileRef:
        self.uploads.append((filename, data))
        return FileRef(bucket="jobs", key=f"uploads/{filename}")

    def download_bytes(self, reference: FileRef) -> bytes:
        self.downloads.append(reference)
        return b"artifact-data"

    def open_download(self, reference: FileRef) -> tuple[io.BytesIO, int]:
        self.downloads.append(reference)
        data = b"artifact-data"
        return io.BytesIO(data), len(data)


class RecordingProducer:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str, Any]] = []
        self.tombstones: list[tuple[str, str]] = []

    def send(self, topic: str, key: str, model: Any, headers=None) -> None:
        del headers
        self.messages.append((topic, key, model))

    def send_tombstones(self, topic: str, keys: list[str]) -> None:
        self.tombstones.extend((topic, key) for key in keys)


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


def _download(url: str) -> tuple[int, dict[str, str], bytes]:
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            return response.status, dict(response.headers), response.read()
    except urllib.error.HTTPError as exc:
        return exc.code, dict(exc.headers), exc.read()


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
    store.put_log(
        {
            "job_id": job.job_id,
            "sequence": 0,
            "kind": "system",
            "text": "started",
            "timestamp": "2026-01-01T00:00:00Z",
        }
    )
    store.put_log(
        {
            "job_id": job.job_id,
            "sequence": 1,
            "kind": "goose.tool",
            "text": '{"name": "shell"}',
            "timestamp": "2026-01-01T00:00:01Z",
        }
    )

    detail = store.job(job.job_id)

    assert detail is not None
    assert detail["status"] == "succeeded"
    assert detail["result"] == "done"
    assert detail["events"][0]["type"] == "job.received"
    assert [record["sequence"] for record in store.logs(job.job_id, after=0)] == [1]


def test_gui_submits_a_job_and_serves_state() -> None:
    settings = _settings()
    producer = RecordingProducer()
    store = JobStore(settings)
    server = JobServer(("127.0.0.1", 0), store, producer, settings)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        status, schema = _request(f"{base}/api/schema/job")
        assert status == 200
        assert {"instruction", "sandbox", "agents"} <= set(schema["properties"])

        status, features = _request(f"{base}/api/features")
        assert status == 200
        assert features["unlimited_jobs"] is False
        assert features["reflect_default"] is True

        status, job = _request(
            f"{base}/api/jobs",
            {
                "instruction": "Write a file",
                "input": {"name": "world"},
                "sandbox": "reverse",
                "timeout_seconds": 120,
                "reflect": False,
            },
        )
        assert status == 201
        assert job["instruction"] == "Write a file"
        assert job["sandbox"] == "reverse"
        assert job["reflect"] is False
        # The Job message carries the OCI image reference to run.
        assert job["sandbox_image"] == "kafkallm-rea-job:latest"
        assert producer.messages[0][2].sandbox_image == job["sandbox_image"]
        assert producer.messages[0][2].reflect is False
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

        store.put_log(
            {
                "job_id": job["job_id"],
                "sequence": 0,
                "kind": "goose.tool",
                "text": '{"name": "shell"}',
                "timestamp": "2026-01-01T00:00:00Z",
            }
        )
        status, logs = _request(f"{base}/api/jobs/{job['job_id']}/logs?after=-1")
        assert status == 200
        assert logs["logs"][0]["kind"] == "goose.tool"
        assert logs["next"] == 0

        status, error = _request(
            f"{base}/api/jobs", {"instruction": "", "unexpected": True}
        )
        assert status == 400
        assert "Invalid job" in error["error"]

        status, error = _request(
            f"{base}/api/jobs",
            {"instruction": "Run forever", "unlimited": True},
        )
        assert status == 400
        assert "Unlimited jobs are disabled" in error["error"]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_gui_accepts_unlimited_jobs_when_enabled() -> None:
    settings = replace(_settings(), allow_unlimited_jobs=True)
    producer = RecordingProducer()
    server = JobServer(("127.0.0.1", 0), JobStore(settings), producer, settings)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        status, features = _request(f"{base}/api/features")
        assert status == 200
        assert features["unlimited_jobs"] is True

        status, job = _request(
            f"{base}/api/jobs",
            {"instruction": "Run forever", "unlimited": True},
        )
        assert status == 201
        assert job["unlimited"] is True
        assert producer.messages[0][2].unlimited is True
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_gui_stops_queued_and_running_jobs() -> None:
    settings = _settings()
    producer = RecordingProducer()
    store = JobStore(settings)
    job = Job(instruction="Run for a while")
    store.put_state(JobState.queued(job).model_dump(mode="json"))
    finished = Job(instruction="Already done")
    store.put_state(
        JobState.queued(finished)
        .model_copy(update={"status": "succeeded"})
        .model_dump(mode="json")
    )
    server = JobServer(("127.0.0.1", 0), store, producer, settings)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        status, data = _request(
            f"{base}/api/jobs/{job.job_id}/cancel",
            {},
        )
        assert status == 202
        assert data == {"status": "cancelling", "job_id": job.job_id}
        assert "jobs.cancel" in [topic for topic, _, _ in producer.messages]
        detail = store.job(job.job_id)
        assert detail is not None
        assert detail["cancelled"] is True
        assert detail["status"] == "failed"

        status, data = _request(
            f"{base}/api/jobs/{job.job_id}/cancel",
            {},
        )
        assert status == 200
        assert data["status"] == "cancelled"

        status, error = _request(
            f"{base}/api/jobs/{finished.job_id}/cancel",
            {},
        )
        assert status == 400
        assert "Only queued or running" in error["error"]
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


def test_gui_manages_agents_and_attaches_the_selected_flock(
    tmp_path: Path,
) -> None:
    settings = replace(_settings(), goose_job_agents_dir=str(tmp_path))
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

        profile = (tmp_path / "security-reviewer.md").read_text(encoding="utf-8")
        assert 'name: "Security reviewer"' in profile
        assert "goosetown-reviewer" in profile

        status, disabled = _delete(f"{base}/api/agents/security-reviewer")
        assert status == 200
        assert disabled["enabled"] is False
        profile = (tmp_path / "security-reviewer.md").read_text(encoding="utf-8")
        assert "enabled: false" in profile

        status, error = _request(
            f"{base}/api/agents",
            {"agent_id": "worker", "name": "Renamed worker"},
        )
        assert status == 400
        assert "Built-in agents" in error["error"]

        status, error = _delete(f"{base}/api/agents/worker")
        assert status == 400
        assert "Built-in agents" in error["error"]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_gui_manages_sandboxes_and_validates_job_environments() -> None:
    settings = _settings()
    producer = RecordingProducer()
    sandboxes = SandboxStore(settings)
    for sandbox in default_sandboxes(settings):
        sandboxes.put(sandbox)
    server = JobServer(
        ("127.0.0.1", 0),
        JobStore(settings),
        producer,
        settings,
        sandboxes=sandboxes,
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        status, data = _request(f"{base}/api/sandboxes")
        assert status == 200
        assert {sandbox["sandbox_id"] for sandbox in data["sandboxes"]} >= {
            "default",
            "reverse",
        }

        status, created = _request(
            f"{base}/api/sandboxes",
            {
                "name": "Malware analysis",
                "image": "localhost/kafkallm-malware:latest",
                "description": "YARA and Python RE tooling.",
                "env": {"YARA_RULES": "/opt/rules"},
                "extensions": ["rea:/opt/kafkallm/rea-mcp"],
            },
        )
        assert status == 201
        assert created["sandbox_id"] == "malware-analysis"
        assert created["env"] == {"YARA_RULES": "/opt/rules"}
        assert any(topic == "sandboxes.state" for topic, _, _ in producer.messages)

        status, job = _request(
            f"{base}/api/jobs",
            {"instruction": "Analyze the sample", "sandbox": "malware-analysis"},
        )
        assert status == 201
        assert job["sandbox"] == "malware-analysis"

        status, error = _request(
            f"{base}/api/jobs",
            {"instruction": "Use a missing environment", "sandbox": "missing"},
        )
        assert status == 400
        assert "Unknown sandbox" in error["error"]

        status, disabled = _delete(f"{base}/api/sandboxes/malware-analysis")
        assert status == 200
        assert disabled["enabled"] is False

        status, error = _request(
            f"{base}/api/jobs",
            {
                "instruction": "Use a disabled environment",
                "sandbox": "malware-analysis",
            },
        )
        assert status == 400
        assert "disabled" in error["error"]

        status, error = _delete(f"{base}/api/sandboxes/default")
        assert status == 400
        assert "cannot be disabled" in error["error"]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_gui_downloads_results_and_artifacts() -> None:
    settings = _settings()
    store = JobStore(settings)
    storage = FakeStorage()
    job = Job(instruction="Produce a report", sandbox="default")
    artifact = FileRef(
        bucket="jobs",
        key=f"{job.job_id}/{job.run_id}/report.md",
    )
    store.put_state(
        JobState.queued(job)
        .model_copy(
            update={
                "status": "succeeded",
                "result": "# Report\n\nAll good.",
                "artifacts": [artifact],
            }
        )
        .model_dump(mode="json")
    )
    running = Job(instruction="Still running")
    store.put_state(JobState.queued(running).model_dump(mode="json"))
    server = JobServer(
        ("127.0.0.1", 0),
        store,
        RecordingProducer(),
        settings,
        storage_factory=lambda _settings: storage,
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        status, headers, body = _download(f"{base}/api/jobs/{job.job_id}/result")
        assert status == 200
        assert body == b"# Report\n\nAll good."
        assert headers["Content-Type"].startswith("text/markdown")
        assert "attachment" in headers["Content-Disposition"]
        assert f"{job.job_id}-result.md" in headers["Content-Disposition"]

        status, headers, body = _download(f"{base}/api/jobs/{job.job_id}/artifacts/0")
        assert status == 200
        assert body == b"artifact-data"
        assert storage.downloads == [artifact]
        assert headers["Content-Type"] == "text/markdown"
        assert "report.md" in headers["Content-Disposition"]

        status, _, _ = _download(f"{base}/api/jobs/{job.job_id}/artifacts/1")
        assert status == 404
        status, _, _ = _download(f"{base}/api/jobs/{job.job_id}/artifacts/not-a-number")
        assert status == 404
        status, _, _ = _download(f"{base}/api/jobs/{running.job_id}/result")
        assert status == 404
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_gui_restarts_a_finished_job_with_its_settings() -> None:
    settings = _settings()
    producer = RecordingProducer()
    store = JobStore(settings)
    job = Job(
        instruction="Do it again",
        input={"key": "value"},
        sandbox="reverse",
        sandbox_image="kafkallm-rea-job:latest",
        multi_agent=False,
        timeout_seconds=120,
        reflect=False,
    )
    store.put_state(
        JobState.queued(job)
        .model_copy(update={"status": "succeeded", "result": "done"})
        .model_dump(mode="json")
    )
    server = JobServer(("127.0.0.1", 0), store, producer, settings)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        status, restarted = _request(f"{base}/api/jobs/{job.job_id}/restart", {})

        assert status == 201
        assert restarted["job_id"] != job.job_id
        assert restarted["run_id"] != job.run_id
        assert restarted["instruction"] == "Do it again"
        assert restarted["input"] == {"key": "value"}
        assert restarted["sandbox_image"] == "kafkallm-rea-job:latest"
        assert restarted["multi_agent"] is False
        assert restarted["reflect"] is False
        assert [message[0] for message in producer.messages] == [
            "jobs.in",
            "jobs.state",
        ]
        assert store.job(restarted["job_id"]) is not None
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_gui_rejects_restarting_an_active_job() -> None:
    settings = _settings()
    store = JobStore(settings)
    job = Job(instruction="Still running")
    store.put_state(
        JobState.queued(job)
        .model_copy(update={"status": "running"})
        .model_dump(mode="json")
    )
    server = JobServer(("127.0.0.1", 0), store, RecordingProducer(), settings)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        status, error = _request(f"{base}/api/jobs/{job.job_id}/restart", {})
        assert status == 400
        assert "Only finished jobs" in error["error"]

        status, error = _request(f"{base}/api/jobs/missing/restart", {})
        assert status == 404
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_gui_clears_finished_jobs() -> None:
    settings = _settings()
    producer = RecordingProducer()
    store = JobStore(settings)
    waiting = Job(instruction="Still queued")
    done = Job(instruction="Done")
    failed = Job(instruction="Failed")
    store.put_state(JobState.queued(waiting).model_dump(mode="json"))
    store.put_state(
        JobState.queued(done)
        .model_copy(update={"status": "succeeded"})
        .model_dump(mode="json")
    )
    store.put_state(
        JobState.queued(failed)
        .model_copy(update={"status": "failed"})
        .model_dump(mode="json")
    )
    server = JobServer(("127.0.0.1", 0), store, producer, settings)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        status, data = _request(f"{base}/api/jobs/clear-finished", {})

        assert status == 200
        assert data == {"removed": 2}
        assert store.job(waiting.job_id) is not None
        assert store.job(done.job_id) is None
        assert store.job(failed.job_id) is None
        assert set(producer.tombstones) == {
            (settings.state_topic, done.job_id),
            (settings.state_topic, failed.job_id),
        }

        status, data = _request(f"{base}/api/jobs/clear-finished", {})
        assert status == 200
        assert data == {"removed": 0}
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


def test_gui_serves_split_assets_and_ignores_query_strings() -> None:
    settings = _settings()
    server = JobServer(
        ("127.0.0.1", 0), JobStore(settings), RecordingProducer(), settings
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        status, headers, body = _download(f"{base}/app.js")
        assert status == 200
        assert headers["Content-Type"].startswith("text/javascript")
        assert b"loadJobs" in body

        status, headers, body = _download(f"{base}/app.css")
        assert status == 200
        assert headers["Content-Type"].startswith("text/css")

        status, features = _request(f"{base}/api/features?ignore=me")
        assert status == 200
        assert "unlimited_jobs" in features
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_gui_rejects_oversized_json_bodies() -> None:
    settings = replace(_settings(), gui_max_request_bytes=1024)
    producer = RecordingProducer()
    server = JobServer(("127.0.0.1", 0), JobStore(settings), producer, settings)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        status, error = _request(f"{base}/api/jobs", {"instruction": "x" * 2000})
        assert status == 413
        assert "exceeds the 1024 byte limit" in error["error"]
        assert producer.messages == []
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
