from __future__ import annotations

import json
from dataclasses import replace
from typing import Any

from kafkallm.core.models import JobControl
from kafkallm.core.settings import Settings
from kafkallm.worker.controls import ControlStore


class FakeMessage:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    def value(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")


def _store(
    monkeypatch: Any, messages: list[dict[str, Any]], **kwargs: Any
) -> ControlStore:
    settings = replace(Settings.from_env(), control_topic="test.controls")

    def fake_iter(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        for payload in messages:
            yield FakeMessage(payload)

    monkeypatch.setattr("kafkallm.worker.controls.iter_messages", fake_iter)
    return ControlStore(settings, **kwargs)


def test_control_store_tracks_cancellation_by_run(monkeypatch: Any) -> None:
    control = JobControl(action="cancel", job_id="job-1", run_id="run-1")
    store = _store(monkeypatch, [control.model_dump(mode="json")])

    store._consume(
        name="test", group="test-cancel", unique_group=True, actions={"cancel"}
    )

    assert store.is_cancelled("job-1", "run-1") is True
    assert store.is_cancelled("job-1", "run-2") is False


def test_control_store_handles_retry_once(monkeypatch: Any) -> None:
    control = JobControl(
        action="retry",
        job_id="job-1",
        run_id="run-1",
        new_job_id="job-2",
        new_run_id="run-2",
    )
    handled: list[JobControl] = []
    store = _store(
        monkeypatch,
        [control.model_dump(mode="json"), control.model_dump(mode="json")],
        on_retry=handled.append,
    )

    store._consume(
        name="test", group="test-retry", unique_group=False, actions={"retry"}
    )

    assert len(handled) == 1
    assert handled[0].new_job_id == "job-2"
