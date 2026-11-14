"""Live activity and trace helpers for the goose job runtime."""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from gaensestall.sandbox.goose_output import stream_event_log_records
from gaensestall.sandbox.safe_fs import open_text_no_follow


class _SpanActivity:
    """Record goose stream activity as span events for the trace timeline.

    Thinking chunks, tool calls, and tool results already feed the live log; the
    same records become OTel span events so MLflow shows them in the trace.
    """

    _IGNORED_PREFIXES = ("stderr", "system")

    def __init__(self, span: Any, limit: int = 400, text_limit: int = 4000) -> None:
        self.span = span
        self.limit = limit
        self.text_limit = text_limit
        self._count = 0
        self._lock = threading.Lock()

    def __call__(self, kind: str, text: str) -> None:
        if kind.startswith(self._IGNORED_PREFIXES) or not self.span.is_recording():
            return
        with self._lock:
            if self._count >= self.limit:
                return
            self._count += 1
        self.span.add_event(
            kind,
            attributes={
                "gaensestall.kind": kind,
                "gaensestall.text": text[: self.text_limit],
            },
        )


class _DelegationTracker:
    """Turn delegate tool calls and completions into agent-scoped records.

    Goose streams the delegate request, an acknowledgement with the subagent
    session id, and a ``Subagent <id> completed`` notification. The subagent's
    final text is not part of the main stream, so the returned event reports the
    role and duration rather than inventing a result.
    """

    def __init__(self) -> None:
        self._calls: dict[str, tuple[str, float]] = {}
        self._fallback: list[tuple[str, float]] = []
        self._subagents: dict[str, tuple[str, float]] = {}

    def observe_event(self, event: dict[str, Any]) -> list[tuple[str, str]]:
        if event.get("type") != "message":
            return []
        message = event.get("message")
        if not isinstance(message, dict):
            return []
        role = message.get("role")
        records: list[tuple[str, str]] = []
        for block in message.get("content") or []:
            if not isinstance(block, dict):
                continue
            block_type = block.get("type")
            if block_type in {"toolRequest", "tool_use", "tool_call"}:
                if role != "assistant":
                    continue
                if block_type == "toolRequest":
                    call = block.get("toolCall") or {}
                    value = call.get("value") or {}
                else:
                    call = block
                    value = {"name": block.get("name"), "input": block.get("input")}
                if value.get("name") != "delegate":
                    continue
                call_id = str(block.get("id") or call.get("id") or "")
                data = value.get("arguments") or value.get("input") or {}
                if not isinstance(data, dict):
                    data = {}
                source = str(data.get("source") or "ad-hoc").strip() or "ad-hoc"
                instructions = str(data.get("instructions") or "").strip()
                started = time.monotonic()
                if call_id:
                    self._calls[call_id] = (source, started)
                else:
                    self._fallback.append((source, started))
                detail = f"{source}: {instructions}" if instructions else source
                records.append(("agent.delegated", detail[:4000]))
            elif block_type in {"toolResponse", "tool_result"} and role == "user":
                call_id = str(block.get("id") or "")
                value = (block.get("toolResult") or {}).get("value") or {}
                meta = value.get("_meta") or {}
                session_id = meta.get("subagent_session_id")
                pending = self._calls.pop(call_id, None) if call_id else None
                if pending is None and not call_id and self._fallback:
                    pending = self._fallback.pop(0)
                if pending is None:
                    continue
                source, started = pending
                if isinstance(session_id, str) and session_id:
                    self._subagents[session_id] = (source, started)
                    continue
                elapsed = max(0.0, time.monotonic() - started)
                content = _tool_result_text(value)
                records.append(
                    (
                        "agent.returned",
                        f"{source} ({elapsed:.1f}s): {content}"[:4000],
                    )
                )
            elif block_type == "systemNotification":
                message_text = str(block.get("msg") or "")
                prefix, suffix = "Subagent ", " completed"
                if not message_text.startswith(prefix) or not message_text.endswith(
                    suffix
                ):
                    continue
                session_id = message_text[len(prefix) : -len(suffix)]
                pending = self._subagents.pop(session_id, None)
                if pending is None:
                    continue
                source, started = pending
                elapsed = max(0.0, time.monotonic() - started)
                records.append(
                    (
                        "agent.returned",
                        f"{source} completed ({elapsed:.1f}s, subagent {session_id})"[
                            :4000
                        ],
                    )
                )
        return records


def _tool_result_text(value: dict[str, Any]) -> str:
    content = value.get("content")
    if isinstance(content, list):
        text = "\n".join(
            item.get("text", "")
            for item in content
            if isinstance(item, dict) and item.get("type") == "text"
        )
        if text:
            return text
    if isinstance(content, str):
        return content
    return ""


class _LogCollector:
    """Tail the job workspace and turn goose stream-json into live activity."""

    def __init__(self, workdir: Path, log: Callable[[str, str], None]) -> None:
        self.workdir = workdir
        self.log = log
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []

    def start(self) -> None:
        jobs = (
            ("main.err", "stderr", False, ""),
            ("main.jsonl", "goose", True, "goose"),
        )
        for name, kind, parse_json, prefix in jobs:
            thread = threading.Thread(
                target=self._tail,
                args=(self.workdir / ".gaensestall" / name, kind, parse_json, prefix),
                name=f"gaensestall-log-{name}",
                daemon=True,
            )
            thread.start()
            self._threads.append(thread)

    def stop(self) -> None:
        self._stop.set()
        for thread in self._threads:
            thread.join(timeout=2)

    def _tail(
        self,
        path: Path,
        kind: str,
        parse_json: bool,
        prefix: str,
    ) -> None:
        pending: dict[str, str] = {}
        delegation = _DelegationTracker() if parse_json and prefix == "goose" else None

        def flush(kind_name: str | None = None) -> None:
            if kind_name is None:
                for name in list(pending):
                    flush(name)
                return
            text = pending.pop(kind_name, "").strip()
            if text:
                self.log(kind_name, text)

        def emit(record_kind: str, text: str) -> None:
            if record_kind.endswith(
                (".tool", ".tool_result", ".usage", ".error", ".raw")
            ) or record_kind.startswith("agent."):
                flush()
                self.log(record_kind, text)
                return
            if record_kind not in pending:
                flush()
                pending[record_kind] = ""
            pending[record_kind] += text
            if len(pending[record_kind]) >= 400 or "\n" in pending[record_kind]:
                flush(record_kind)

        while not self._stop.is_set():
            stream = open_text_no_follow(path)
            if stream is None:
                self._stop.wait(0.5)
                continue
            idle_since = time.monotonic()
            try:
                while not self._stop.is_set():
                    line = stream.readline()
                    if line == "":
                        if pending and time.monotonic() - idle_since >= 1.0:
                            flush()
                        self._stop.wait(0.5)
                        continue
                    idle_since = time.monotonic()
                    line = line.rstrip("\r\n")
                    if not line:
                        continue
                    if not parse_json:
                        self.log(kind, line)
                        continue
                    try:
                        event = json.loads(line)
                    except ValueError:
                        emit(f"{prefix}.raw", line)
                        continue
                    if isinstance(event, dict):
                        if delegation is not None:
                            for extra_kind, extra_text in delegation.observe_event(
                                event
                            ):
                                emit(extra_kind, extra_text)
                        for record_kind, text in stream_event_log_records(
                            event, prefix
                        ):
                            emit(record_kind, text)
                    else:
                        emit(f"{prefix}.raw", line)
            finally:
                flush()
                stream.close()
