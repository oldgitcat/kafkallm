from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from kafkallm.safe_fs import open_text_no_follow, read_text_no_follow

# A stream-json event repeats large metadata (for example the advertised tool
# list) on every line. A long job can therefore produce a main.jsonl that is
# far larger than the control-file read limit. Never silently truncate it: the
# final assistant message sits at the end, and a truncated prefix would make
# the parser return an old intermediate sentence as the job result.
_MAX_STREAM_LINE_BYTES = 4 * 1024 * 1024
_MAX_STREAM_BYTES = 2 * 1024 * 1024 * 1024

NO_OUTPUT_ERROR = "goose produced no output."


def parse_goose_output(raw: str) -> tuple[str, dict[str, int], int, str | None]:
    """Parse goose output from either the legacy JSON or stream-json format.

    The runtime now requests line-delimited `stream-json` events, but old job
    bundles and fixtures still contain one JSON document. Both are accepted.
    """
    if not raw.strip():
        return "", {}, 0, NO_OUTPUT_ERROR
    try:
        document = json.loads(raw)
    except ValueError:
        stream = _parse_stream_events(raw)
        if stream is not None:
            return stream
        start = raw.find("{")
        if start < 0:
            return "", {}, 0, "goose output is not JSON."
        try:
            document = json.loads(raw[start:])
        except ValueError as exc:
            return "", {}, 0, f"goose output is not valid JSON: {exc}"
    if not isinstance(document, dict):
        return "", {}, 0, "goose output is not a JSON object."
    if document.get("type") in {"message", "complete", "error"}:
        stream = _parse_stream_events(raw)
        if stream is not None:
            return stream
    return _parse_legacy_document(document)


def _parse_legacy_document(
    document: dict[str, Any],
) -> tuple[str, dict[str, int], int, str | None]:
    metadata = document.get("metadata") or {}
    usage = _usage(metadata)
    messages = document.get("messages") or []
    steps = sum(
        1
        for message in messages
        if isinstance(message, dict) and message.get("role") == "assistant"
    )
    text, error = _message_content(messages)
    if metadata.get("status") == "error" and not error:
        error = text.strip() or "goose reported an error."
    return text, usage, steps, error


def _parse_stream_events(
    raw: str,
) -> tuple[str, dict[str, int], int, str | None] | None:
    events: list[dict[str, Any]] = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
        except ValueError:
            continue
        if isinstance(event, dict):
            events.append(event)
    if not events:
        return None
    if not any(
        event.get("type") in {"message", "complete", "error"} for event in events
    ):
        return None
    return _parse_stream_event_list(events)


class _StreamAccumulator:
    """Collect the final assistant message without keeping the whole stream."""

    def __init__(self) -> None:
        self.texts: dict[str, list[str]] = {}
        self.order: list[str] = []
        self.usage: dict[str, int] = {}
        self.error: str | None = None
        self.message_ids: set[str] = set()
        self.events = 0
        self._fallback_id = 0

    def add(self, event: dict[str, Any]) -> None:
        event_type = event.get("type")
        if event_type == "message":
            self.events += 1
            self._add_message(event.get("message"))
        elif event_type == "complete":
            self.usage = _usage(event)
        elif event_type == "error":
            message = event.get("message") or event.get("error")
            if isinstance(message, str):
                self.error = message

    def _add_message(self, message: Any) -> None:
        if not isinstance(message, dict) or message.get("role") != "assistant":
            return
        message_id = message.get("id")
        if not isinstance(message_id, str) or not message_id:
            self._fallback_id += 1
            message_id = f"message-{self._fallback_id}"
        if message_id not in self.message_ids:
            self.message_ids.add(message_id)
            self.order.append(message_id)
        for block in message.get("content") or []:
            if not isinstance(block, dict):
                continue
            block_type = block.get("type")
            if block_type == "text" and isinstance(block.get("text"), str):
                self.texts.setdefault(message_id, []).append(block["text"])
            elif block_type == "error" and isinstance(block.get("message"), str):
                self.error = block["message"]

    def result(self) -> tuple[str, dict[str, int], int, str | None]:
        text = ""
        for message_id in reversed(self.order):
            if self.texts.get(message_id):
                text = "".join(self.texts[message_id]).strip()
                break
        return text, self.usage, len(self.message_ids), self.error


def _parse_stream_event_list(
    events: list[dict[str, Any]],
) -> tuple[str, dict[str, int], int, str | None]:
    accumulator = _StreamAccumulator()
    for event in events:
        accumulator.add(event)
    return accumulator.result()


def parse_goose_output_file(
    path: Path,
) -> tuple[str, dict[str, int], int, str | None]:
    """Parse a goose result file without truncating large stream-json output.

    ``read_text_no_follow`` is deliberately bounded for small control files,
    but a long goose run writes one stream-json event per line and repeats the
    advertised tool list in each event. Reading only the prefix silently drops
    the final assistant message, so the file is consumed line by line instead.
    """
    stream = open_text_no_follow(path)
    if stream is None:
        return "", {}, 0, NO_OUTPUT_ERROR
    try:
        size = os.fstat(stream.fileno()).st_size
        if size > _MAX_STREAM_BYTES:
            return (
                "",
                {},
                0,
                f"goose output is too large to parse ({size} bytes; "
                f"limit {_MAX_STREAM_BYTES}).",
            )
        if size == 0:
            return "", {}, 0, NO_OUTPUT_ERROR

        accumulator = _StreamAccumulator()
        oversized = 0
        while True:
            line = stream.readline(_MAX_STREAM_LINE_BYTES)
            if line == "":
                break
            if not line.endswith("\n") and len(line) >= _MAX_STREAM_LINE_BYTES:
                # Discard the rest of an oversized line instead of letting it
                # exhaust worker memory. Report it after the scan.
                while True:
                    rest = stream.readline(_MAX_STREAM_LINE_BYTES)
                    if rest == "" or rest.endswith("\n"):
                        break
                oversized += 1
                continue
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except ValueError:
                continue
            if isinstance(event, dict):
                accumulator.add(event)

        if oversized:
            return (
                "",
                {},
                len(accumulator.message_ids),
                f"goose output contains {oversized} line(s) larger than "
                f"{_MAX_STREAM_LINE_BYTES} bytes.",
            )
        if accumulator.events == 0:
            if accumulator.usage or accumulator.error is not None:
                return accumulator.result()
            # Legacy bundles contain one JSON document instead of stream-json.
            return parse_goose_output(read_text_no_follow(path))
        return accumulator.result()
    finally:
        stream.close()


def parse_goose_run(
    workdir: Path,
    stem: str,
) -> tuple[str, dict[str, int], int, str | None]:
    """Parse ``<stem>.jsonl`` and fall back to legacy ``<stem>.json``."""
    text, usage, steps, error = parse_goose_output_file(
        workdir / ".kafkallm" / f"{stem}.jsonl"
    )
    if error == NO_OUTPUT_ERROR:
        text, usage, steps, error = parse_goose_output_file(
            workdir / ".kafkallm" / f"{stem}.json"
        )
    return text, usage, steps, error


def _message_content(messages: Any) -> tuple[str, str | None]:
    if not isinstance(messages, list):
        return "", None
    for message in reversed(messages):
        if not isinstance(message, dict) or message.get("role") != "assistant":
            continue
        texts: list[str] = []
        errors: list[str] = []
        for block in message.get("content") or []:
            if not isinstance(block, dict):
                continue
            if block.get("type") == "text" and isinstance(block.get("text"), str):
                texts.append(block["text"])
            elif block.get("type") == "error":
                message_text = block.get("message")
                if isinstance(message_text, str):
                    errors.append(message_text)
        if texts or errors:
            return "\n\n".join(texts).strip(), ("\n".join(errors).strip() or None)
    return "", None


def _usage(metadata: Any) -> dict[str, int]:
    if not isinstance(metadata, dict):
        return {}
    mapping = {
        "input_tokens": "input_tokens",
        "output_tokens": "output_tokens",
        "total_tokens": "total_tokens",
        "cache_read_input_tokens": "cache_read_tokens",
        "cache_write_input_tokens": "cache_write_tokens",
    }
    return {
        target: int(metadata[source])
        for source, target in mapping.items()
        if isinstance(metadata.get(source), int)
        and not isinstance(metadata.get(source), bool)
    }


def stream_event_log_records(
    event: dict[str, Any],
    prefix: str,
) -> list[tuple[str, str]]:
    """Turn one stream-json event into human-readable live activity records."""
    event_type = event.get("type")
    if event_type == "message":
        message = event.get("message")
        if not isinstance(message, dict):
            return []
        role = message.get("role")
        records: list[tuple[str, str]] = []
        for block in message.get("content") or []:
            if not isinstance(block, dict):
                continue
            block_type = block.get("type")
            if block_type in {"tool_result", "toolResponse"}:
                if block_type == "toolResponse":
                    result = block.get("toolResult") or {}
                    value = result.get("value") or {}
                    content = value.get("content")
                    if isinstance(content, list):
                        content = "\n".join(
                            item.get("text", "")
                            for item in content
                            if isinstance(item, dict) and item.get("type") == "text"
                        )
                    if not isinstance(content, str):
                        content = json.dumps(content, ensure_ascii=False)
                    if not content:
                        content = json.dumps(value, ensure_ascii=False)
                else:
                    content = block.get("content")
                    if not isinstance(content, str):
                        content = json.dumps(content, ensure_ascii=False)
                records.append((f"{prefix}.tool_result", content))
                continue
            if role != "assistant":
                continue
            if block_type == "thinking" and isinstance(block.get("thinking"), str):
                records.append((f"{prefix}.thinking", block["thinking"]))
            elif block_type == "text" and isinstance(block.get("text"), str):
                records.append((f"{prefix}.assistant", block["text"]))
            elif block_type in {"tool_use", "tool_call", "toolRequest"}:
                if block_type == "toolRequest":
                    call = block.get("toolCall") or {}
                    value = call.get("value") or {}
                    name = value.get("name")
                    tool_input = value.get("arguments")
                else:
                    name = block.get("name")
                    tool_input = block.get("input")
                records.append(
                    (
                        f"{prefix}.tool",
                        json.dumps(
                            {"name": name, "input": tool_input},
                            ensure_ascii=False,
                        ),
                    )
                )
            elif block_type == "error" and isinstance(block.get("message"), str):
                records.append((f"{prefix}.error", block["message"]))
        return records
    if event_type == "complete":
        usage = _usage(event)
        total = usage.get("total_tokens")
        summary = (
            f"tokens total={total} input={usage.get('input_tokens', 0)} "
            f"output={usage.get('output_tokens', 0)} "
            f"cache_read={usage.get('cache_read_tokens', 0)}"
            if total is not None
            else "goose run complete"
        )
        return [(f"{prefix}.usage", summary)]
    if event_type == "error":
        message = event.get("message") or event.get("error")
        if isinstance(message, str):
            return [(f"{prefix}.error", message)]
    return []
