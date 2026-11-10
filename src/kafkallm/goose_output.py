from __future__ import annotations

import json
from typing import Any


def parse_goose_output(raw: str) -> tuple[str, dict[str, int], int, str | None]:
    """Parse goose output from either the legacy JSON or stream-json format.

    The runtime now requests line-delimited `stream-json` events, but old job
    bundles and fixtures still contain one JSON document. Both are accepted.
    """
    if not raw.strip():
        return "", {}, 0, "goose produced no output."
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


def _parse_stream_event_list(
    events: list[dict[str, Any]],
) -> tuple[str, dict[str, int], int, str | None]:
    texts: dict[str, list[str]] = {}
    order: list[str] = []
    usage: dict[str, int] = {}
    error: str | None = None
    message_ids: set[str] = set()
    fallback_id = 0
    for event in events:
        event_type = event.get("type")
        if event_type == "message":
            message = event.get("message")
            if not isinstance(message, dict) or message.get("role") != "assistant":
                continue
            message_id = message.get("id")
            if not isinstance(message_id, str) or not message_id:
                fallback_id += 1
                message_id = f"message-{fallback_id}"
            if message_id not in message_ids:
                message_ids.add(message_id)
                order.append(message_id)
            for block in message.get("content") or []:
                if not isinstance(block, dict):
                    continue
                block_type = block.get("type")
                if block_type == "text" and isinstance(block.get("text"), str):
                    texts.setdefault(message_id, []).append(block["text"])
                elif block_type == "error" and isinstance(block.get("message"), str):
                    error = block["message"]
        elif event_type == "complete":
            usage = _usage(event)
        elif event_type == "error":
            message = event.get("message") or event.get("error")
            if isinstance(message, str):
                error = message
    text = ""
    for message_id in reversed(order):
        if texts.get(message_id):
            text = "".join(texts[message_id]).strip()
            break
    return text, usage, len(message_ids), error


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
