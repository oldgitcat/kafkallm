from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any

from opentelemetry.trace import Status, StatusCode

from hermes.audit import JobAudit
from hermes.llm import LLM
from hermes.telemetry import get_tracer, set_content, set_error
from hermes.tools import ToolError, ToolRegistry


SYSTEM_PROMPT = """You are an agent inside a restricted job workspace.
Input files are located under inputs/. Save generated result files under outputs/.
Use only the provided tools. Verify tool results and never claim that an action
succeeded if it failed. Finish with the actual result.
"""


@dataclass(slots=True)
class AgentResult:
    text: str
    steps: int
    usage: dict[str, int] = field(default_factory=dict)


def _elapsed_ms(started: float) -> float:
    return round((time.perf_counter() - started) * 1000, 3)


class AgentRunner:
    def __init__(
        self,
        llm: LLM,
        tools: ToolRegistry,
        audit: JobAudit | None = None,
        provider_name: str = "unknown",
        model_name: str = "unknown",
    ) -> None:
        self.llm = llm
        self.tools = tools
        self.audit = audit
        self.provider_name = provider_name
        self.model_name = model_name

    def run(
        self,
        instruction: str,
        job_input: dict[str, Any],
        deadline: float,
        max_steps: int,
    ) -> AgentResult:
        messages: list[dict[str, Any]] = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {
                "role": "user",
                "content": json.dumps(
                    {"instruction": instruction, "input": job_input},
                    ensure_ascii=False,
                ),
            },
        ]
        usage: dict[str, int] = {}
        parent_event_id: str | None = None

        for step in range(1, max_steps + 1):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("The job timeout was reached.")

            llm_started = time.perf_counter()
            with get_tracer().start_as_current_span(
                "llm.complete",
                attributes={
                    "openinference.span.kind": "LLM",
                    "llm.provider": self.provider_name,
                    "llm.model_name": self.model_name,
                    "agent.step": step,
                },
            ) as span:
                set_content(
                    span,
                    "input.value",
                    json.dumps(messages, ensure_ascii=False),
                )
                try:
                    reply = self.llm.complete(
                        messages=messages,
                        tools=self.tools.specifications,
                        timeout_seconds=remaining,
                    )
                except Exception as exc:
                    set_error(span, exc)
                    if self.audit:
                        self.audit.emit(
                            "llm.completed",
                            "failed",
                            parent_event_id=parent_event_id,
                            duration_ms=_elapsed_ms(llm_started),
                            attributes={
                                "provider": self.provider_name,
                                "model": self.model_name,
                                "step": step,
                            },
                            error=str(exc),
                        )
                    raise

                set_content(span, "output.value", reply.content)
                for key, value in reply.usage.items():
                    metric = key.replace("_tokens", "")
                    span.set_attribute(f"llm.token_count.{metric}", value)
                span.set_attribute("llm.tool_call.count", len(reply.tool_calls))

            if self.audit:
                parent_event_id = self.audit.emit(
                    "llm.completed",
                    "succeeded",
                    parent_event_id=parent_event_id,
                    duration_ms=_elapsed_ms(llm_started),
                    attributes={
                        "provider": self.provider_name,
                        "model": self.model_name,
                        "step": step,
                        "tool_calls": len(reply.tool_calls),
                        "content_bytes": len((reply.content or "").encode("utf-8")),
                        "usage": reply.usage,
                    },
                )

            messages.append(reply.assistant_message())
            for key, value in reply.usage.items():
                usage[key] = usage.get(key, 0) + value

            if not reply.tool_calls:
                if reply.content is None or not reply.content.strip():
                    raise RuntimeError("The LLM returned an empty response.")
                if len(reply.content.encode("utf-8")) > 500_000:
                    raise RuntimeError(
                        "The text result is too large for jobs.out; "
                        "save it under outputs/ instead."
                    )
                return AgentResult(text=reply.content, steps=step, usage=usage)

            for call in reply.tool_calls:
                if time.monotonic() > deadline:
                    raise TimeoutError("The job timeout was reached.")

                tool_started = time.perf_counter()
                with get_tracer().start_as_current_span(
                    f"tool.{call.name}",
                    attributes={
                        "openinference.span.kind": "TOOL",
                        "tool.name": call.name,
                        "tool.call_id": call.id,
                        "agent.step": step,
                    },
                ) as span:
                    set_content(span, "input.value", call.arguments_json)
                    try:
                        output = self.tools.execute(call.name, call.arguments_json)
                        if "error" in output:
                            span.set_status(
                                Status(StatusCode.ERROR, str(output["error"]))
                            )
                    except (ToolError, UnicodeError, OSError) as exc:
                        output = {"error": str(exc)}
                        set_error(span, exc)
                    except Exception as exc:
                        set_error(span, exc)
                        if self.audit:
                            self.audit.emit(
                                "tool.completed",
                                "failed",
                                parent_event_id=parent_event_id,
                                duration_ms=_elapsed_ms(tool_started),
                                attributes={
                                    "tool": call.name,
                                    "call_id": call.id,
                                    "step": step,
                                },
                                error=str(exc),
                            )
                        raise
                    set_content(
                        span,
                        "output.value",
                        json.dumps(output, ensure_ascii=False),
                    )

                tool_status = "failed" if "error" in output else "succeeded"
                if self.audit:
                    parent_event_id = self.audit.emit(
                        "tool.completed",
                        tool_status,
                        parent_event_id=parent_event_id,
                        duration_ms=_elapsed_ms(tool_started),
                        attributes={
                            "tool": call.name,
                            "call_id": call.id,
                            "step": step,
                            "argument_bytes": len(
                                call.arguments_json.encode("utf-8")
                            ),
                        },
                        error=str(output.get("error", "")) or None,
                    )
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": call.id,
                        "content": json.dumps(output, ensure_ascii=False),
                    }
                )

        raise RuntimeError(f"The agent step limit of {max_steps} was reached.")
