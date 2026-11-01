from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from openai import OpenAI

from kafkallm.settings import Settings


@dataclass(frozen=True, slots=True)
class ToolCall:
    id: str
    name: str
    arguments_json: str


@dataclass(slots=True)
class LLMReply:
    content: str | None = None
    tool_calls: list[ToolCall] = field(default_factory=list)
    usage: dict[str, int] = field(default_factory=dict)

    def assistant_message(self) -> dict[str, Any]:
        message: dict[str, Any] = {"role": "assistant", "content": self.content}
        if self.tool_calls:
            message["tool_calls"] = [
                {
                    "id": call.id,
                    "type": "function",
                    "function": {
                        "name": call.name,
                        "arguments": call.arguments_json,
                    },
                }
                for call in self.tool_calls
            ]
        return message


class LLM(Protocol):
    def complete(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        timeout_seconds: float,
    ) -> LLMReply: ...


class EchoLLM:
    def complete(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        timeout_seconds: float,
    ) -> LLMReply:
        del tools, timeout_seconds
        content = next(
            (
                message.get("content", "")
                for message in reversed(messages)
                if message.get("role") == "user"
            ),
            "",
        )
        return LLMReply(content=f"Echo: {content}")


class OpenAICompatibleLLM:
    def __init__(self, model: str, api_key: str, base_url: str = "") -> None:
        if not model:
            raise ValueError("LLM_MODEL muss für den OpenAI-Provider gesetzt sein.")
        if not api_key:
            raise ValueError("LLM_API_KEY muss für den OpenAI-Provider gesetzt sein.")
        options: dict[str, Any] = {"api_key": api_key, "max_retries": 0}
        if base_url:
            options["base_url"] = base_url
        self.model = model
        self.client = OpenAI(**options)

    def complete(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        timeout_seconds: float,
    ) -> LLMReply:
        response = self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            tools=tools,
            timeout=max(1.0, timeout_seconds),
        )
        message = response.choices[0].message
        calls = [
            ToolCall(
                id=call.id,
                name=call.function.name,
                arguments_json=call.function.arguments,
            )
            for call in (message.tool_calls or [])
        ]
        usage: dict[str, int] = {}
        if response.usage:
            usage = {
                "prompt_tokens": response.usage.prompt_tokens,
                "completion_tokens": response.usage.completion_tokens,
                "total_tokens": response.usage.total_tokens,
            }
        return LLMReply(content=message.content, tool_calls=calls, usage=usage)


def build_llm(settings: Settings) -> LLM:
    if settings.llm_provider == "echo":
        return EchoLLM()
    if settings.llm_provider in {"openai", "openai-compatible"}:
        api_key = settings.llm_api_key
        if not api_key and settings.llm_base_url:
            api_key = "local"
        return OpenAICompatibleLLM(
            model=settings.llm_model,
            api_key=api_key,
            base_url=settings.llm_base_url,
        )
    raise ValueError(f"Unbekannter LLM_PROVIDER: {settings.llm_provider}")
