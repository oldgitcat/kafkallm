import json
import time
from pathlib import Path
from typing import Any

from hermes.agent import AgentRunner
from hermes.llm import LLMReply, ToolCall
from hermes.tools import ToolRegistry, Workspace


class ToolUsingLLM:
    def __init__(self) -> None:
        self.calls = 0

    def complete(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        timeout_seconds: float,
    ) -> LLMReply:
        del tools, timeout_seconds
        self.calls += 1
        if self.calls == 1:
            return LLMReply(
                tool_calls=[
                    ToolCall(
                        id="call-1",
                        name="write_workspace_file",
                        arguments_json=json.dumps(
                            {"path": "outputs/result.txt", "content": "Result"}
                        ),
                    )
                ],
                usage={"total_tokens": 3},
            )
        assert messages[-1]["role"] == "tool"
        return LLMReply(content="File created.", usage={"total_tokens": 2})


def test_agent_executes_tool_and_returns_result(tmp_path: Path) -> None:
    workspace = Workspace(tmp_path)
    runner = AgentRunner(ToolUsingLLM(), ToolRegistry(workspace))

    result = runner.run(
        instruction="Save the result",
        job_input={},
        deadline=time.monotonic() + 10,
        max_steps=3,
    )

    assert result.text == "File created."
    assert result.steps == 2
    assert result.usage == {"total_tokens": 5}
    assert workspace.read("outputs/result.txt") == "Result"
