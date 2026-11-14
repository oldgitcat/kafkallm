"""Prompt and environment construction for goose jobs."""

from __future__ import annotations

import json

from kafkallm.core.models import AgentSpec, Job, SandboxSpec
from kafkallm.core.settings import Settings
from kafkallm.sandbox.errors import GooseJobRuntimeError

_MAIN_PATH = (
    "/opt/kafkallm/tools/bin:/opt/java/openjdk/bin:"
    "/opt/ghidra/support:/usr/local/goose/bin:"
    "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
)
_REFLECT_PATH = (
    "/usr/local/goose/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
)
_NO_PROXY = "127.0.0.1,localhost,host.containers.internal"


def agent_source(agent: AgentSpec) -> str:
    """Render one selected flock member as a goosetown agent source.

    goose's Summon extension discovers agent sources from `.claude/agents/*.md`
    and exposes them through `delegate(source: "<name>")`. The YAML values are
    JSON strings because JSON is valid YAML and safely quotes multiline text.
    """
    description = agent.description.strip() or f"{agent.role} delegate."
    instructions = agent.instructions.strip() or "No additional role instructions."
    if agent.skills:
        skills = "\n".join(
            f'- `{skill}`: call `load_skill(name: "{skill}")` before starting. '
            "If that tool is unavailable, read "
            f"`/workspace/.claude/skills/{skill}/SKILL.md` and follow it."
            for skill in agent.skills
        )
    else:
        skills = "- No reusable skill is assigned."
    return (
        "---\n"
        f"name: {json.dumps(agent.name)}\n"
        f"description: {json.dumps(description)}\n"
        "---\n\n"
        f"You are {agent.name}, the {agent.role} delegate in one isolated "
        "Gänsestall job.\n\n"
        f"{description}\n\n"
        "Role instructions:\n"
        f"{instructions}\n\n"
        "Reusable skills:\n"
        f"{skills}\n\n"
        "Coordination:\n"
        "- Your gtwall ID is provided in the delegate instructions. FIRST "
        "ACTION: run `./gtwall --usage`, then post and read updates with "
        "`./gtwall <your-id>`.\n"
        "- Work only on the files assigned by the orchestrator. Other delegates "
        "may be working in parallel, so never edit files they own.\n"
        "- Write results incrementally to the exact output path assigned to "
        "you, and report that path when you finish.\n"
        "- You cannot spawn further delegates.\n"
        "- There is no web access. Use only /workspace and the installed "
        "command-line tools.\n"
    )


def build_prompt(job: Job, sandbox: SandboxSpec | None) -> str:
    if job.multi_agent:
        role = (
            "You are the goosetown orchestrator for one isolated Gänsestall "
            "job. Follow the goosetown-orchestrator skill.\n"
        )
        delegates = [agent for agent in job.agents if agent.role != "orchestrator"]
        if delegates:
            flock = "\n".join(
                f"- {agent.name} ({agent.role}): {agent.description}"
                for agent in delegates
            )
            mode = (
                "Goosetown multi-agent mode is ON. The summon extension "
                "provides the `delegate` and `load` tools.\n"
                "The selected flock is installed as agent sources in "
                "/workspace/.claude/agents. Call `load()` once to list "
                "them.\n"
                f"Selected flock:\n{flock}\n"
                "For each independent piece, call "
                '`delegate(source: "<exact role name>", '
                'instructions: "...")`. Give the delegate the gtwall '
                "preamble, a unique gtwall ID, exact files it owns, and "
                "acceptance criteria. Use `load(source: ...)` only to read "
                "a role into your own context. You can also use "
                "`delegate(instructions: ...)` for ad-hoc work.\n"
                "Read /jobboot/agents.json for the complete selected "
                "flock metadata. Delegate nontrivial artifacts and use a "
                "second role to review them; only do truly trivial "
                "single-step work yourself.\n"
            )
        else:
            mode = (
                "Goosetown multi-agent mode is ON. The summon extension "
                "provides the `delegate` and `load` tools. No flock roles "
                "were selected, so use `delegate(instructions: ...)` with "
                "the built-in goosetown role skills (researcher-local, "
                "worker, writer, reviewer) and load those skills in the "
                "delegate task.\n"
            )
    else:
        role = "You are the goose agent for one isolated Gänsestall job.\n"
        mode = (
            "Goosetown multi-agent mode is OFF. Do the job yourself; do "
            "not delegate or spawn subagents.\n"
        )
    if sandbox is not None and sandbox.instructions.strip():
        sandbox_note = f"{sandbox.instructions.strip()}\n"
    else:
        sandbox_note = ""
    return (
        role
        + mode
        + sandbox_note
        + "Decompose the job when useful, coordinate through gtwall, and "
        "synthesize the final result.\n"
        "Workspace root: /workspace\n"
        "Input files: /workspace/inputs\n"
        "Write all generated result files to: /workspace/outputs\n"
        "Do not modify files outside /workspace. The skills in "
        "/workspace/.claude/skills are a disposable copy of the shared "
        "skill store; the worker decides whether the learning pass keeps "
        "any changes.\n"
        "There is no web browsing. Your only network endpoint is the model "
        "gateway used by goose; do not try to reach any other host or "
        "internal service. Work only from the provided instruction and "
        "input files.\n"
        "Work autonomously; do not ask for approval or clarification.\n"
        "Return the completed result as your final response.\n\n"
        f"Instruction:\n{job.instruction}\n\n"
        "Structured job input:\n"
        f"{job.input}"
    )


def _gateway_url(settings: Settings) -> str:
    return settings.llm_gateway_url.rstrip("/").removesuffix("/v1")


def _base_environment(settings: Settings, path: str) -> dict[str, str]:
    return {
        "HOME": "/opt/data",
        "PATH": path,
        "GOOSE_PATH_ROOT": "/opt/data/goose",
        "GOOSE_MODE": "auto",
        "GOOSE_DISABLE_KEYRING": "1",
        "GOOSE_DISABLE_SESSION_NAMING": "true",
        "GOOSE_TELEMETRY_ENABLED": "false",
        "GOOSE_AUTO_COMPACT_THRESHOLD": "0.9",
        "GOOSE_PROVIDER": "openai",
        "OPENAI_HOST": _gateway_url(settings),
        # The gateway replaces this virtual key with the provider key.
        "OPENAI_API_KEY": settings.llm_gateway_virtual_key,
        "NO_PROXY": _NO_PROXY,
    }


def main_environment(
    settings: Settings,
    *,
    main_budget: int,
    sandbox: SandboxSpec,
    multi_agent: bool,
    agent_count: int,
    unlimited: bool,
) -> dict[str, str]:
    if not settings.gateway_configured:
        raise GooseJobRuntimeError(
            "The LLM gateway is not configured. Set LLM_GATEWAY_URL and "
            "LLM_GATEWAY_VIRTUAL_KEY for the worker."
        )
    environment = {
        **_base_environment(settings, _MAIN_PATH),
        "KAFKALLM_TOOLS_DIR": "/opt/kafkallm/tools",
        "KAFKALLM_CONFIG_DIR": "/opt/kafkallm/config",
        "KAFKALLM_AGENTS_PATH": "/jobboot/agents.json",
        "KAFKALLM_SANDBOX": sandbox.sandbox_id,
        "KAFKALLM_MULTI_AGENT": "1" if multi_agent else "0",
        "KAFKALLM_AGENT_COUNT": str(agent_count),
        "KAFKALLM_JOB_UNLIMITED": "1" if unlimited else "0",
        "GOOSE_MAX_TURNS": str(
            settings.unlimited_max_turns if unlimited else settings.goose_job_max_turns
        ),
        "GOOSE_SUBAGENT_MAX_TURNS": str(
            settings.unlimited_max_subagent_turns
            if unlimited
            else settings.goose_job_subagent_max_turns
        ),
        "GOOSE_MAX_CONCURRENT_SUBAGENTS": str(
            max(1, min(5, agent_count)) if agent_count else 5
        ),
        "KAFKALLM_JOB_MAIN_TIMEOUT": "0" if unlimited else str(main_budget + 10),
    }
    if sandbox.extensions:
        environment["KAFKALLM_SANDBOX_EXTENSIONS"] = ",".join(sandbox.extensions)
    # SandboxSpec rejects reserved key names and prefixes, so custom values
    # cannot override the gateway credentials or the runtime controls.
    environment.update(sandbox.env)
    if settings.goose_model:
        environment["GOOSE_MODEL"] = settings.goose_model
    return environment


def reflect_environment(settings: Settings, reflect_seconds: int) -> dict[str, str]:
    """Environment for the isolated learning pass.

    It deliberately contains no sandbox-defined variables and no job metadata.
    The pass only needs the gateway, the read-only outputs, and its own
    disposable skill copy.
    """
    environment = {
        **_base_environment(settings, _REFLECT_PATH),
        "GOOSE_REFLECT_MAX_TURNS": "20",
        "KAFKALLM_JOB_REFLECT_TIMEOUT": str(max(1, reflect_seconds - 5)),
    }
    if settings.goose_model:
        environment["GOOSE_MODEL"] = settings.goose_model
    return environment
