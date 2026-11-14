from __future__ import annotations

import json
import logging
import os
import re
from pathlib import Path
from typing import Any

import yaml
from gaensestall.core.models import ID_PATTERN, AgentSpec
from gaensestall.core.settings import Settings
from gaensestall.core.slugs import slugify
from gaensestall.kafka.client import KafkaProducer
from gaensestall.kafka.consumer import CompactedStore, read_compacted

logger = logging.getLogger(__name__)

_ROLE_ORDER = {
    "orchestrator": 0,
    "researcher": 1,
    "worker": 2,
    "writer": 3,
    "reviewer": 4,
}


def _default_agents_dir() -> Path:
    """Directory with the built-in goosetown agent profiles."""
    return Path(os.getenv("GOOSE_JOB_DEFAULT_AGENTS_DIR") or "goose-assets/agents")


def default_agents() -> list[AgentSpec]:
    """Load the built-in flock from the agent profile files.

    The repository ships ``goose-assets/agents/*.md``; the app image copies them to
    ``agents/*.md``. They use the same format and parser as custom agents, so
    the definition lives in data rather than in Python.
    """
    directory = _default_agents_dir()
    if not directory.is_dir():
        logger.error("Default agents directory %s does not exist", directory)
        return []
    agents: list[AgentSpec] = []
    for path in sorted(directory.glob("*.md")):
        agent = agent_spec_from_file(path)
        if agent is not None:
            agents.append(agent)
    agents.sort(key=lambda agent: (_ROLE_ORDER.get(agent.role, 99), agent.name))
    return agents


def default_agent_ids() -> set[str]:
    """IDs of the built-in flock; the GUI protects them from deletion."""
    return {agent.agent_id for agent in default_agents()}


def new_agent_id(name: str) -> str:
    return slugify(name, prefix="agent")


def _split_front_matter(text: str) -> tuple[dict[str, Any], str]:
    """Split the YAML front matter block from an agent profile file."""
    if not text.startswith("---"):
        return {}, text.strip()
    lines = text.splitlines()
    if lines[0].strip() != "---":
        return {}, text.strip()
    end = next(
        (index for index in range(1, len(lines)) if lines[index].strip() == "---"),
        None,
    )
    if end is None:
        return {}, text.strip()
    try:
        metadata = yaml.safe_load("\n".join(lines[1:end])) or {}
    except yaml.YAMLError:
        return {}, text.strip()
    if not isinstance(metadata, dict):
        return {}, text.strip()
    return metadata, "\n".join(lines[end + 1 :]).strip()


def agent_spec_from_file(path: Path) -> AgentSpec | None:
    """Parse one ``*.md`` agent profile, or return ``None`` if it is invalid."""
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        logger.warning("Could not read agent profile %s", path)
        return None
    meta, body = _split_front_matter(text)
    name = str(meta.get("name") or "").strip()
    if not meta or not name:
        logger.warning("Skipping agent profile %s: missing front matter or name", path)
        return None
    agent_id = path.stem
    if not re.fullmatch(ID_PATTERN, agent_id):
        agent_id = new_agent_id(name)
    skills = meta.get("skills") or []
    if isinstance(skills, str):
        skills = [skills]
    instructions = str(meta.get("instructions") or body or "").strip()
    try:
        return AgentSpec(
            agent_id=agent_id,
            name=name,
            role=str(meta.get("role") or "custom").strip() or "custom",
            description=str(meta.get("description") or "").strip(),
            instructions=instructions,
            skills=[str(skill).strip() for skill in skills if str(skill).strip()],
            enabled=bool(meta.get("enabled", True)),
        )
    except ValueError:
        logger.warning("Skipping invalid agent profile %s", path)
        return None


def import_agents_from_dir(settings: Settings, publisher: AgentPublisher) -> int:
    """Upsert agent profiles from ``GOOSE_JOB_AGENTS_DIR``.

    The filename (without ``.md``) is the ``agent_id``. Files are the durable
    source for custom agents and for edits to the seeded built-in profiles, so
    they win over the registry entry with the same ID.
    """
    directory = Path(settings.goose_job_agents_dir)
    if not directory.is_dir():
        return 0
    imported = 0
    for path in sorted(directory.glob("*.md")):
        agent = agent_spec_from_file(path)
        if agent is None:
            continue
        publisher.save(agent)
        imported += 1
    if imported:
        logger.info("Imported %s agent profile(s) from %s", imported, directory)
    return imported


def agent_profile_text(agent: AgentSpec) -> str:
    """Render an agent profile in the markdown format the importer reads."""
    lines = [
        "---",
        f"name: {json.dumps(agent.name)}",
        f"role: {json.dumps(agent.role)}",
        f"description: {json.dumps(agent.description)}",
    ]
    if agent.skills:
        lines.append("skills:")
        lines.extend(f"  - {json.dumps(skill)}" for skill in agent.skills)
    else:
        lines.append("skills: []")
    lines.append(f"enabled: {'true' if agent.enabled else 'false'}")
    lines.extend(["---", "", agent.instructions.strip(), ""])
    return "\n".join(lines)


def agent_profile_path(settings: Settings, agent_id: str) -> Path:
    return Path(settings.goose_job_agents_dir) / f"{agent_id}.md"


def write_agent_profile(settings: Settings, agent: AgentSpec) -> Path:
    """Write one custom agent profile atomically and return its path."""
    path = agent_profile_path(settings, agent.agent_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(agent_profile_text(agent), encoding="utf-8")
    temporary.replace(path)
    return path


def export_agent_profiles(
    settings: Settings,
    agents: dict[str, AgentSpec] | None = None,
) -> int:
    """Create a profile file for custom registry agents that have none yet.

    Built-in roles are never written. Existing files are left untouched, so the
    directory remains the durable source for custom agents.
    """
    if agents is None:
        agents = _existing_agents(settings)
    builtin = default_agent_ids()
    written = 0
    for agent_id, agent in agents.items():
        if agent_id in builtin:
            continue
        if agent_profile_path(settings, agent_id).exists():
            continue
        write_agent_profile(settings, agent)
        written += 1
    if written:
        logger.info(
            "Exported %s custom agent profile(s) to %s",
            written,
            settings.goose_job_agents_dir,
        )
    return written


class AgentPublisher:
    def __init__(self, settings: Settings, producer: KafkaProducer) -> None:
        self.settings = settings
        self.producer = producer

    def save(self, agent: AgentSpec) -> None:
        self.producer.send(
            topic=self.settings.agents_topic,
            key=agent.agent_id,
            model=agent,
        )


def _existing_agents(settings: Settings, timeout: float = 2.0) -> dict[str, AgentSpec]:
    """Read the compacted agent registry from the beginning."""
    return read_compacted(
        settings,
        settings.agents_topic,
        AgentSpec.model_validate_json,
        timeout=timeout,
    )


def _existing_agent_ids(settings: Settings, timeout: float = 2.0) -> set[str]:
    return set(_existing_agents(settings, timeout=timeout))


def seed_default_agents(
    settings: Settings,
    producer: KafkaProducer,
    existing: set[str] | None = None,
) -> None:
    """Create the standard goosetown flock on the first GUI start."""
    if existing is None:
        existing = _existing_agent_ids(settings)
    publisher = AgentPublisher(settings, producer)
    seeded = 0
    for agent in default_agents():
        if agent.agent_id in existing:
            continue
        publisher.save(agent)
        seeded += 1
    if seeded:
        logger.info("Seeded %s default goosetown agents", seeded)


def seed_default_agent_profiles(settings: Settings) -> int:
    """Copy the built-in agent profiles into GOOSE_JOB_AGENTS_DIR if missing.

    After the first GUI start every agent, including the built-in flock, has a
    file in the same directory as custom agents. Existing files are left alone
    so operators can edit them.
    """
    source = _default_agents_dir()
    if not source.is_dir():
        logger.error("Default agents directory %s does not exist", source)
        return 0
    target = Path(settings.goose_job_agents_dir)
    target.mkdir(parents=True, exist_ok=True)
    written = 0
    for path in sorted(source.glob("*.md")):
        destination = target / path.name
        if destination.exists():
            continue
        destination.write_bytes(path.read_bytes())
        written += 1
    if written:
        logger.info("Seeded %s built-in agent profile(s) into %s", written, target)
    return written


def sync_agents(settings: Settings, producer: KafkaProducer) -> None:
    """Seed profile files, seed the registry, export, then import files."""
    seed_default_agent_profiles(settings)
    existing = _existing_agents(settings)
    seed_default_agents(settings, producer, existing=set(existing))
    export_agent_profiles(settings, existing)
    import_agents_from_dir(settings, AgentPublisher(settings, producer))


class AgentStore(CompactedStore[AgentSpec]):
    """In-memory view of the compacted agent registry topic."""

    def __init__(self, settings: Settings) -> None:
        super().__init__(
            settings,
            settings.agents_topic,
            AgentSpec,
            group_prefix="gaensestall-gui-agents",
            key_attr="agent_id",
            label="agent",
        )

    def agents(self) -> list[AgentSpec]:
        agents = self.values()
        agents.sort(key=lambda agent: (_ROLE_ORDER.get(agent.role, 99), agent.name))
        return agents
