from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException

from kafkallm.kafka import KafkaProducer
from kafkallm.models import ID_PATTERN, AgentSpec, utc_now
from kafkallm.settings import Settings

logger = logging.getLogger(__name__)

_ROLE_ORDER = {
    "orchestrator": 0,
    "researcher": 1,
    "worker": 2,
    "writer": 3,
    "reviewer": 4,
}


def default_agents() -> list[AgentSpec]:
    now = utc_now()
    return [
        AgentSpec(
            agent_id="orchestrator",
            name="Orchestrator",
            role="orchestrator",
            description="Decomposes the job, dispatches delegates, and synthesizes the result.",
            instructions=(
                "You are the main goosetown session. Read the job instruction and "
                "input files, choose the smallest useful decomposition, dispatch "
                "delegates for independent work, and synthesize the final result. "
                "Do not delegate trivial single-step work."
            ),
            skills=["goosetown-orchestrator"],
            updated_at=now,
        ),
        AgentSpec(
            agent_id="researcher-local",
            name="Local researcher",
            role="researcher",
            description="Searches inputs, prior notes, scratch files, and skills. Read-only.",
            instructions=(
                "Search the job inputs, WORK_LOGS, .scratch, and .claude/skills for "
                "relevant prior context. Return concise findings with file paths. "
                "Never edit source or output files."
            ),
            skills=["goosetown-researcher-local"],
            updated_at=now,
        ),
        AgentSpec(
            agent_id="worker",
            name="Worker",
            role="worker",
            description="Builds code, scripts, configuration, and data files.",
            instructions=(
                "Implement one concrete piece of the job. Own only the files assigned "
                "to you, write incrementally, verify what you build, and report the "
                "exact output paths."
            ),
            skills=["goosetown-worker"],
            updated_at=now,
        ),
        AgentSpec(
            agent_id="writer",
            name="Writer",
            role="writer",
            description="Synthesizes research and source material into one document.",
            instructions=(
                "Turn the supplied sources and findings into a single coherent "
                "deliverable. State assumptions and cite the input paths you used."
            ),
            skills=["goosetown-writer"],
            updated_at=now,
        ),
        AgentSpec(
            agent_id="reviewer",
            name="Reviewer",
            role="reviewer",
            description="Reviews an output against explicit criteria. Read-only.",
            instructions=(
                "Review the assigned output against the stated acceptance criteria. "
                "Report only material issues, with file and line references. Never "
                "edit the output."
            ),
            skills=["goosetown-reviewer"],
            updated_at=now,
        ),
    ]


def default_agent_ids() -> set[str]:
    """IDs of the built-in flock; these cannot be edited or overridden."""
    return {agent.agent_id for agent in default_agents()}


def new_agent_id(name: str) -> str:
    slug = "".join(
        char if char.isalnum() else "-" for char in name.strip().lower()
    ).strip("-")
    slug = "-".join(part for part in slug.split("-") if part)[:64]
    return slug or f"agent-{uuid.uuid4().hex[:8]}"


def _split_front_matter(text: str) -> tuple[dict[str, Any], str]:
    """Split a small YAML front matter block from an agent profile file.

    The parser intentionally supports only the flat subset used by agent
    profiles: ``key: value``, booleans, and simple or block lists. That keeps
    the control plane free of a YAML dependency.
    """
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
    return _parse_flat_yaml(lines[1:end]), "\n".join(lines[end + 1 :]).strip()


def _parse_flat_yaml(lines: list[str]) -> dict[str, Any]:
    data: dict[str, Any] = {}
    current_list: str | None = None
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("- ") and current_list is not None:
            data[current_list].append(_scalar(line[2:]))
            continue
        current_list = None
        key, separator, value = line.partition(":")
        if not separator:
            continue
        key = key.strip()
        value = value.strip()
        if not value:
            data[key] = []
            current_list = key
        elif value.startswith("[") and value.endswith("]"):
            inner = value[1:-1].strip()
            data[key] = [_scalar(item) for item in inner.split(",")] if inner else []
        else:
            data[key] = _scalar(value)
    return data


def _scalar(value: str) -> Any:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        if value[0] == '"':
            try:
                return json.loads(value)
            except ValueError:
                return value[1:-1]
        return value[1:-1]
    if value.lower() in {"true", "false"}:
        return value.lower() == "true"
    return value


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
    """Upsert custom agent profiles from ``GOOSE_JOB_AGENTS_DIR``.

    The filename (without ``.md``) is the ``agent_id``. Built-in roles are
    skipped: they are fixed in code and cannot be overridden by files. Files are
    the durable source for custom agents, so they win over the registry entry
    with the same ID.
    """
    directory = Path(settings.goose_job_agents_dir)
    if not directory.is_dir():
        return 0
    builtin = default_agent_ids()
    imported = 0
    for path in sorted(directory.glob("*.md")):
        if path.stem in builtin:
            logger.warning(
                "Ignoring agent profile %s: %s is a built-in agent", path, path.stem
            )
            continue
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
    # The GUI runs under rootless Podman, where its UID maps to a host subuid.
    # A world-writable profile lets the host user edit the file directly.
    os.chmod(path, 0o666)
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
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": f"kafkallm-agent-seed-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([settings.agents_topic])
    found: dict[str, AgentSpec] = {}
    deadline = time.monotonic() + timeout
    try:
        while time.monotonic() < deadline:
            message = consumer.poll(timeout=0.25)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(message.error())
            key = message.key()
            if not isinstance(key, bytes) or not key:
                continue
            agent_id = key.decode("utf-8")
            value = message.value()
            if value is None:
                found.pop(agent_id, None)
                continue
            try:
                found[agent_id] = AgentSpec.model_validate_json(value)
            except ValueError:
                logger.warning("Skipping malformed agent record for %s", agent_id)
    finally:
        consumer.close()
    return found


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


def sync_agents(settings: Settings, producer: KafkaProducer) -> None:
    """Seed defaults, persist custom registry agents, then apply profile files."""
    existing = _existing_agents(settings)
    seed_default_agents(settings, producer, existing=set(existing))
    export_agent_profiles(settings, existing)
    import_agents_from_dir(settings, AgentPublisher(settings, producer))


class AgentStore:
    """In-memory view of the compacted agent registry topic."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._agents: dict[str, AgentSpec] = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._consume,
            name="kafkallm-gui-agents",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def agents(self) -> list[AgentSpec]:
        with self._lock:
            agents = list(self._agents.values())
        agents.sort(key=lambda agent: (_ROLE_ORDER.get(agent.role, 99), agent.name))
        return agents

    def get(self, agent_id: str) -> AgentSpec | None:
        with self._lock:
            return self._agents.get(agent_id)

    def put(self, agent: AgentSpec) -> None:
        with self._lock:
            self._agents[agent.agent_id] = agent

    def remove(self, agent_id: str) -> None:
        with self._lock:
            self._agents.pop(agent_id, None)

    def _consume(self) -> None:
        while not self._stop.is_set():
            consumer = Consumer(
                {
                    "bootstrap.servers": self.settings.kafka_bootstrap_servers,
                    "group.id": f"kafkallm-gui-agents-{uuid.uuid4()}",
                    "auto.offset.reset": "earliest",
                    "enable.auto.commit": False,
                    "session.timeout.ms": 15000,
                }
            )
            consumer.subscribe([self.settings.agents_topic])
            try:
                while not self._stop.is_set():
                    message = consumer.poll(timeout=1.0)
                    if message is None:
                        continue
                    if message.error():
                        if message.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        raise KafkaException(message.error())
                    value = message.value()
                    key = message.key()
                    if value is None:
                        if isinstance(key, bytes) and key:
                            self.remove(key.decode("utf-8"))
                        continue
                    try:
                        payload: Any = json.loads(value.decode("utf-8"))
                        agent = AgentSpec.model_validate(payload)
                    except (UnicodeDecodeError, ValueError):
                        logger.warning("Skipping malformed agent record")
                        continue
                    self.put(agent)
            except Exception:
                logger.exception("GUI agent consumer failed; reconnecting")
                self._stop.wait(2)
            finally:
                consumer.close()
