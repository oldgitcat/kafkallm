from __future__ import annotations

import json
import logging
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
    """Upsert agent profiles from ``GOOSE_JOB_AGENTS_DIR``.

    The filename (without ``.md``) is the ``agent_id``. Files are the
    authoritative source for the IDs they define, so they win over an existing
    registry entry on every GUI start. Agents without a file are untouched.
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


def _existing_agent_ids(settings: Settings, timeout: float = 2.0) -> set[str]:
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": f"kafkallm-agent-seed-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([settings.agents_topic])
    found: set[str] = set()
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
            if isinstance(key, bytes) and key:
                found.add(key.decode("utf-8"))
    finally:
        consumer.close()
    return found


def seed_default_agents(
    settings: Settings,
    producer: KafkaProducer,
) -> None:
    """Create the standard goosetown flock on the first GUI start."""
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
    """Seed the defaults, then apply profiles from the host agents directory."""
    seed_default_agents(settings, producer)
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
