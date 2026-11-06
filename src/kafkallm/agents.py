from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException

from kafkallm.kafka import KafkaProducer
from kafkallm.models import AgentSpec, utc_now
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
    """Create the standard goosetown flock once, on the first GUI start."""
    if _existing_agent_ids(settings):
        return
    publisher = AgentPublisher(settings, producer)
    for agent in default_agents():
        publisher.save(agent)
    logger.info("Seeded %s default goosetown agents", len(default_agents()))


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
