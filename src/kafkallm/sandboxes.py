from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException

from kafkallm.kafka import KafkaProducer
from kafkallm.models import SandboxSpec, utc_now
from kafkallm.settings import Settings

logger = logging.getLogger(__name__)

_ORDER = {"default": 0, "reverse": 1}


def default_sandboxes(settings: Settings) -> list[SandboxSpec]:
    now = utc_now()
    return [
        SandboxSpec(
            sandbox_id="default",
            name="Default",
            description=("goose + goosetown with the standard developer toolchain."),
            image=settings.goose_image,
            updated_at=now,
        ),
        SandboxSpec(
            sandbox_id="reverse",
            name="Reverse engineering",
            description=(
                "REA as an MCP server, Ghidra, GDB, binwalk, YARA, and Python "
                "reverse-engineering libraries."
            ),
            image=settings.goose_re_image,
            instructions=(
                "Reverse-engineering sandbox: the local REA MCP server is "
                "enabled as 'rea'. Use it to inspect binaries, firmware, "
                "JavaScript/Electron apps, .NET assemblies, APKs, and other "
                "artifacts. Ghidra is installed at /opt/ghidra and the `rea` "
                "CLI is on PATH. Kuna is available as an alternative "
                "decompiler through REA, JADX handles Android APKs, and Frida "
                "is available for local dynamic instrumentation. Prefer REA's "
                "structured tools over ad-hoc disassembly, and cite the input "
                "paths and evidence in your result."
            ),
            env={
                "GHIDRA_INSTALL_DIR": "/opt/ghidra",
                "JAVA_HOME": "/opt/java/openjdk",
                "REA_KUNA_COMMAND": "/usr/local/bin/kuna",
                "REA_JADX_MCP_JAR": ("/opt/kafkallm/jadx-headless-mcp-all.jar"),
                # Large binaries can take a while; the job deadline still caps
                # the total run.
                "REA_GHIDRA_STARTUP_TIMEOUT_MS": "900000",
                "REA_MCP_INPUT_SCHEMA_PROFILE": "compact",
                "REA_MCP_OUTPUT_SCHEMA_PROFILE": "off",
            },
            extensions=["rea:/opt/kafkallm/rea-mcp"],
            updated_at=now,
        ),
    ]


def builtin_sandbox(sandbox_id: str, settings: Settings) -> SandboxSpec | None:
    """Return a built-in sandbox for jobs submitted before the registry exists."""
    for sandbox in default_sandboxes(settings):
        if sandbox.sandbox_id == sandbox_id:
            return sandbox
    return None


def new_sandbox_id(name: str) -> str:
    slug = "".join(
        char if char.isalnum() else "-" for char in name.strip().lower()
    ).strip("-")
    slug = "-".join(part for part in slug.split("-") if part)[:64]
    return slug or f"sandbox-{uuid.uuid4().hex[:8]}"


class SandboxPublisher:
    def __init__(self, settings: Settings, producer: KafkaProducer) -> None:
        self.settings = settings
        self.producer = producer

    def save(self, sandbox: SandboxSpec) -> None:
        self.producer.send(
            topic=self.settings.sandboxes_topic,
            key=sandbox.sandbox_id,
            model=sandbox,
        )


def _existing_sandbox_ids(settings: Settings, timeout: float = 2.0) -> set[str]:
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": f"kafkallm-sandbox-seed-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([settings.sandboxes_topic])
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


def seed_default_sandboxes(
    settings: Settings,
    producer: KafkaProducer,
) -> None:
    """Create the standard environments once, on the first GUI start."""
    if _existing_sandbox_ids(settings):
        return
    publisher = SandboxPublisher(settings, producer)
    for sandbox in default_sandboxes(settings):
        publisher.save(sandbox)
    logger.info("Seeded %s default sandboxes", len(default_sandboxes(settings)))


class SandboxStore:
    """In-memory view of the compacted sandbox registry topic."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._sandboxes: dict[str, SandboxSpec] = {}
        self._condition = threading.Condition()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._consume,
            name="kafkallm-gui-sandboxes",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def sandboxes(self) -> list[SandboxSpec]:
        with self._condition:
            sandboxes = list(self._sandboxes.values())
        sandboxes.sort(
            key=lambda sandbox: (
                _ORDER.get(sandbox.sandbox_id, 99),
                sandbox.name.lower(),
            )
        )
        return sandboxes

    def get(self, sandbox_id: str) -> SandboxSpec | None:
        with self._condition:
            return self._sandboxes.get(sandbox_id)

    def wait_for(self, sandbox_id: str, timeout: float = 3.0) -> SandboxSpec | None:
        with self._condition:
            self._condition.wait_for(
                lambda: sandbox_id in self._sandboxes,
                timeout=timeout,
            )
            return self._sandboxes.get(sandbox_id)

    def put(self, sandbox: SandboxSpec) -> None:
        with self._condition:
            self._sandboxes[sandbox.sandbox_id] = sandbox
            self._condition.notify_all()

    def remove(self, sandbox_id: str) -> None:
        with self._condition:
            self._sandboxes.pop(sandbox_id, None)
            self._condition.notify_all()

    def _consume(self) -> None:
        while not self._stop.is_set():
            consumer = Consumer(
                {
                    "bootstrap.servers": self.settings.kafka_bootstrap_servers,
                    "group.id": f"kafkallm-gui-sandboxes-{uuid.uuid4()}",
                    "auto.offset.reset": "earliest",
                    "enable.auto.commit": False,
                    "session.timeout.ms": 15000,
                }
            )
            consumer.subscribe([self.settings.sandboxes_topic])
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
                        sandbox = SandboxSpec.model_validate(payload)
                    except (UnicodeDecodeError, ValueError):
                        logger.warning("Skipping malformed sandbox record")
                        continue
                    self.put(sandbox)
            except Exception:
                logger.exception("GUI sandbox consumer failed; reconnecting")
                self._stop.wait(2)
            finally:
                consumer.close()
