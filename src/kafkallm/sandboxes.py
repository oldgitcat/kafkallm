from __future__ import annotations

import logging

from kafkallm.kafka import KafkaProducer
from kafkallm.kafka_consumer import CompactedStore, read_compacted
from kafkallm.models import SandboxSpec, utc_now
from kafkallm.settings import Settings
from kafkallm.slugs import slugify

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


def resolve_sandbox(settings: Settings, sandbox_id: str) -> SandboxSpec | None:
    """Return the registry entry, or the built-in definition for a known ID."""
    found = read_compacted(
        settings,
        settings.sandboxes_topic,
        SandboxSpec.model_validate_json,
        timeout=2.0,
    )
    return found.get(sandbox_id) or builtin_sandbox(sandbox_id, settings)


def new_sandbox_id(name: str) -> str:
    return slugify(name, prefix="sandbox")


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
    return set(
        read_compacted(
            settings,
            settings.sandboxes_topic,
            SandboxSpec.model_validate_json,
            timeout=timeout,
        )
    )


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


class SandboxStore(CompactedStore[SandboxSpec]):
    """In-memory view of the compacted sandbox registry topic."""

    def __init__(self, settings: Settings) -> None:
        super().__init__(
            settings,
            settings.sandboxes_topic,
            SandboxSpec,
            group_prefix="kafkallm-gui-sandboxes",
            key_attr="sandbox_id",
            label="sandbox",
        )

    def sandboxes(self) -> list[SandboxSpec]:
        sandboxes = self.values()
        sandboxes.sort(
            key=lambda sandbox: (
                _ORDER.get(sandbox.sandbox_id, 99),
                sandbox.name.lower(),
            )
        )
        return sandboxes
