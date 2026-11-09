from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from kafkallm.agents import (
    AgentStore,
    agent_spec_from_file,
    default_agents,
    import_agents_from_dir,
    new_agent_id,
)
from kafkallm.models import AgentSpec
from kafkallm.settings import Settings


def test_default_flock_contains_the_goosetown_roles() -> None:
    agents = default_agents()

    assert [agent.agent_id for agent in agents] == [
        "orchestrator",
        "researcher-local",
        "worker",
        "writer",
        "reviewer",
    ]
    assert all(agent.enabled for agent in agents)
    assert all(agent.skills for agent in agents)


def test_new_agent_id_is_a_stable_slug() -> None:
    assert new_agent_id("Security Reviewer!") == "security-reviewer"
    assert new_agent_id("!!!").startswith("agent-")


def test_agent_store_sorts_roles() -> None:
    store = AgentStore(Settings.from_env())
    store.put(
        AgentSpec(
            agent_id="custom",
            name="Custom",
            role="custom",
        )
    )
    store.put(
        AgentSpec(
            agent_id="reviewer",
            name="Reviewer",
            role="reviewer",
        )
    )
    store.put(
        AgentSpec(
            agent_id="orchestrator",
            name="Orchestrator",
            role="orchestrator",
        )
    )

    assert [agent.agent_id for agent in store.agents()] == [
        "orchestrator",
        "reviewer",
        "custom",
    ]


def test_agent_profile_file_becomes_a_registry_entry(tmp_path: Path) -> None:
    path = tmp_path / "security-reviewer.md"
    path.write_text(
        "---\n"
        'name: "Security reviewer"\n'
        "role: reviewer\n"
        'description: "Checks security-sensitive outputs."\n'
        "skills:\n"
        "  - goosetown-reviewer\n"
        "enabled: true\n"
        "---\n\n"
        "Review the assigned output for security issues.\n",
        encoding="utf-8",
    )

    agent = agent_spec_from_file(path)

    assert agent is not None
    assert agent.agent_id == "security-reviewer"
    assert agent.name == "Security reviewer"
    assert agent.role == "reviewer"
    assert agent.skills == ["goosetown-reviewer"]
    assert agent.instructions.startswith("Review the assigned output")


def test_agent_profile_file_supports_inline_lists(tmp_path: Path) -> None:
    path = tmp_path / "writer.md"
    path.write_text(
        "---\n"
        "name: Writer\n"
        "role: writer\n"
        "skills: [goosetown-writer, notes]\n"
        "---\n"
        "Write it up.\n",
        encoding="utf-8",
    )

    agent = agent_spec_from_file(path)

    assert agent is not None
    assert agent.skills == ["goosetown-writer", "notes"]
    assert agent.enabled is True


def test_agent_profile_without_front_matter_is_skipped(tmp_path: Path) -> None:
    path = tmp_path / "plain.md"
    path.write_text("Just text.\n", encoding="utf-8")

    assert agent_spec_from_file(path) is None


def test_import_agents_from_dir_publishes_markdown_profiles(tmp_path: Path) -> None:
    (tmp_path / "reviewer.md").write_text(
        "---\nname: Reviewer\nrole: reviewer\n---\nReview things.\n",
        encoding="utf-8",
    )
    (tmp_path / "broken.md").write_text("no front matter", encoding="utf-8")
    (tmp_path / "notes.txt").write_text("ignored", encoding="utf-8")
    settings = replace(Settings.from_env(), goose_job_agents_dir=str(tmp_path))
    saved: list[AgentSpec] = []

    class Publisher:
        def save(self, agent: AgentSpec) -> None:
            saved.append(agent)

    imported = import_agents_from_dir(settings, Publisher())  # type: ignore[arg-type]

    assert imported == 1
    assert [agent.agent_id for agent in saved] == ["reviewer"]
