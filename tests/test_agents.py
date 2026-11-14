from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from kafkallm.core.models import AgentSpec
from kafkallm.core.settings import Settings
from tools.gui.agents import (
    AgentStore,
    agent_profile_text,
    agent_spec_from_file,
    default_agent_ids,
    default_agents,
    export_agent_profiles,
    import_agents_from_dir,
    new_agent_id,
    seed_default_agent_profiles,
)


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
    (tmp_path / "security-reviewer.md").write_text(
        "---\nname: Security reviewer\nrole: reviewer\n---\nReview things.\n",
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
    assert [agent.agent_id for agent in saved] == ["security-reviewer"]


def test_agent_profile_text_round_trips(tmp_path: Path) -> None:
    agent = AgentSpec(
        agent_id="custom",
        name='Quote " and colon: here',
        role="reviewer",
        description="First line\nSecond line",
        instructions="Do the thing.",
        skills=["alpha", "beta"],
        enabled=False,
    )
    path = tmp_path / "custom.md"
    path.write_text(agent_profile_text(agent), encoding="utf-8")

    parsed = agent_spec_from_file(path)

    assert parsed is not None
    assert parsed.agent_id == "custom"
    assert parsed.name == agent.name
    assert parsed.description == agent.description
    assert parsed.instructions == agent.instructions
    assert parsed.skills == ["alpha", "beta"]
    assert parsed.enabled is False


def test_import_applies_builtin_file_overrides(tmp_path: Path) -> None:
    (tmp_path / "worker.md").write_text(
        "---\nname: My worker\nrole: worker\n---\nMine.\n",
        encoding="utf-8",
    )
    settings = replace(Settings.from_env(), goose_job_agents_dir=str(tmp_path))
    saved: list[AgentSpec] = []

    class Publisher:
        def save(self, agent: AgentSpec) -> None:
            saved.append(agent)

    assert import_agents_from_dir(settings, Publisher()) == 1  # type: ignore[arg-type]
    assert [agent.agent_id for agent in saved] == ["worker"]
    assert saved[0].name == "My worker"
    assert "worker" in default_agent_ids()


def test_seed_default_agent_profiles_copies_only_missing_files(
    tmp_path: Path, monkeypatch
) -> None:
    source = tmp_path / "defaults"
    source.mkdir()
    (source / "worker.md").write_text(
        "---\nname: Worker\nrole: worker\n---\nDo work.\n",
        encoding="utf-8",
    )
    target = tmp_path / "agents"
    target.mkdir()
    monkeypatch.setattr("tools.gui.agents._default_agents_dir", lambda: source)
    settings = replace(Settings.from_env(), goose_job_agents_dir=str(target))

    assert seed_default_agent_profiles(settings) == 1
    destination = target / "worker.md"
    assert destination.is_file()
    mode = destination.stat().st_mode & 0o777
    assert mode & 0o600 == 0o600
    assert mode & 0o002 == 0

    destination.write_text("edited", encoding="utf-8")
    assert seed_default_agent_profiles(settings) == 0
    assert destination.read_text(encoding="utf-8") == "edited"


def test_export_writes_missing_custom_profiles(tmp_path: Path) -> None:
    settings = replace(Settings.from_env(), goose_job_agents_dir=str(tmp_path))
    custom = AgentSpec(agent_id="custom", name="Custom", role="custom")
    builtin = next(agent for agent in default_agents() if agent.agent_id == "worker")

    written = export_agent_profiles(settings, {"custom": custom, "worker": builtin})

    assert written == 1
    profile = tmp_path / "custom.md"
    assert profile.is_file()
    mode = profile.stat().st_mode & 0o777
    assert mode & 0o600 == 0o600
    assert mode & 0o002 == 0
    assert not (tmp_path / "worker.md").exists()

    (tmp_path / "custom.md").write_text("keep me", encoding="utf-8")
    assert export_agent_profiles(settings, {"custom": custom}) == 0
    assert (tmp_path / "custom.md").read_text(encoding="utf-8") == "keep me"
