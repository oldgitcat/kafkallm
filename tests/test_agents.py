from __future__ import annotations

from kafkallm.agents import AgentStore, default_agents, new_agent_id
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
