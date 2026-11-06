from __future__ import annotations

from dataclasses import replace

from kafkallm.models import SandboxSpec
from kafkallm.sandboxes import (
    SandboxStore,
    builtin_sandbox,
    default_sandboxes,
    new_sandbox_id,
)
from kafkallm.settings import Settings


def _settings() -> Settings:
    return replace(
        Settings.from_env(),
        goose_image="goose:test",
        goose_re_image="rea:test",
    )


def test_default_sandboxes_describe_both_images() -> None:
    sandboxes = default_sandboxes(_settings())

    assert [sandbox.sandbox_id for sandbox in sandboxes] == ["default", "reverse"]
    assert sandboxes[0].image == "goose:test"
    assert sandboxes[1].image == "rea:test"
    assert sandboxes[1].extensions == ["rea:/opt/kafkallm/rea-mcp"]
    assert sandboxes[1].env["GHIDRA_INSTALL_DIR"] == "/opt/ghidra"
    assert "REA" in sandboxes[1].instructions


def test_builtin_sandbox_allows_legacy_jobs_before_seeding() -> None:
    settings = _settings()

    assert builtin_sandbox("default", settings).image == "goose:test"  # type: ignore[union-attr]
    assert builtin_sandbox("reverse", settings).image == "rea:test"  # type: ignore[union-attr]
    assert builtin_sandbox("custom", settings) is None


def test_new_sandbox_id_is_a_stable_slug() -> None:
    assert new_sandbox_id("Malware Analysis!") == "malware-analysis"
    assert new_sandbox_id("!!!").startswith("sandbox-")


def test_sandbox_store_keeps_default_first_and_notifies_waiters() -> None:
    store = SandboxStore(_settings())
    store.put(
        SandboxSpec(
            sandbox_id="malware",
            name="Malware",
            image="malware:test",
        )
    )

    assert store.wait_for("malware", timeout=0).name == "Malware"  # type: ignore[union-attr]
    assert store.wait_for("missing", timeout=0) is None

    store.remove("malware")

    assert store.get("malware") is None


def test_sandbox_store_sorts_builtins_first() -> None:
    store = SandboxStore(_settings())
    for sandbox in (
        SandboxSpec(sandbox_id="zeta", name="Zeta", image="zeta:test"),
        SandboxSpec(sandbox_id="reverse", name="Reverse", image="rea:test"),
        SandboxSpec(sandbox_id="default", name="Default", image="goose:test"),
        SandboxSpec(sandbox_id="alpha", name="Alpha", image="alpha:test"),
    ):
        store.put(sandbox)

    assert [sandbox.sandbox_id for sandbox in store.sandboxes()] == [
        "default",
        "reverse",
        "alpha",
        "zeta",
    ]
