from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SETTINGS = ROOT / "src" / "kafkallm" / "core" / "settings.py"
COMPOSE = ROOT / "compose.yaml"
ENV_EXAMPLE = ROOT / ".env.example"

# Rendering happens on the host before Compose starts; the worker receives the
# resolved value through GOOSE_PROVIDER instead.
HOST_ONLY = {"LLM_UPSTREAM_PROVIDER"}


def _settings_env_names() -> set[str]:
    source = SETTINGS.read_text(encoding="utf-8")
    return set(
        re.findall(
            r'_(?:str|int|bool)_env\(\s*\n?\s*"([A-Z][A-Z0-9_]*)"',
            source,
        )
    )


def _compose_env_names() -> set[str]:
    source = COMPOSE.read_text(encoding="utf-8")
    return set(re.findall(r"^\s{2,}([A-Z][A-Z0-9_]*):", source, re.M))


def _example_env_names() -> list[str]:
    source = ENV_EXAMPLE.read_text(encoding="utf-8")
    return re.findall(r"^([A-Z][A-Z0-9_]*)=", source, re.M)


def test_compose_forwards_every_settings_variable() -> None:
    missing = _settings_env_names() - _compose_env_names() - HOST_ONLY

    assert missing == set()


def test_env_example_has_no_duplicate_keys() -> None:
    keys = _example_env_names()

    assert len(keys) == len(set(keys))
    assert "MAX_AGENT_STEPS" not in keys


def test_settings_variables_are_documented() -> None:
    documented = _compose_env_names() | set(_example_env_names())

    assert _settings_env_names() - documented == set()
