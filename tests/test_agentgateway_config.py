from __future__ import annotations

import pytest

from kafkallm.agentgateway_config import (
    ConfigError,
    api_key_from_env,
    base_url_from_env,
    cors_origins_from_env,
    render_config,
)

TEMPLATE = """config:
  adminAddr: "0.0.0.0:15000"

# __LLM_SECTION__
"""


def test_openrouter_config_uses_an_environment_key_reference() -> None:
    rendered = render_config(
        TEMPLATE,
        provider="openrouter",
        include_api_key=True,
        model="openai/gpt-4.1-mini",
    )

    assert "provider: openrouter" in rendered
    assert 'apiKey: "$LLM_UPSTREAM_API_KEY"' in rendered
    assert "real-provider-key" not in rendered
    assert 'name: "openai/gpt-4.1-mini"' in rendered
    assert 'name: "*"' in rendered
    assert "reference: upstream" in rendered


def test_ollama_defaults_to_the_host_base_url_and_needs_no_key() -> None:
    rendered = render_config(
        TEMPLATE,
        provider="ollama",
        include_api_key=False,
        model="llama3.2",
    )

    assert "provider: ollama" in rendered
    assert 'baseUrl: "http://host.containers.internal:11434/v1"' in rendered
    assert 'apiKey: "$LLM_UPSTREAM_API_KEY"' not in rendered
    assert 'name: "llama3.2"' in rendered


def test_custom_provider_requires_a_base_url_and_renders_its_format() -> None:
    with pytest.raises(ConfigError, match="LLM_UPSTREAM_BASE_URL"):
        render_config(TEMPLATE, provider="custom")

    rendered = render_config(
        TEMPLATE,
        provider="custom",
        base_url="https://llm.example.com/v1",
        include_api_key=True,
        provider_format="messages",
    )

    assert "provider:" in rendered
    assert "custom:" in rendered
    assert "- type: messages" in rendered
    assert 'baseUrl: "https://llm.example.com/v1"' in rendered


def test_unknown_provider_and_format_are_rejected() -> None:
    with pytest.raises(ConfigError, match="Unknown LLM_UPSTREAM_PROVIDER"):
        render_config(TEMPLATE, provider="not-a-provider")

    with pytest.raises(ConfigError, match="Unknown LLM_UPSTREAM_FORMAT"):
        render_config(
            TEMPLATE,
            provider="custom",
            base_url="https://llm.example.com/v1",
            provider_format="not-a-format",
        )


def test_template_requires_the_llm_marker() -> None:
    with pytest.raises(ConfigError, match="__LLM_SECTION__"):
        render_config("config: {}\n", provider="openrouter")


def test_provider_settings_fall_back_to_the_legacy_openrouter_key() -> None:
    legacy = {"OPENROUTER_API_KEY": "legacy-key"}
    assert api_key_from_env(legacy) == "legacy-key"
    assert api_key_from_env({"LLM_UPSTREAM_API_KEY": "new-key", **legacy}) == "new-key"
    assert api_key_from_env({"LLM_UPSTREAM_PROVIDER": "openai", **legacy}) == ""


def test_ollama_base_url_defaults_for_environment_reads() -> None:
    assert base_url_from_env({"LLM_UPSTREAM_PROVIDER": "ollama"}) == (
        "http://host.containers.internal:11434/v1"
    )
    assert (
        base_url_from_env(
            {
                "LLM_UPSTREAM_PROVIDER": "ollama",
                "LLM_UPSTREAM_BASE_URL": "http://ollama:11434/v1",
            }
        )
        == "http://ollama:11434/v1"
    )


def test_cors_origins_are_rendered_for_the_playground() -> None:
    rendered = render_config(
        TEMPLATE,
        provider="openrouter",
        include_api_key=True,
        cors_origins=["http://127.0.0.1:15000", "http://localhost:15000"],
    )

    assert "    cors:" in rendered
    assert '        - "http://127.0.0.1:15000"' in rendered
    assert '        - "http://localhost:15000"' in rendered
    assert "        - OPTIONS" in rendered
    assert "        - Authorization" in rendered


def test_empty_cors_origins_disable_the_policy() -> None:
    rendered = render_config(
        TEMPLATE,
        provider="openrouter",
        include_api_key=True,
        cors_origins=[],
    )

    assert "cors:" not in rendered


def test_cors_origins_from_env_defaults_and_overrides() -> None:
    defaults = cors_origins_from_env({})
    assert "http://127.0.0.1:15000" in defaults
    assert cors_origins_from_env({"AGENTGATEWAY_CORS_ORIGINS": ""}) == []
    assert cors_origins_from_env(
        {"AGENTGATEWAY_CORS_ORIGINS": "https://a.example, https://b.example"}
    ) == ["https://a.example", "https://b.example"]
