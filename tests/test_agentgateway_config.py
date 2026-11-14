from __future__ import annotations

import pytest
import yaml

from kafkallm.gateway.config import (
    ConfigError,
    base_url_from_env,
    cors_origins_from_env,
    render_config,
)


def test_openai_compatible_config_uses_an_environment_key_reference() -> None:
    rendered = render_config(
        base_url="https://openrouter.ai/api/v1",
        include_api_key=True,
        model="openai/gpt-4.1-mini",
    )
    config = yaml.safe_load(rendered)
    llm = config["llm"]

    assert config["gateways"]["default"]["port"] == 8091
    assert llm["policies"]["apiKey"]["mode"] == "strict"
    assert llm["policies"]["apiKey"]["keys"][0] == {
        "key": "$LLM_GATEWAY_VIRTUAL_KEY",
        "metadata": {"name": "kafkallm-worker"},
    }
    provider = llm["providers"][0]
    assert provider["name"] == "upstream"
    assert provider["provider"] == {"custom": {"formats": [{"type": "completions"}]}}
    assert provider["params"] == {
        "baseUrl": "https://openrouter.ai/api/v1",
        "apiKey": "$LLM_UPSTREAM_API_KEY",
    }
    assert llm["models"][0]["name"] == "openai/gpt-4.1-mini"
    assert llm["models"][-1]["name"] == "*"
    assert all(model["provider"]["reference"] == "upstream" for model in llm["models"])
    assert "real-provider-key" not in rendered


def test_base_url_is_required() -> None:
    with pytest.raises(ConfigError, match="LLM_UPSTREAM_BASE_URL"):
        render_config(base_url="")


def test_base_url_is_read_from_the_environment() -> None:
    assert base_url_from_env({"LLM_UPSTREAM_BASE_URL": "http://ollama:11434/v1"}) == (
        "http://ollama:11434/v1"
    )


def test_cors_origins_are_rendered_for_the_playground() -> None:
    rendered = render_config(
        base_url="https://example.com/v1",
        include_api_key=True,
        cors_origins=["http://127.0.0.1:15000", "http://localhost:15000"],
    )
    cors = yaml.safe_load(rendered)["llm"]["policies"]["cors"]

    assert cors["allowOrigins"] == [
        "http://127.0.0.1:15000",
        "http://localhost:15000",
    ]
    assert cors["allowMethods"] == ["GET", "POST", "OPTIONS"]
    assert cors["allowHeaders"] == ["Authorization", "Content-Type", "Accept"]
    assert cors["maxAge"] == "10m"


def test_empty_cors_origins_disable_the_policy() -> None:
    rendered = render_config(
        base_url="https://example.com/v1",
        include_api_key=True,
        cors_origins=[],
    )

    assert "cors" not in yaml.safe_load(rendered)["llm"]["policies"]


def test_cors_origins_from_env_defaults_and_overrides() -> None:
    defaults = cors_origins_from_env({})
    assert "http://127.0.0.1:15000" in defaults
    assert cors_origins_from_env({"AGENTGATEWAY_CORS_ORIGINS": ""}) == []
    assert cors_origins_from_env(
        {"AGENTGATEWAY_CORS_ORIGINS": "https://a.example, https://b.example"}
    ) == ["https://a.example", "https://b.example"]


def test_llm_port_is_rendered_and_validated() -> None:
    rendered = render_config(base_url="https://example.com/v1", llm_port=9000)
    config = yaml.safe_load(rendered)

    assert config["gateways"]["default"]["port"] == 9000
    assert "port" not in config["llm"]

    with pytest.raises(ConfigError, match="outside 1-65535"):
        render_config(base_url="https://example.com/v1", llm_port=70000)


def test_virtual_key_can_be_inlined_for_the_playground() -> None:
    rendered = render_config(
        base_url="https://example.com/v1",
        virtual_key="local-secret",
    )
    keys = yaml.safe_load(rendered)["llm"]["policies"]["apiKey"]["keys"]

    assert keys[0]["key"] == "local-secret"
    assert "$LLM_GATEWAY_VIRTUAL_KEY" not in rendered


def test_virtual_key_defaults_to_the_environment_reference() -> None:
    rendered = render_config(base_url="https://example.com/v1")
    keys = yaml.safe_load(rendered)["llm"]["policies"]["apiKey"]["keys"]

    assert keys[0]["key"] == "$LLM_GATEWAY_VIRTUAL_KEY"


def test_insecure_skip_tls_verify_adds_provider_tls_defaults() -> None:
    rendered = render_config(
        base_url="https://self-signed.example/v1",
        insecure_skip_tls_verify=True,
    )
    provider = yaml.safe_load(rendered)["llm"]["providers"][0]

    assert provider["defaults"] == {"tls": {"insecure": True}}
