from __future__ import annotations

from dataclasses import replace

from gaensestall.core.settings import Settings
from gaensestall.setup.doctor import (
    Check,
    configuration_checks,
    format_report,
    valid_llm_proxy_token,
)


def _settings() -> Settings:
    return replace(
        Settings.from_env(),
        goose_model="gaensestall",
        mlflow_gateway_endpoint="gaensestall",
        llm_proxy_token="a" * 64,
    )


def _complete_env() -> dict[str, str]:
    return {
        "LLM_UPSTREAM_BASE_URL": "https://openrouter.ai/api/v1",
        "LLM_UPSTREAM_MODEL": "openai/gpt-4.1-mini",
        "LLM_UPSTREAM_API_KEY": "sk-or-v1-test",
        "MLFLOW_GATEWAY_ENDPOINT": "gaensestall",
    }


def test_configuration_checks_accept_a_complete_environment() -> None:
    checks = configuration_checks(_settings(), _complete_env())

    assert [check.name for check in checks] == [
        "Gateway endpoint",
        "Goose model",
        "LLM endpoint",
        "Upstream model",
        "Provider API key",
        "LLM proxy token",
    ]
    assert [check.status for check in checks] == ["ok"] * 6


def test_configuration_checks_allow_an_unauthenticated_endpoint() -> None:
    checks = configuration_checks(
        _settings(),
        {
            "LLM_UPSTREAM_BASE_URL": "http://host.containers.internal:11434/v1",
            "LLM_UPSTREAM_MODEL": "llama3",
        },
    )
    by_name = {check.name: check for check in checks}

    assert by_name["LLM endpoint"].status == "ok"
    assert by_name["Upstream model"].status == "ok"
    assert by_name["Provider API key"].status == "warn"


def test_configuration_checks_require_a_base_url() -> None:
    checks = configuration_checks(_settings(), {})
    by_name = {check.name: check for check in checks}

    assert by_name["LLM endpoint"].status == "fail"
    assert by_name["Upstream model"].status == "fail"


def test_configuration_checks_require_goose_model_to_match_the_endpoint() -> None:
    checks = configuration_checks(
        _settings(),
        {**_complete_env(), "MLFLOW_GATEWAY_ENDPOINT": "other-endpoint"},
    )
    by_name = {check.name: check for check in checks}

    assert by_name["Goose model"].status == "fail"
    assert "other-endpoint" in by_name["Goose model"].hint


def test_configuration_checks_report_missing_values() -> None:
    settings = replace(_settings(), goose_model="", llm_proxy_token="change-me")

    checks = configuration_checks(settings, {})
    by_name = {check.name: check for check in checks}

    assert by_name["Goose model"].status == "fail"
    assert by_name["LLM endpoint"].status == "fail"
    assert by_name["Upstream model"].status == "fail"
    assert by_name["Provider API key"].status == "warn"
    assert by_name["LLM proxy token"].status == "fail"


def test_valid_llm_proxy_token_rejects_placeholders_and_short_values() -> None:
    assert valid_llm_proxy_token("a" * 64)
    assert not valid_llm_proxy_token("")
    assert not valid_llm_proxy_token("change-me-to-a-long-random-string")
    assert not valid_llm_proxy_token("a" * 31)


def test_report_includes_hints_and_summary() -> None:
    report = format_report(
        [
            Check("Python", "ok", "3.12"),
            Check("Podman socket", "fail", "missing", "Enable podman.socket."),
        ]
    )

    assert "PASS  Python" in report
    assert "FAIL  Podman socket" in report
    assert "-> Enable podman.socket." in report
    assert "Summary: 1 passed, 0 warnings, 1 failed" in report
