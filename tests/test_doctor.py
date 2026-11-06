from __future__ import annotations

from dataclasses import replace

from kafkallm.doctor import (
    Check,
    configuration_checks,
    format_report,
    valid_virtual_key,
)
from kafkallm.settings import Settings


def _settings() -> Settings:
    return replace(
        Settings.from_env(),
        goose_model="openai/gpt-4.1-mini",
        llm_gateway_virtual_key="a" * 64,
    )


def test_configuration_checks_accept_a_complete_environment() -> None:
    checks = configuration_checks(_settings(), {"OPENROUTER_API_KEY": "sk-or-v1-test"})

    assert [check.status for check in checks] == ["ok", "ok", "ok"]


def test_configuration_checks_report_missing_values() -> None:
    settings = replace(_settings(), goose_model="", llm_gateway_virtual_key="change-me")

    checks = configuration_checks(settings, {})
    by_name = {check.name: check for check in checks}

    assert by_name["Goose model"].status == "fail"
    assert by_name["OpenRouter API key"].status == "fail"
    assert by_name["Gateway virtual key"].status == "fail"


def test_valid_virtual_key_rejects_placeholders_and_short_values() -> None:
    assert valid_virtual_key("a" * 64)
    assert not valid_virtual_key("")
    assert not valid_virtual_key("change-me-to-a-long-random-string")
    assert not valid_virtual_key("a" * 31)


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
