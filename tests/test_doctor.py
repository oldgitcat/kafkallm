from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace

from kafkallm.core.settings import Settings
from kafkallm.setup import doctor
from kafkallm.setup.doctor import (
    Check,
    configuration_checks,
    format_report,
    gateway_config_check,
    valid_virtual_key,
)


def _settings() -> Settings:
    return replace(
        Settings.from_env(),
        goose_model="openai/gpt-4.1-mini",
        llm_gateway_virtual_key="a" * 64,
    )


def test_configuration_checks_accept_a_complete_environment() -> None:
    checks = configuration_checks(
        _settings(),
        {
            "LLM_UPSTREAM_BASE_URL": "https://openrouter.ai/api/v1",
            "LLM_UPSTREAM_API_KEY": "sk-or-v1-test",
        },
    )

    assert [check.name for check in checks] == [
        "Goose model",
        "LLM endpoint",
        "Provider API key",
        "Gateway virtual key",
    ]
    assert [check.status for check in checks] == ["ok"] * 4


def test_configuration_checks_allow_an_unauthenticated_endpoint() -> None:
    checks = configuration_checks(
        _settings(),
        {"LLM_UPSTREAM_BASE_URL": "http://host.containers.internal:11434/v1"},
    )
    by_name = {check.name: check for check in checks}

    assert by_name["LLM endpoint"].status == "ok"
    assert by_name["Provider API key"].status == "warn"


def test_configuration_checks_require_a_base_url() -> None:
    checks = configuration_checks(_settings(), {})
    by_name = {check.name: check for check in checks}

    assert by_name["LLM endpoint"].status == "fail"


def test_configuration_checks_report_missing_values() -> None:
    settings = replace(_settings(), goose_model="", llm_gateway_virtual_key="change-me")

    checks = configuration_checks(settings, {})
    by_name = {check.name: check for check in checks}

    assert by_name["Goose model"].status == "fail"
    assert by_name["LLM endpoint"].status == "fail"
    assert by_name["Provider API key"].status == "warn"
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


def test_gateway_config_check_reports_missing(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    check = gateway_config_check({})

    assert check.status == "warn"
    assert "not rendered" in check.detail


def test_gateway_config_check_validates_with_agentgateway(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.chdir(tmp_path)
    config = tmp_path / "infra" / "agentgateway" / "config.yml"
    config.parent.mkdir(parents=True)
    config.write_text("llm: {}\n", encoding="utf-8")
    monkeypatch.setattr(doctor.shutil, "which", lambda _: "/usr/bin/podman")
    calls: list[list[str]] = []

    def fake_run(args, timeout=10):
        del timeout
        calls.append(args)
        if args[:3] == ["podman", "image", "exists"]:
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        return SimpleNamespace(
            returncode=0, stdout="Configuration is valid!", stderr=""
        )

    monkeypatch.setattr(doctor, "_run", fake_run)

    check = gateway_config_check(
        {
            "LLM_GATEWAY_VIRTUAL_KEY": "k" * 64,
            "LLM_UPSTREAM_API_KEY": "sk-test",
        }
    )

    assert check.status == "ok"
    assert check.detail == "valid"
    validate_command = calls[-1]
    assert "LLM_UPSTREAM_API_KEY" in validate_command
    assert not any("sk-test" in str(argument) for argument in validate_command)


def test_gateway_config_check_reports_an_invalid_config(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    config = tmp_path / "infra" / "agentgateway" / "config.yml"
    config.parent.mkdir(parents=True)
    config.write_text("llm: {}\n", encoding="utf-8")
    monkeypatch.setattr(doctor.shutil, "which", lambda _: "/usr/bin/podman")

    def fake_run(args, timeout=10):
        del timeout
        if args[:3] == ["podman", "image", "exists"]:
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        return SimpleNamespace(returncode=1, stdout="", stderr="bad config")

    monkeypatch.setattr(doctor, "_run", fake_run)

    check = gateway_config_check({})

    assert check.status == "fail"
    assert "bad config" in check.detail
