from __future__ import annotations

from collections.abc import Iterator
from types import SimpleNamespace

import pytest

from kafkallm import setup_wizard

BASE_ENV = """\
LLM_GATEWAY_VIRTUAL_KEY=test-virtual-key
LLM_UPSTREAM_PROVIDER=openrouter
LLM_UPSTREAM_BASE_URL=
LLM_UPSTREAM_API_KEY=
LLM_UPSTREAM_FORMAT=completions
GOOSE_MODEL=nvidia/nemotron-3.5-lightning:free
"""


def _answers(values: list[str]) -> Iterator[str]:
    return iter(values)


def _env(tmp_path, text: str = BASE_ENV):
    path = tmp_path / ".env"
    path.write_text(text, encoding="utf-8")
    return path


def _values(path) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            key, value = line.split("=", 1)
            result[key] = value
    return result


def test_non_interactive_setup_rejects_a_missing_key(tmp_path) -> None:
    path = _env(tmp_path)

    assert setup_wizard.run(path, interactive=False) == 2
    assert _values(path)["LLM_UPSTREAM_API_KEY"] == ""


def test_openrouter_wizard_writes_the_key_and_model(tmp_path, monkeypatch) -> None:
    path = _env(tmp_path)
    answers = _answers(["1", "", ""])
    monkeypatch.setattr("builtins.input", lambda _: next(answers))
    monkeypatch.setattr(setup_wizard.getpass, "getpass", lambda _: "sk-or-test")

    assert setup_wizard.run(path, interactive=True) == 0
    values = _values(path)

    assert values["LLM_UPSTREAM_PROVIDER"] == "openrouter"
    assert values["LLM_UPSTREAM_API_KEY"] == "sk-or-test"
    assert values["LLM_UPSTREAM_BASE_URL"] == ""
    assert values["GOOSE_MODEL"] == "nvidia/nemotron-3.5-lightning:free"


def test_ollama_wizard_needs_no_key_and_sets_the_defaults(
    tmp_path, monkeypatch
) -> None:
    path = _env(tmp_path)
    answers = _answers(["3", "", ""])
    monkeypatch.setattr("builtins.input", lambda _: next(answers))

    assert setup_wizard.run(path, interactive=True) == 0
    values = _values(path)

    assert values["LLM_UPSTREAM_PROVIDER"] == "ollama"
    assert values["LLM_UPSTREAM_API_KEY"] == ""
    assert values["LLM_UPSTREAM_BASE_URL"] == (
        "http://host.containers.internal:11434/v1"
    )
    assert values["GOOSE_MODEL"] == "llama3.2"


def test_custom_wizard_writes_the_base_url_format_and_model(
    tmp_path, monkeypatch
) -> None:
    path = _env(tmp_path)
    answers = _answers(["5", "https://llm.example/v1", "", "my-model"])
    monkeypatch.setattr("builtins.input", lambda _: next(answers))
    monkeypatch.setattr(setup_wizard.getpass, "getpass", lambda _: "")

    assert setup_wizard.run(path, interactive=True) == 0
    values = _values(path)

    assert values["LLM_UPSTREAM_PROVIDER"] == "custom"
    assert values["LLM_UPSTREAM_BASE_URL"] == "https://llm.example/v1"
    assert values["LLM_UPSTREAM_FORMAT"] == "completions"
    assert values["GOOSE_MODEL"] == "my-model"


def test_switching_provider_does_not_reuse_the_old_key(tmp_path, monkeypatch) -> None:
    path = _env(
        tmp_path,
        BASE_ENV.replace(
            "LLM_UPSTREAM_API_KEY=", "LLM_UPSTREAM_API_KEY=old-openrouter-key"
        ),
    )
    answers = _answers(["2", "", ""])
    monkeypatch.setattr("builtins.input", lambda _: next(answers))
    monkeypatch.setattr(setup_wizard.getpass, "getpass", lambda _: "sk-openai-test")

    assert setup_wizard.run(path, interactive=True) == 0
    values = _values(path)

    assert values["LLM_UPSTREAM_PROVIDER"] == "openai"
    assert values["LLM_UPSTREAM_API_KEY"] == "sk-openai-test"
    assert values["GOOSE_MODEL"] == "gpt-4.1-mini"


def test_env_editor_preserves_comments_and_replaces_a_value(tmp_path) -> None:
    path = _env(
        tmp_path,
        "# comment\nLLM_UPSTREAM_PROVIDER=openrouter\nUNRELATED=keep\n",
    )
    env = setup_wizard.EnvFile(path)
    env.set("LLM_UPSTREAM_PROVIDER", "ollama")
    env.set("NEW_KEY", "new-value")
    env.write()

    text = path.read_text(encoding="utf-8")
    assert "# comment" in text
    assert "LLM_UPSTREAM_PROVIDER=ollama" in text
    assert "UNRELATED=keep" in text
    assert "NEW_KEY=new-value" in text


def test_wizard_rejects_an_unknown_menu_choice(tmp_path, monkeypatch) -> None:
    path = _env(tmp_path)
    answers = _answers(["9", "1", "", ""])
    monkeypatch.setattr("builtins.input", lambda _: next(answers))
    monkeypatch.setattr(setup_wizard.getpass, "getpass", lambda _: "sk-or-test")

    assert setup_wizard.run(path, interactive=True) == 0
    assert _values(path)["LLM_UPSTREAM_PROVIDER"] == "openrouter"


def test_wizard_cancels_on_eof(tmp_path, monkeypatch) -> None:
    path = _env(tmp_path)

    def raise_eof(_):
        raise EOFError

    monkeypatch.setattr(setup_wizard.sys, "stdin", SimpleNamespace(isatty=lambda: True))
    monkeypatch.setattr("builtins.input", raise_eof)
    assert setup_wizard.main(["--env-file", str(path)]) == 130


@pytest.mark.parametrize(
    ("provider", "expected"),
    [("openrouter", "1"), ("openai", "2"), ("ollama", "3"), ("custom", "5")],
)
def test_menu_default_matches_the_current_provider(provider, expected) -> None:
    assert setup_wizard._menu_default(provider) == expected
