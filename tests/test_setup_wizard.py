from __future__ import annotations

from collections.abc import Iterator
from types import SimpleNamespace

from gaensestall.setup import wizard as setup_wizard

BASE_ENV = """\
LLM_PROXY_TOKEN=test-virtual-key
LLM_UPSTREAM_BASE_URL=https://openrouter.ai/api/v1
LLM_UPSTREAM_API_KEY=
LLM_UPSTREAM_MODEL=nvidia/nemotron-3.5-lightning:free
MLFLOW_GATEWAY_ENDPOINT=gaensestall
GOOSE_MODEL=gaensestall
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


def test_non_interactive_setup_accepts_an_optional_key(tmp_path) -> None:
    path = _env(tmp_path)

    assert setup_wizard.run(path, interactive=False) == 0
    assert _values(path)["LLM_UPSTREAM_API_KEY"] == ""


def test_non_interactive_setup_requires_a_base_url(tmp_path) -> None:
    path = _env(tmp_path, BASE_ENV.replace("LLM_UPSTREAM_BASE_URL=.*", ""))
    path.write_text(
        BASE_ENV.replace("LLM_UPSTREAM_BASE_URL=https://openrouter.ai/api/v1\n", ""),
        encoding="utf-8",
    )

    assert setup_wizard.run(path, interactive=False) == 2


def test_non_interactive_setup_requires_an_upstream_model(tmp_path) -> None:
    path = _env(
        tmp_path,
        BASE_ENV.replace(
            "LLM_UPSTREAM_MODEL=nvidia/nemotron-3.5-lightning:free\n",
            "LLM_UPSTREAM_MODEL=\n",
        ),
    )

    assert setup_wizard.run(path, interactive=False) == 2


def test_wizard_writes_endpoint_key_and_model(tmp_path, monkeypatch) -> None:
    path = _env(tmp_path)
    answers = _answers(["https://llm.example/v1", "my-model"])
    monkeypatch.setattr("builtins.input", lambda _: next(answers))
    monkeypatch.setattr(setup_wizard.getpass, "getpass", lambda _: "test-key")

    assert setup_wizard.run(path, interactive=True) == 0
    values = _values(path)

    assert values["LLM_UPSTREAM_BASE_URL"] == "https://llm.example/v1"
    assert values["LLM_UPSTREAM_API_KEY"] == "test-key"
    assert values["LLM_UPSTREAM_MODEL"] == "my-model"
    assert values["MLFLOW_GATEWAY_ENDPOINT"] == "gaensestall"
    assert values["GOOSE_MODEL"] == "gaensestall"


def test_wizard_keeps_an_existing_key_when_blank(tmp_path, monkeypatch) -> None:
    path = _env(
        tmp_path,
        BASE_ENV.replace("LLM_UPSTREAM_API_KEY=", "LLM_UPSTREAM_API_KEY=old-key"),
    )
    answers = _answers(["https://llm.example/v1", "my-model"])
    monkeypatch.setattr("builtins.input", lambda _: next(answers))
    monkeypatch.setattr(setup_wizard.getpass, "getpass", lambda _: "")

    assert setup_wizard.run(path, interactive=True) == 0
    assert _values(path)["LLM_UPSTREAM_API_KEY"] == "old-key"


def test_env_editor_preserves_comments_and_replaces_a_value(tmp_path) -> None:
    path = _env(
        tmp_path,
        "# comment\nLLM_UPSTREAM_BASE_URL=https://old.example/v1\nUNRELATED=keep\n",
    )
    env = setup_wizard.EnvFile(path)
    env.set("LLM_UPSTREAM_BASE_URL", "https://new.example/v1")
    env.set("NEW_KEY", "new-value")
    env.write()

    text = path.read_text(encoding="utf-8")
    assert "# comment" in text
    assert "LLM_UPSTREAM_BASE_URL=https://new.example/v1" in text
    assert "UNRELATED=keep" in text
    assert "NEW_KEY=new-value" in text


def test_wizard_cancels_on_eof(tmp_path, monkeypatch) -> None:
    path = _env(tmp_path)

    def raise_eof(_):
        raise EOFError

    monkeypatch.setattr(setup_wizard.sys, "stdin", SimpleNamespace(isatty=lambda: True))
    monkeypatch.setattr("builtins.input", raise_eof)
    assert setup_wizard.main(["--env-file", str(path)]) == 130
