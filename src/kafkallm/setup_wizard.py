"""Interactive provider setup used by ``make setup``.

The wizard edits ``.env`` in place. It never prints the API key. In a
non-interactive shell it only validates that the existing values are complete,
so scripts and CI keep working.
"""

from __future__ import annotations

import argparse
import contextlib
import getpass
import sys
from pathlib import Path

from kafkallm.agentgateway_config import (
    ALL_PROVIDERS,
    DEFAULT_CORS_ORIGINS,
    DEFAULT_PROVIDER,
    KEY_OPTIONAL_PROVIDERS,
    PROVIDER_FORMATS,
)

DEFAULT_FORMAT = "completions"
DEFAULT_OLLAMA_BASE_URL = "http://host.containers.internal:11434/v1"
DEFAULT_MODELS = {
    "openrouter": "nvidia/nemotron-3.5-lightning:free",
    "openai": "gpt-4.1-mini",
    "ollama": "llama3.2",
}

# The sentinel values select a custom OpenAI-compatible endpoint or a free-form
# provider preset without pretending they are real provider names.
OPENAI_CUSTOM = "__openai_custom"
OTHER = "__other"

MENU = (
    ("1", "openrouter", "OpenRouter"),
    ("2", "openai", "OpenAI"),
    ("3", "ollama", "Ollama (local, no API key)"),
    ("4", OPENAI_CUSTOM, "Custom OpenAI-compatible endpoint (no key required)"),
    ("5", "custom", "Custom provider (choose API format)"),
    ("6", OTHER, "Other provider preset (anthropic, gemini, ...)"),
)


class EnvFile:
    """Small line-preserving .env editor."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.lines = path.read_text(encoding="utf-8").splitlines()

    def get(self, key: str, default: str = "") -> str:
        prefix = key + "="
        for line in self.lines:
            if line.startswith(prefix):
                return line.split("=", 1)[1].strip()
        return default

    def has(self, key: str) -> bool:
        prefix = key + "="
        return any(line.startswith(prefix) for line in self.lines)

    def set(self, key: str, value: str) -> None:
        clean = value.replace("\r", " ").replace("\n", " ").strip()
        prefix = key + "="
        for index, line in enumerate(self.lines):
            if line.startswith(prefix):
                self.lines[index] = f"{key}={clean}"
                return
        self.lines.append(f"{key}={clean}")

    def ensure(self, key: str, value: str) -> None:
        if not self.has(key):
            self.set(key, value)

    def write(self) -> None:
        self.path.write_text("\n".join(self.lines) + "\n", encoding="utf-8")
        with contextlib.suppress(OSError):
            self.path.chmod(0o600)


def _ask(prompt: str, default: str = "") -> str:
    label = f"{prompt} [{default}]" if default else prompt
    answer = input(f"{label}: ").strip()
    return answer or default


def _ask_secret(prompt: str, *, keep: bool, required: bool) -> str:
    if keep:
        suffix = "leave empty to keep current"
    elif required:
        suffix = "required"
    else:
        suffix = "optional, leave empty for none"
    return getpass.getpass(f"{prompt} [{suffix}]: ").strip()


def _menu_default(current: str) -> str:
    for number, provider, _ in MENU:
        if provider == current:
            return number
    if current not in {"", DEFAULT_PROVIDER}:
        return "6"
    return "1"


def _choose_provider(current: str) -> str:
    print("\nWhich LLM provider do you want to use?")
    for number, _, label in MENU:
        print(f"  {number}) {label}")
    default = _menu_default(current)
    while True:
        answer = input(f"Choose [1-6] (current: {current}): ").strip() or default
        for number, provider, _ in MENU:
            if answer == number:
                return provider
        print("Please enter a number from 1 to 6.")


def _choose_other_provider(current: str) -> str:
    default = current if current in ALL_PROVIDERS else "anthropic"
    while True:
        value = _ask("Provider preset", default).strip().lower()
        if value in ALL_PROVIDERS:
            return value
        print(
            "Unknown provider preset. Choose one of: "
            + ", ".join(sorted(ALL_PROVIDERS))
        )


def _default_model(provider: str, current: str) -> str:
    if provider == "openrouter":
        return current or DEFAULT_MODELS["openrouter"]
    if provider == "openai":
        if current and "/" not in current:
            return current
        return DEFAULT_MODELS["openai"]
    if provider == "ollama":
        if current and "/" not in current:
            return current
        return DEFAULT_MODELS["ollama"]
    return current


def _summary(provider: str, base_url: str, model: str, key: str) -> None:
    print("\nProvider configuration:")
    print(f"  provider:  {provider}")
    print(f"  base URL:  {base_url or 'provider default'}")
    print(f"  model:     {model}")
    print(f"  API key:   {'configured' if key else 'not set'}")


def run(env_path: Path, *, interactive: bool | None = None) -> int:
    env = EnvFile(env_path)
    current_provider = (
        (env.get("LLM_UPSTREAM_PROVIDER") or DEFAULT_PROVIDER).strip().lower()
    )
    current_key = env.get("LLM_UPSTREAM_API_KEY") or env.get("OPENROUTER_API_KEY", "")
    current_base = env.get("LLM_UPSTREAM_BASE_URL", "")
    current_model = env.get("GOOSE_MODEL", "")
    current_format = env.get("LLM_UPSTREAM_FORMAT", DEFAULT_FORMAT)

    if interactive is None:
        interactive = sys.stdin.isatty()

    if not interactive:
        provider = current_provider
        if provider == "custom" and not current_base:
            print(
                "Non-interactive setup needs LLM_UPSTREAM_BASE_URL for "
                "provider=custom. Edit .env or run 'make setup' in a terminal.",
                file=sys.stderr,
            )
            return 2
        if provider not in KEY_OPTIONAL_PROVIDERS and not current_key:
            print(
                f"Non-interactive setup needs LLM_UPSTREAM_API_KEY for "
                f"provider={provider}. Edit .env or run 'make setup' in a "
                "terminal.",
                file=sys.stderr,
            )
            return 2
        if not current_model:
            print(
                "Non-interactive setup needs GOOSE_MODEL. Edit .env or run "
                "'make setup' in a terminal.",
                file=sys.stderr,
            )
            return 2
        env.ensure("LLM_UPSTREAM_FORMAT", DEFAULT_FORMAT)
        env.ensure("AGENTGATEWAY_CORS_ORIGINS", ",".join(DEFAULT_CORS_ORIGINS))
        env.ensure("AGENTGATEWAY_INLINE_VIRTUAL_KEY", "true")
        env.write()
        _summary(provider, current_base, current_model, current_key)
        return 0

    choice = _choose_provider(current_provider)
    if choice == OPENAI_CUSTOM:
        provider = "custom"
        provider_format = DEFAULT_FORMAT
    elif choice == OTHER:
        provider = _choose_other_provider(current_provider)
        provider_format = DEFAULT_FORMAT
    else:
        provider = choice
        provider_format = DEFAULT_FORMAT

    key = current_key
    if provider == "ollama":
        key = ""
    else:
        required = provider not in KEY_OPTIONAL_PROVIDERS
        keep = bool(current_key) and provider == current_provider
        while True:
            secret = _ask_secret("API key", keep=keep, required=required)
            if secret:
                key = secret
                break
            if keep or not required:
                break
            print("An API key is required for this provider.")

    base_default = current_base
    if provider == "ollama" and not base_default:
        base_default = DEFAULT_OLLAMA_BASE_URL
    while True:
        base_url = _ask("Base URL (blank = provider default)", base_default)
        if provider != "custom" or base_url:
            break
        print("A base URL is required for the custom provider.")

    if provider == "custom":
        default_format = current_format or provider_format
        while True:
            provider_format = _ask(
                "API format (" + ", ".join(sorted(PROVIDER_FORMATS)) + ")",
                default_format,
            )
            if provider_format in PROVIDER_FORMATS:
                break
            print("Unknown API format.")
    else:
        provider_format = DEFAULT_FORMAT

    default_model = _default_model(provider, current_model)
    while True:
        model = _ask("Model", default_model)
        if model:
            break
        print("A model name is required.")

    env.set("LLM_UPSTREAM_PROVIDER", provider)
    env.set("LLM_UPSTREAM_BASE_URL", base_url)
    env.set("LLM_UPSTREAM_API_KEY", key)
    env.set("LLM_UPSTREAM_FORMAT", provider_format)
    env.set("GOOSE_MODEL", model)
    env.ensure("AGENTGATEWAY_CORS_ORIGINS", ",".join(DEFAULT_CORS_ORIGINS))
    env.ensure("AGENTGATEWAY_INLINE_VIRTUAL_KEY", "true")
    env.write()
    _summary(provider, base_url, model, key)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="kafkallm-setup-wizard",
        description="Choose the LLM provider and write it to .env.",
    )
    parser.add_argument("--env-file", default=".env")
    parser.add_argument(
        "--non-interactive",
        action="store_true",
        help="Validate the existing values without prompting.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return run(
            Path(args.env_file),
            interactive=False if args.non_interactive else None,
        )
    except (EOFError, KeyboardInterrupt):
        print("\nSetup cancelled.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
