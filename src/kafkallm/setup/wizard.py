"""Interactive endpoint setup used by ``make setup``.

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

from kafkallm.gateway.config import DEFAULT_CORS_ORIGINS

DEFAULT_BASE_URL = "https://openrouter.ai/api/v1"


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


def _ask_secret(prompt: str, *, keep: bool) -> str:
    suffix = "leave empty to keep current" if keep else "optional"
    return getpass.getpass(f"{prompt} [{suffix}]: ").strip()


def _summary(base_url: str, model: str, key: str) -> None:
    print("\nLLM configuration:")
    print("  endpoint: OpenAI-compatible")
    print(f"  base URL: {base_url}")
    print(f"  model:    {model}")
    print(f"  API key:  {'configured' if key else 'not set'}")


def run(env_path: Path, *, interactive: bool | None = None) -> int:
    env = EnvFile(env_path)
    base_url = env.get("LLM_UPSTREAM_BASE_URL")
    key = env.get("LLM_UPSTREAM_API_KEY")
    model = env.get("GOOSE_MODEL")

    if interactive is None:
        interactive = sys.stdin.isatty()

    if not interactive:
        if not base_url:
            print(
                "Non-interactive setup needs LLM_UPSTREAM_BASE_URL. "
                "Edit .env or run 'make setup' in a terminal.",
                file=sys.stderr,
            )
            return 2
        if not model:
            print(
                "Non-interactive setup needs GOOSE_MODEL. "
                "Edit .env or run 'make setup' in a terminal.",
                file=sys.stderr,
            )
            return 2
        env.ensure("AGENTGATEWAY_CORS_ORIGINS", ",".join(DEFAULT_CORS_ORIGINS))
        env.ensure("AGENTGATEWAY_INLINE_VIRTUAL_KEY", "true")
        env.write()
        _summary(base_url, model, key)
        return 0

    base_url = _ask("OpenAI-compatible base URL", base_url or DEFAULT_BASE_URL)
    while not base_url:
        print("A base URL is required.")
        base_url = _ask("OpenAI-compatible base URL", DEFAULT_BASE_URL)

    secret = _ask_secret("API key", keep=bool(key))
    if secret:
        key = secret

    while True:
        model = _ask("Model", model)
        if model:
            break
        print("A model name is required.")

    env.set("LLM_UPSTREAM_BASE_URL", base_url)
    env.set("LLM_UPSTREAM_API_KEY", key)
    env.set("GOOSE_MODEL", model)
    env.ensure("AGENTGATEWAY_CORS_ORIGINS", ",".join(DEFAULT_CORS_ORIGINS))
    env.ensure("AGENTGATEWAY_INLINE_VIRTUAL_KEY", "true")
    env.write()
    _summary(base_url, model, key)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="kafkallm-setup-wizard",
        description="Configure one OpenAI-compatible LLM endpoint in .env.",
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
