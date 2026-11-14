"""Render the agentgateway configuration from one OpenAI-compatible endpoint.

Jobs always speak OpenAI-compatible HTTP to agentgateway. agentgateway forwards
to exactly one upstream: any endpoint that implements the OpenAI
``/v1/chat/completions`` API, including OpenRouter, OpenAI, Ollama, Open WebUI,
and self-hosted gateways.

The provider API key is never written to the generated file. The rendered
config references ``$LLM_UPSTREAM_API_KEY`` and agentgateway resolves it from
its own process environment.
"""

from __future__ import annotations

import argparse
import contextlib
import os
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml

DEFAULT_CORS_ORIGINS = (
    "http://127.0.0.1:15000",
    "http://localhost:15000",
)
PLACEHOLDER_VIRTUAL_KEYS = frozenset(
    {
        "",
        "change-me-to-a-long-random-string",
        "replace-with-openssl-rand-hex-32",
    }
)


class ConfigError(ValueError):
    """A provider setting cannot be rendered into a valid config."""


def api_key_from_env(env: Mapping[str, str] | None = None) -> str:
    values = os.environ if env is None else env
    return values.get("LLM_UPSTREAM_API_KEY", "").strip()


def base_url_from_env(env: Mapping[str, str] | None = None) -> str:
    values = os.environ if env is None else env
    return values.get("LLM_UPSTREAM_BASE_URL", "").strip()


def cors_origins_from_env(env: Mapping[str, str] | None = None) -> list[str]:
    """Return the browser origins allowed to call the LLM listener.

    An explicitly empty value disables the CORS policy. When the variable is
    absent, the local agentgateway playground origins are allowed so the UI
    works out of the box.
    """
    values = os.environ if env is None else env
    if "AGENTGATEWAY_CORS_ORIGINS" not in values:
        return list(DEFAULT_CORS_ORIGINS)
    return [
        origin.strip()
        for origin in values["AGENTGATEWAY_CORS_ORIGINS"].split(",")
        if origin.strip()
    ]


def render_config(
    template: str,
    *,
    base_url: str,
    include_api_key: bool = True,
    model: str = "",
    cors_origins: list[str] | None = None,
    llm_port: int = 8091,
    virtual_key: str = "$LLM_GATEWAY_VIRTUAL_KEY",
) -> str:
    """Return the template with the generated ``llm:`` section filled in."""
    base_url = (base_url or "").strip()
    if not base_url:
        raise ConfigError("LLM_UPSTREAM_BASE_URL is required.")
    if not 1 <= llm_port <= 65535:
        raise ConfigError(f"LLM port {llm_port} is outside 1-65535.")

    section_data: dict[str, Any] = {
        "gateways": {"default": {"port": llm_port}},
        "llm": {
            "policies": {
                "apiKey": {
                    "mode": "strict",
                    "keys": [
                        {
                            "key": virtual_key,
                            "metadata": {"name": "kafkallm-worker"},
                        }
                    ],
                },
            },
            "providers": [],
            "models": [],
        },
    }
    if cors_origins:
        section_data["llm"]["policies"]["cors"] = {
            "allowOrigins": cors_origins,
            "allowMethods": ["GET", "POST", "OPTIONS"],
            "allowHeaders": ["Authorization", "Content-Type", "Accept"],
            "maxAge": "10m",
        }

    params: dict[str, str] = {"baseUrl": base_url}
    if include_api_key:
        params["apiKey"] = "$LLM_UPSTREAM_API_KEY"
    section_data["llm"]["providers"] = [
        {
            "name": "upstream",
            "provider": {"custom": {"formats": [{"type": "completions"}]}},
            "params": params,
        }
    ]

    models: list[dict[str, object]] = []
    if model.strip():
        models.append(
            {
                "name": model.strip(),
                "provider": {"reference": "upstream"},
            }
        )
    models.append(
        {
            "name": "*",
            "provider": {"reference": "upstream"},
        }
    )
    section_data["llm"]["models"] = models

    section = yaml.safe_dump(
        section_data,
        sort_keys=False,
        default_flow_style=False,
    )
    placeholder = "# __LLM_SECTION__"
    if placeholder not in template:
        raise ConfigError(f"Config template has no {placeholder} marker.")
    return template.replace(placeholder + "\n", section).replace(
        placeholder, section.rstrip("\n")
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="render-agentgateway-config",
        description="Render infra/agentgateway/config.yml from .env.",
    )
    parser.add_argument(
        "--template",
        default="infra/agentgateway/config.template.yml",
        help="Config template path (default: infra/agentgateway/config.template.yml)",
    )
    parser.add_argument(
        "--output",
        default="infra/agentgateway/config.yml",
        help="Output path (default: infra/agentgateway/config.yml)",
    )
    parser.add_argument(
        "--base-url",
        help="Override LLM_UPSTREAM_BASE_URL (OpenAI-compatible /v1 endpoint)",
    )
    parser.add_argument("--model", help="Override GOOSE_MODEL for the model list")
    parser.add_argument(
        "--cors-origins",
        help=(
            "Comma-separated browser origins for the LLM CORS policy; "
            "an empty value disables it (default: AGENTGATEWAY_CORS_ORIGINS "
            "or the local playground origins)"
        ),
    )
    parser.add_argument(
        "--llm-port",
        type=int,
        help="LLM listener port (default: LLM_GATEWAY_PORT or 8091)",
    )
    parser.add_argument(
        "--env-virtual-key",
        action="store_true",
        help=(
            "Write $LLM_GATEWAY_VIRTUAL_KEY instead of the resolved key. "
            "The admin playground then needs the Raw API key value."
        ),
    )
    parser.add_argument(
        "--no-api-key",
        action="store_true",
        help="Do not emit an apiKey reference (for unauthenticated endpoints)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    env = os.environ
    base_url = (
        args.base_url if args.base_url is not None else base_url_from_env(env)
    ).strip()
    model = args.model if args.model is not None else env.get("GOOSE_MODEL", "")
    api_key = api_key_from_env(env)
    if args.cors_origins is not None:
        cors_origins = [
            origin.strip() for origin in args.cors_origins.split(",") if origin.strip()
        ]
    else:
        cors_origins = cors_origins_from_env(env)
    if args.llm_port is not None:
        llm_port = args.llm_port
    else:
        try:
            llm_port = int(env.get("LLM_GATEWAY_PORT") or 8091)
        except ValueError as exc:
            raise ConfigError("LLM_GATEWAY_PORT must be an integer.") from exc
    actual_virtual_key = env.get("LLM_GATEWAY_VIRTUAL_KEY", "").strip()
    inline_virtual_key = env.get(
        "AGENTGATEWAY_INLINE_VIRTUAL_KEY", "true"
    ).strip().lower() not in {"0", "false", "no", "off"}
    if args.env_virtual_key or not inline_virtual_key or not actual_virtual_key:
        virtual_key = "$LLM_GATEWAY_VIRTUAL_KEY"
    else:
        virtual_key = actual_virtual_key

    template_path = Path(args.template)
    try:
        template = template_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigError(f"Could not read config template: {exc}") from exc

    rendered = render_config(
        template,
        base_url=base_url,
        include_api_key=not args.no_api_key and bool(api_key),
        model=model,
        cors_origins=cors_origins,
        llm_port=llm_port,
        virtual_key=virtual_key,
    )
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(rendered, encoding="utf-8")
    # agentgateway runs as uid 65532 and the admin UI saves config changes
    # (for example the playground's Apply CORS action) back to this file. It
    # contains no secrets: the provider key is an environment reference.
    with contextlib.suppress(OSError):
        output_path.chmod(0o666)
    print(f"Wrote {output_path} (base_url={base_url})")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ConfigError as exc:
        print(f"render-agentgateway-config: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
