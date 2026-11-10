"""Render an agentgateway configuration from provider-neutral settings.

The job sandbox always talks OpenAI-compatible HTTP to agentgateway. This
module only decides which upstream provider agentgateway uses, so switching
between OpenRouter, Ollama, OpenAI, and OpenAI-compatible custom endpoints is a
matter of environment values rather than editing YAML by hand.

The provider API key is never written to the generated file. The rendered
config references ``$LLM_UPSTREAM_API_KEY`` and agentgateway resolves it from
its own process environment.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import sys
from collections.abc import Mapping
from pathlib import Path

DEFAULT_PROVIDER = "openrouter"
DEFAULT_CUSTOM_FORMAT = "completions"
DEFAULT_OLLAMA_BASE_URL = "http://host.containers.internal:11434/v1"
DEFAULT_CORS_ORIGINS = (
    "http://127.0.0.1:15000",
    "http://localhost:15000",
)

BUILTIN_PROVIDERS = frozenset(
    {
        "openai",
        "gemini",
        "vertex",
        "anthropic",
        "bedrock",
        "azure",
        "copilot",
    }
)
PRESET_PROVIDERS = frozenset(
    {
        "cohere",
        "ollama",
        "baseten",
        "cerebras",
        "deepinfra",
        "deepseek",
        "groq",
        "huggingface",
        "mistral",
        "openrouter",
        "togetherai",
        "xai",
        "fireworks",
        "meta",
        "perplexity",
        "typesafe",
        "nebius",
    }
)
ALL_PROVIDERS = BUILTIN_PROVIDERS | PRESET_PROVIDERS | {"custom"}

# These either authenticate outside an API key or are configured by the
# operator with provider-specific credentials. Everything else needs a key.
KEY_OPTIONAL_PROVIDERS = frozenset(
    {"ollama", "custom", "azure", "bedrock", "vertex", "copilot"}
)

PROVIDER_FORMATS = frozenset(
    {
        "completions",
        "messages",
        "responses",
        "embeddings",
        "anthropicTokenCount",
        "generateContent",
        "geminiCountTokens",
        "realtime",
        "rerank",
        "decisions",
        "systemOne",
    }
)


class ConfigError(ValueError):
    """A provider setting cannot be rendered into a valid config."""


def provider_from_env(env: Mapping[str, str] | None = None) -> str:
    values = os.environ if env is None else env
    return (values.get("LLM_UPSTREAM_PROVIDER") or DEFAULT_PROVIDER).strip().lower()


def api_key_from_env(env: Mapping[str, str] | None = None) -> str:
    values = os.environ if env is None else env
    key = values.get("LLM_UPSTREAM_API_KEY", "").strip()
    if not key and provider_from_env(values) == "openrouter":
        # Backwards compatibility with the original OpenRouter-only setup.
        key = values.get("OPENROUTER_API_KEY", "").strip()
    return key


def base_url_from_env(env: Mapping[str, str] | None = None) -> str:
    values = os.environ if env is None else env
    base_url = values.get("LLM_UPSTREAM_BASE_URL", "").strip()
    if not base_url and provider_from_env(values) == "ollama":
        return DEFAULT_OLLAMA_BASE_URL
    return base_url


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
    provider: str,
    base_url: str = "",
    include_api_key: bool = True,
    provider_format: str = DEFAULT_CUSTOM_FORMAT,
    model: str = "",
    cors_origins: list[str] | None = None,
    llm_port: int = 8091,
) -> str:
    """Return the template with the generated ``llm:`` section filled in."""
    provider = (provider or DEFAULT_PROVIDER).strip().lower()
    if provider not in ALL_PROVIDERS:
        raise ConfigError(
            f"Unknown LLM_UPSTREAM_PROVIDER {provider!r}. "
            "Use openrouter, openai, ollama, a supported provider preset, or custom."
        )
    if not 1 <= llm_port <= 65535:
        raise ConfigError(f"LLM port {llm_port} is outside 1-65535.")
    if provider == "ollama" and not base_url:
        base_url = DEFAULT_OLLAMA_BASE_URL
    provider_format = (provider_format or DEFAULT_CUSTOM_FORMAT).strip()
    if provider == "custom":
        if not base_url:
            raise ConfigError("LLM_UPSTREAM_BASE_URL is required for provider=custom.")
        if provider_format not in PROVIDER_FORMATS:
            raise ConfigError(
                f"Unknown LLM_UPSTREAM_FORMAT {provider_format!r}. "
                f"Choose one of: {', '.join(sorted(PROVIDER_FORMATS))}."
            )

    lines = [
        "llm:",
        f"  port: {llm_port}",
        "  policies:",
        "    apiKey:",
        "      mode: strict",
        "      keys:",
        '        - key: "$LLM_GATEWAY_VIRTUAL_KEY"',
        "          metadata:",
        "            name: kafkallm-worker",
    ]
    if cors_origins:
        lines.extend(
            [
                "    cors:",
                "      allowOrigins:",
                *[f"        - {json.dumps(origin)}" for origin in cors_origins],
                "      allowMethods:",
                "        - GET",
                "        - POST",
                "        - OPTIONS",
                "      allowHeaders:",
                "        - Authorization",
                "        - Content-Type",
                "        - Accept",
                '      maxAge: "10m"',
            ]
        )
    lines.extend(
        [
            "  providers:",
            "    - name: upstream",
        ]
    )
    if provider == "custom":
        lines.extend(
            [
                "      provider:",
                "        custom:",
                "          formats:",
                f"            - type: {provider_format}",
            ]
        )
    else:
        lines.append(f"      provider: {provider}")

    params: list[str] = []
    if include_api_key:
        params.append('        apiKey: "$LLM_UPSTREAM_API_KEY"')
    if base_url:
        params.append(f"        baseUrl: {json.dumps(base_url)}")
    if params:
        lines.append("      params:")
        lines.extend(params)

    lines.append("  models:")
    if model.strip():
        lines.extend(
            [
                f"    - name: {json.dumps(model.strip())}",
                "      provider:",
                "        reference: upstream",
            ]
        )
    lines.extend(
        [
            '    - name: "*"',
            "      provider:",
            "        reference: upstream",
        ]
    )

    section = "\n".join(lines) + "\n"
    placeholder = "# __LLM_SECTION__"
    if placeholder not in template:
        raise ConfigError(f"Config template has no {placeholder} marker.")
    return template.replace(placeholder + "\n", section).replace(
        placeholder, section.rstrip("\n")
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="render-agentgateway-config",
        description="Render agentgateway/config.generated.yaml from .env.",
    )
    parser.add_argument(
        "--template",
        default="agentgateway/config.template.yaml",
        help="Config template path (default: agentgateway/config.template.yaml)",
    )
    parser.add_argument(
        "--output",
        default="agentgateway/config.generated.yaml",
        help="Output path (default: agentgateway/config.generated.yaml)",
    )
    parser.add_argument("--provider", help="Override LLM_UPSTREAM_PROVIDER")
    parser.add_argument("--base-url", help="Override LLM_UPSTREAM_BASE_URL")
    parser.add_argument("--format", help="Override LLM_UPSTREAM_FORMAT")
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
        "--no-api-key",
        action="store_true",
        help="Do not emit an apiKey reference (for tests or unauthenticated endpoints)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    env = os.environ
    provider = (args.provider or provider_from_env(env)).strip().lower()
    base_url = (
        args.base_url if args.base_url is not None else base_url_from_env(env)
    ).strip()
    provider_format = (
        args.format or env.get("LLM_UPSTREAM_FORMAT") or DEFAULT_CUSTOM_FORMAT
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

    if not args.no_api_key and not api_key and provider not in KEY_OPTIONAL_PROVIDERS:
        raise ConfigError(
            f"Provider {provider!r} needs LLM_UPSTREAM_API_KEY "
            "(the legacy OPENROUTER_API_KEY is accepted for OpenRouter)."
        )

    template_path = Path(args.template)
    try:
        template = template_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigError(f"Could not read config template: {exc}") from exc

    rendered = render_config(
        template,
        provider=provider,
        base_url=base_url,
        include_api_key=(
            not args.no_api_key and provider != "ollama" and bool(api_key)
        ),
        provider_format=provider_format,
        model=model,
        cors_origins=cors_origins,
        llm_port=llm_port,
    )
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(rendered, encoding="utf-8")
    # agentgateway runs as uid 65532 and the admin UI saves config changes
    # (for example the playground's Apply CORS action) back to this file. It
    # contains no secrets: the provider key is an environment reference.
    with contextlib.suppress(OSError):
        output_path.chmod(0o666)
    print(
        f"Wrote {output_path} (provider={provider}, "
        f"base_url={base_url or 'provider default'})"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ConfigError as exc:
        print(f"render-agentgateway-config: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
