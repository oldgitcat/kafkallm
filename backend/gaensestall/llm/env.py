"""Environment helpers for the MLflow AI Gateway.

The gateway itself is provisioned inside the MLflow container from these
variables. The worker and the host tools only need to validate them.
"""

from __future__ import annotations

import os
from collections.abc import Mapping

DEFAULT_GATEWAY_ENDPOINT = "gaensestall"
PLACEHOLDER_LLM_PROXY_TOKENS = frozenset(
    {
        "",
        "change-me-to-a-long-random-string",
        "replace-with-openssl-rand-hex-32",
    }
)


def base_url_from_env(env: Mapping[str, str] | None = None) -> str:
    values = os.environ if env is None else env
    return values.get("LLM_UPSTREAM_BASE_URL", "").strip()


def api_key_from_env(env: Mapping[str, str] | None = None) -> str:
    values = os.environ if env is None else env
    return values.get("LLM_UPSTREAM_API_KEY", "").strip()


def upstream_model_from_env(env: Mapping[str, str] | None = None) -> str:
    values = os.environ if env is None else env
    return values.get("LLM_UPSTREAM_MODEL", "").strip()


def gateway_endpoint_from_env(env: Mapping[str, str] | None = None) -> str:
    values = os.environ if env is None else env
    return values.get("MLFLOW_GATEWAY_ENDPOINT", "").strip() or DEFAULT_GATEWAY_ENDPOINT


def insecure_skip_tls_verify_from_env(env: Mapping[str, str] | None = None) -> bool:
    values = os.environ if env is None else env
    return values.get("LLM_UPSTREAM_INSECURE_SKIP_TLS_VERIFY", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
