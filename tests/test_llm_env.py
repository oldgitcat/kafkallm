from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

from gaensestall.llm.env import (
    DEFAULT_GATEWAY_ENDPOINT,
    api_key_from_env,
    base_url_from_env,
    gateway_endpoint_from_env,
    insecure_skip_tls_verify_from_env,
    upstream_model_from_env,
)

ROOT = Path(__file__).resolve().parents[1]
BOOTSTRAP = ROOT / "infra" / "mlflow" / "bootstrap_gateway.py"


def test_gateway_environment_helpers() -> None:
    env = {
        "LLM_UPSTREAM_BASE_URL": " https://llm.example/v1 ",
        "LLM_UPSTREAM_API_KEY": " secret ",
        "LLM_UPSTREAM_MODEL": " model/a ",
        "MLFLOW_GATEWAY_ENDPOINT": "gaensestall",
        "LLM_UPSTREAM_INSECURE_SKIP_TLS_VERIFY": "YES",
    }

    assert base_url_from_env(env) == "https://llm.example/v1"
    assert api_key_from_env(env) == "secret"
    assert upstream_model_from_env(env) == "model/a"
    assert gateway_endpoint_from_env(env) == "gaensestall"
    assert insecure_skip_tls_verify_from_env(env) is True
    assert gateway_endpoint_from_env({}) == DEFAULT_GATEWAY_ENDPOINT


def _load_bootstrap():
    spec = importlib.util.spec_from_file_location("bootstrap_gateway", BOOTSTRAP)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Missing(LookupError):
    pass


class _Secret:
    secret_id = "s-1"


class _Model:
    model_definition_id = "d-1"


class _Endpoint:
    endpoint_id = "e-1"


class _Store:
    def __init__(self) -> None:
        self.secret = None
        self.model = None
        self.endpoint = None
        self.calls: list[tuple[str, dict]] = []

    def get_secret_info(self, **kwargs):
        if self.secret is None:
            raise _Missing
        return self.secret

    def create_gateway_secret(self, **kwargs):
        self.calls.append(("create_secret", kwargs))
        self.secret = _Secret()
        return self.secret

    def update_gateway_secret(self, **kwargs):
        self.calls.append(("update_secret", kwargs))

    def get_gateway_model_definition(self, **kwargs):
        if self.model is None:
            raise _Missing
        return self.model

    def create_gateway_model_definition(self, **kwargs):
        self.calls.append(("create_model", kwargs))
        self.model = _Model()
        return self.model

    def update_gateway_model_definition(self, **kwargs):
        self.calls.append(("update_model", kwargs))

    def get_gateway_endpoint(self, **kwargs):
        if self.endpoint is None:
            raise _Missing
        return self.endpoint

    def create_gateway_endpoint(self, **kwargs):
        self.calls.append(("create_endpoint", kwargs))
        self.endpoint = _Endpoint()
        return self.endpoint

    def update_gateway_endpoint(self, **kwargs):
        self.calls.append(("update_endpoint", kwargs))


def test_bootstrap_creates_then_updates_the_gateway() -> None:
    module = _load_bootstrap()
    store = _Store()

    kwargs = {
        "endpoint_name": "gaensestall",
        "model_name": "model/a",
        "base_url": "https://llm.example/v1/",
        "api_key": "secret",
        "experiment_id": "0",
        "model_config": lambda model_id: ("model", model_id),
        "is_not_found": lambda exc: isinstance(exc, _Missing),
    }
    module.provision(store, **kwargs)

    assert [name for name, _ in store.calls] == [
        "create_secret",
        "create_model",
        "create_endpoint",
    ]
    assert store.calls[0][1]["auth_config"] == {"api_base": "https://llm.example/v1"}
    assert store.calls[2][1]["model_configs"] == [("model", "d-1")]

    store.calls.clear()
    module.provision(store, **kwargs)

    assert [name for name, _ in store.calls] == [
        "update_secret",
        "update_model",
        "update_endpoint",
    ]
    assert store.calls[0][1]["auth_config"] == {"api_base": "https://llm.example/v1"}


def test_sitecustomize_can_disable_tls_verification(tmp_path: Path) -> None:
    script = (
        "import ssl; c = ssl.create_default_context(); "
        "print(c.check_hostname, int(c.verify_mode))"
    )
    env = {
        "PYTHONPATH": str(ROOT / "infra" / "mlflow"),
        "LLM_UPSTREAM_INSECURE_SKIP_TLS_VERIFY": "true",
        "PATH": "/usr/bin:/bin",
    }
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        env=env,
        check=True,
    )

    assert result.stdout.strip() == f"False {int(__import__('ssl').CERT_NONE)}"
