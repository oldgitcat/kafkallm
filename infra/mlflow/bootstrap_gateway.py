"""Provision the MLflow AI Gateway endpoint from environment variables.

This script runs inside the MLflow image before the tracking server starts. It
is idempotent: rerunning it updates the encrypted provider key and the upstream
model while keeping the endpoint and its experiment binding.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Callable
from typing import Any


def _is_not_found(exc: Exception) -> bool:
    from mlflow.exceptions import MlflowException
    from mlflow.protos.databricks_pb2 import RESOURCE_DOES_NOT_EXIST, ErrorCode

    return isinstance(exc, MlflowException) and exc.error_code == ErrorCode.Name(
        RESOURCE_DOES_NOT_EXIST
    )


def _get_or_none(
    getter: Callable[..., Any],
    *,
    is_not_found: Callable[[Exception], bool],
    **kwargs: Any,
) -> Any | None:
    try:
        return getter(**kwargs)
    except Exception as exc:
        if is_not_found(exc):
            return None
        raise


def provision(
    store: Any,
    *,
    endpoint_name: str,
    model_name: str,
    base_url: str,
    api_key: str,
    experiment_id: str | None,
    model_config: Callable[[str], Any],
    is_not_found: Callable[[Exception], bool] = _is_not_found,
) -> None:
    """Create or update the secret, model definition, and gateway endpoint."""
    secret_name = f"{endpoint_name}-upstream"
    model_definition_name = f"{endpoint_name}-model"
    auth_config = {"api_base": base_url.rstrip("/")}

    secret = _get_or_none(
        store.get_secret_info,
        is_not_found=is_not_found,
        secret_name=secret_name,
    )
    if secret is None:
        secret = store.create_gateway_secret(
            secret_name=secret_name,
            secret_value={"api_key": api_key},
            provider="openai",
            auth_config=auth_config,
        )
        print(f"Created gateway secret {secret_name}", flush=True)
    else:
        store.update_gateway_secret(
            secret_id=secret.secret_id,
            secret_value={"api_key": api_key},
            auth_config=auth_config,
        )
        print(f"Updated gateway secret {secret_name}", flush=True)

    model_definition = _get_or_none(
        store.get_gateway_model_definition,
        is_not_found=is_not_found,
        name=model_definition_name,
    )
    if model_definition is None:
        model_definition = store.create_gateway_model_definition(
            name=model_definition_name,
            secret_id=secret.secret_id,
            provider="openai",
            model_name=model_name,
        )
        print(f"Created gateway model {model_definition_name}", flush=True)
    else:
        store.update_gateway_model_definition(
            model_definition_id=model_definition.model_definition_id,
            secret_id=secret.secret_id,
            model_name=model_name,
        )
        print(f"Updated gateway model {model_definition_name}", flush=True)

    endpoint = _get_or_none(
        store.get_gateway_endpoint,
        is_not_found=is_not_found,
        name=endpoint_name,
    )
    model_configs = [model_config(model_definition.model_definition_id)]
    if endpoint is None:
        store.create_gateway_endpoint(
            name=endpoint_name,
            model_configs=model_configs,
            experiment_id=experiment_id,
            usage_tracking=True,
        )
        print(f"Created gateway endpoint {endpoint_name}", flush=True)
    else:
        store.update_gateway_endpoint(
            endpoint_id=endpoint.endpoint_id,
            model_configs=model_configs,
            experiment_id=experiment_id,
            usage_tracking=True,
        )
        print(f"Updated gateway endpoint {endpoint_name}", flush=True)


def main() -> int:
    endpoint_name = os.environ.get("MLFLOW_GATEWAY_ENDPOINT", "gaensestall").strip()
    base_url = os.environ.get("LLM_UPSTREAM_BASE_URL", "").strip()
    model_name = os.environ.get("LLM_UPSTREAM_MODEL", "").strip()
    api_key = os.environ.get("LLM_UPSTREAM_API_KEY", "").strip()
    experiment_id = os.environ.get("MLFLOW_EXPERIMENT_ID", "0").strip() or None

    missing = [
        name
        for name, value in (
            ("LLM_UPSTREAM_BASE_URL", base_url),
            ("LLM_UPSTREAM_MODEL", model_name),
        )
        if not value
    ]
    if missing:
        print(
            "Missing required environment variables: " + ", ".join(missing),
            file=sys.stderr,
        )
        return 2

    import mlflow
    from mlflow.entities.gateway_endpoint import (
        GatewayEndpointModelConfig,
        GatewayModelLinkageType,
    )
    from mlflow.tracking._tracking_service.utils import _get_store

    mlflow.set_tracking_uri(os.environ["MLFLOW_BACKEND_STORE_URI"])
    store = _get_store()

    def make_model_config(model_definition_id: str) -> Any:
        return GatewayEndpointModelConfig(
            model_definition_id=model_definition_id,
            linkage_type=GatewayModelLinkageType.PRIMARY,
        )

    provision(
        store,
        endpoint_name=endpoint_name,
        model_name=model_name,
        base_url=base_url,
        api_key=api_key,
        experiment_id=experiment_id,
        model_config=make_model_config,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
