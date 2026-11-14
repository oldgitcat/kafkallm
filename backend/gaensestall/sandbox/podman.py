"""Small Podman/Docker API helpers used by the goose runtime."""

from __future__ import annotations

import json
import time
from typing import Any

from docker.utils import parse_bytes
from gaensestall.sandbox.errors import GooseJobRuntimeError


def create_container(
    client: Any,
    body: dict[str, Any],
    *,
    name: str | None = None,
) -> Any:
    api = client.api
    params = {"name": name} if name else None
    response = api._post(
        api._url("/containers/create"),
        data=json.dumps(body),
        headers={"Content-Type": "application/json"},
        params=params,
    )
    if response.status_code >= 300:
        raise GooseJobRuntimeError(
            "Could not create the container: "
            f"{response.status_code} {response.text[:1000]}"
        )
    return client.containers.get(response.json()["Id"])


def is_not_found(exc: Exception) -> bool:
    return getattr(exc, "status_code", None) == 404 or exc.__class__.__name__ in {
        "NotFound",
        "ImageNotFound",
    }


def remove_container(container: Any | None) -> bool:
    """Remove a container, returning false only for a real failure."""
    if container is None:
        return True
    try:
        container.remove(force=True)
    except Exception as exc:
        if not is_not_found(exc):
            return False
    return True


def container_networks(container: Any) -> dict[str, Any]:
    attrs = getattr(container, "attrs", None) or {}
    networks = (attrs.get("NetworkSettings") or {}).get("Networks") or {}
    return networks if isinstance(networks, dict) else {}


def container_ipv6(container: Any, network: str) -> str:
    endpoint = container_networks(container).get(network) or {}
    value = endpoint.get("GlobalIPv6Address")
    return value if isinstance(value, str) else ""


def wait_for_ipv6(container: Any, network: str, timeout: float = 15.0) -> str:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        container.reload()
        address = container_ipv6(container, network)
        if address:
            return address
        time.sleep(0.2)
    raise GooseJobRuntimeError(
        f"The job LLM proxy did not get an IPv6 address on {network}."
    )


def parse_size(value: str) -> int:
    text = value.strip()
    if not text or text == "0":
        return 0
    try:
        return parse_bytes(text)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid size value: {value}") from exc
