"""Create the isolated, IPv6-only Podman network used by goose jobs.

Rootless Podman normally creates an IPv4 subnet for every new network. Some
hosts expose an interface with an unusually broad netmask (for example a ``/0``
address from a mobile hotspot), so netavark correctly rejects every IPv4 subnet
as conflicting. An IPv6-only network avoids the conflict and is a better fit for
the job sandbox: the job has no IPv4 address and no default route, so the only
reachable peer is the gateway proxy.

Podman's CLI and API always add an IPv4 subnet, even with ``--ipv6``, so the
network definition is written directly to the rootless network storage. Podman
picks it up like any other file-backed network.
"""

from __future__ import annotations

import json
import os
import secrets
import time
from pathlib import Path

DEFAULT_NETWORK = "kafkallm-jobs"
DEFAULT_SUBNET = "fd00:6b61:666b:616c::/64"
DEFAULT_INTERFACE = "podman1"


class JobNetworkError(RuntimeError):
    pass


def network_directory() -> Path:
    override = os.getenv("KAFKALLM_PODMAN_NETWORK_DIR", "").strip()
    if override:
        return Path(override).expanduser()
    data_home = os.getenv("XDG_DATA_HOME", "").strip()
    base = (
        Path(data_home).expanduser() if data_home else Path.home() / ".local" / "share"
    )
    return base / "containers" / "storage" / "networks"


def _used_interfaces(directory: Path) -> set[str]:
    used: set[str] = set()
    for config in directory.glob("*.json"):
        try:
            document = json.loads(config.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        interface = document.get("network_interface")
        if isinstance(interface, str) and interface:
            used.add(interface)
    return used


def _pick_interface(directory: Path, requested: str) -> str:
    used = _used_interfaces(directory)
    if requested:
        if requested in used:
            raise JobNetworkError(
                f"network interface {requested!r} is already used by another "
                "Podman network; set GOOSE_JOB_NETWORK_INTERFACE to a free name"
            )
        return requested
    for index in range(1, 100):
        candidate = f"podman{index}"
        if candidate not in used:
            return candidate
    raise JobNetworkError("could not find a free podman bridge interface name")


def ensure_job_network(
    *,
    name: str | None = None,
    subnet: str | None = None,
    interface: str | None = None,
    directory: Path | None = None,
) -> Path:
    """Write an IPv6-only internal network definition if it does not exist."""
    name = (name or os.getenv("GOOSE_JOB_NETWORK") or DEFAULT_NETWORK).strip()
    subnet = (subnet or os.getenv("GOOSE_JOB_NETWORK_SUBNET") or DEFAULT_SUBNET).strip()
    requested_interface = (
        interface
        if interface is not None
        else os.getenv("GOOSE_JOB_NETWORK_INTERFACE", "")
    ).strip()
    target_directory = directory or network_directory()

    if not name or "/" in name or name in {".", ".."}:
        raise JobNetworkError(f"invalid job network name: {name!r}")
    if ":" not in subnet or "/" not in subnet:
        raise JobNetworkError(
            f"GOOSE_JOB_NETWORK_SUBNET must be an IPv6 CIDR, got {subnet!r}"
        )

    target_directory.mkdir(parents=True, exist_ok=True)
    path = target_directory / f"{name}.json"
    if path.exists():
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise JobNetworkError(
                f"existing network definition {path} is not valid JSON: {exc}"
            ) from exc
        subnets = existing.get("subnets") or []
        if not any(
            isinstance(item, dict) and item.get("subnet") == subnet for item in subnets
        ):
            raise JobNetworkError(
                f"network {name!r} already exists with a different subnet; "
                f"remove {path} or choose another GOOSE_JOB_NETWORK"
            )
        return path

    bridge = _pick_interface(target_directory, requested_interface)
    document = {
        "name": name,
        "id": secrets.token_hex(32),
        "driver": "bridge",
        "network_interface": bridge,
        "created": time.strftime("%Y-%m-%dT%H:%M:%S.000000000Z", time.gmtime()),
        "subnets": [{"subnet": subnet, "gateway": _gateway(subnet)}],
        "ipv6_enabled": True,
        "internal": True,
        "dns_enabled": False,
        "ipam_options": {"driver": "host-local"},
        "containers": {},
    }
    path.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    os.chmod(path, 0o644)
    return path


def _gateway(subnet: str) -> str:
    try:
        import ipaddress

        address = ipaddress.IPv6Network(subnet, strict=False).network_address
        return str(address + 1)
    except ValueError as exc:
        raise JobNetworkError(f"invalid IPv6 subnet {subnet!r}: {exc}") from exc
