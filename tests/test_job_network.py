from __future__ import annotations

import json
from pathlib import Path

import pytest

from kafkallm.sandbox.network import JobNetworkError, ensure_job_network


def test_creates_an_ipv6_only_internal_network(tmp_path: Path) -> None:
    path = ensure_job_network(
        name="test-jobs",
        subnet="fd00:1234::/64",
        directory=tmp_path,
    )

    document = json.loads(path.read_text(encoding="utf-8"))
    assert document["name"] == "test-jobs"
    assert document["subnets"] == [
        {"subnet": "fd00:1234::/64", "gateway": "fd00:1234::1"}
    ]
    assert document["ipv6_enabled"] is True
    assert document["internal"] is True
    assert document["dns_enabled"] is False
    assert document["network_interface"] == "podman1"


def test_is_idempotent_and_picks_a_free_interface(tmp_path: Path) -> None:
    (tmp_path / "existing.json").write_text(
        json.dumps({"name": "existing", "network_interface": "podman1"}),
        encoding="utf-8",
    )

    first = ensure_job_network(
        name="test-jobs", subnet="fd00:1234::/64", directory=tmp_path
    )
    second = ensure_job_network(
        name="test-jobs", subnet="fd00:1234::/64", directory=tmp_path
    )

    assert first == second
    document = json.loads(first.read_text(encoding="utf-8"))
    assert document["network_interface"] == "podman2"


def test_refuses_a_conflicting_subnet(tmp_path: Path) -> None:
    ensure_job_network(name="test-jobs", subnet="fd00:1234::/64", directory=tmp_path)

    with pytest.raises(JobNetworkError, match="different subnet"):
        ensure_job_network(
            name="test-jobs", subnet="fd00:5678::/64", directory=tmp_path
        )


def test_requires_an_ipv6_subnet(tmp_path: Path) -> None:
    with pytest.raises(JobNetworkError, match="IPv6"):
        ensure_job_network(name="test-jobs", subnet="10.99.0.0/24", directory=tmp_path)
