"""Host and configuration checks for ``gaensestall doctor``.

The command is intentionally dependency-free so it can run before the Python
package is installed. It reports what is missing and how to fix it instead of
starting a stack that will fail later during job startup.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from gaensestall.core.dotenv import load_dotenv
from gaensestall.core.settings import Settings, default_job_root
from gaensestall.llm.env import (
    PLACEHOLDER_LLM_PROXY_TOKENS,
    api_key_from_env,
    base_url_from_env,
    gateway_endpoint_from_env,
    upstream_model_from_env,
)

STATUSES = ("ok", "warn", "fail")
LABELS = {"ok": "PASS", "warn": "WARN", "fail": "FAIL"}


@dataclass(frozen=True, slots=True)
class Check:
    name: str
    status: str
    detail: str
    hint: str = ""


def valid_llm_proxy_token(value: str) -> bool:
    return (
        value.strip() not in PLACEHOLDER_LLM_PROXY_TOKENS and len(value.strip()) >= 32
    )


def _run(args: list[str], timeout: int = 10) -> subprocess.CompletedProcess[str] | None:
    try:
        return subprocess.run(
            args,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None


def _http_status(url: str, token: str = "", timeout: float = 4.0) -> int | None:
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    request = Request(url, headers=headers)
    try:
        with urlopen(request, timeout=timeout) as response:
            return response.status
    except HTTPError as exc:
        return exc.code
    except (URLError, OSError, TimeoutError):
        return None


def _http_models(url: str, timeout: float = 4.0) -> tuple[int | None, set[str]]:
    """Return the HTTP status and model ids from an OpenAI-compatible list."""
    try:
        with urlopen(Request(url), timeout=timeout) as response:
            payload = json.load(response)
            status = response.status
    except HTTPError as exc:
        return exc.code, set()
    except (URLError, OSError, TimeoutError, ValueError):
        return None, set()
    data = payload.get("data") if isinstance(payload, dict) else None
    models = {
        item["id"]
        for item in data or []
        if isinstance(item, dict) and isinstance(item.get("id"), str)
    }
    return status, models


def configuration_checks(
    settings: Settings, env: Mapping[str, str] | None = None
) -> list[Check]:
    """Checks that only depend on the environment, not on the host."""
    values = os.environ if env is None else env
    checks: list[Check] = []

    endpoint = (
        gateway_endpoint_from_env(values) or settings.mlflow_gateway_endpoint
    ).strip()
    checks.append(
        Check(
            "Gateway endpoint",
            "ok" if endpoint else "fail",
            endpoint or "not set",
            "Set MLFLOW_GATEWAY_ENDPOINT in .env.",
        )
    )

    model = settings.goose_model.strip()
    model_ok = bool(model) and model == endpoint
    checks.append(
        Check(
            "Goose model",
            "ok" if model_ok else "fail",
            model or "not set",
            "GOOSE_MODEL must match the MLflow gateway endpoint "
            f"({endpoint or 'MLFLOW_GATEWAY_ENDPOINT'}).",
        )
    )

    base_url = base_url_from_env(values)
    checks.append(
        Check(
            "LLM endpoint",
            "ok" if base_url else "fail",
            base_url or "not set",
            "Set LLM_UPSTREAM_BASE_URL to an OpenAI-compatible /v1 endpoint.",
        )
    )
    upstream_model = upstream_model_from_env(values)
    checks.append(
        Check(
            "Upstream model",
            "ok" if upstream_model else "fail",
            upstream_model or "not set",
            "Set LLM_UPSTREAM_MODEL to the model id used by the provider.",
        )
    )
    provider_key = api_key_from_env(values)
    checks.append(
        Check(
            "Provider API key",
            "ok" if provider_key else "warn",
            "configured" if provider_key else "not set",
            "Set LLM_UPSTREAM_API_KEY for authenticated endpoints.",
        )
    )

    virtual_key = settings.llm_proxy_token.strip()
    key_ok = valid_llm_proxy_token(virtual_key)
    checks.append(
        Check(
            "LLM proxy token",
            "ok" if key_ok else "fail",
            "configured" if key_ok else "missing or placeholder",
            "Run 'make setup' or set LLM_PROXY_TOKEN in .env.",
        )
    )

    return checks


def _command_check(name: str, command: str, version_args: list[str]) -> Check:
    path = shutil.which(command)
    if path is None:
        return Check(name, "fail", "not installed", f"Install {command}.")
    result = _run([command, *version_args], timeout=10)
    if result is None or result.returncode != 0:
        return Check(
            name, "fail", "installed but not working", f"Run '{command}' manually."
        )
    detail = (result.stdout or result.stderr).strip().splitlines()
    return Check(name, "ok", detail[0] if detail else path)


def host_checks(
    settings: Settings, env: Mapping[str, str] | None = None
) -> list[Check]:
    values = os.environ if env is None else env
    checks: list[Check] = []

    version = sys.version.split()[0]
    checks.append(
        Check(
            "Python",
            "ok" if sys.version_info >= (3, 12) else "fail",
            version,
            "Gänsestall requires Python 3.12 or newer.",
        )
    )

    env_file = Path(".env")
    checks.append(
        Check(
            ".env file",
            "ok" if env_file.is_file() else "warn",
            "present" if env_file.is_file() else "missing",
            "Run 'make setup' to create it from .env.example.",
        )
    )

    checks.append(_command_check("Podman", "podman", ["--version"]))

    if shutil.which("podman") is not None:
        compose = _run(["podman", "compose", "version"], timeout=10)
        checks.append(
            Check(
                "Podman Compose",
                "ok" if compose is not None and compose.returncode == 0 else "fail",
                (
                    (compose.stdout or compose.stderr).strip()
                    if compose
                    else "not available"
                ),
                "Install a Compose provider such as the Docker Compose plugin.",
            )
        )
        config = _run(["podman", "compose", "config", "--quiet"], timeout=15)
        config_ok = config is not None and config.returncode == 0
        checks.append(
            Check(
                "Compose configuration",
                "ok" if config_ok else "fail",
                (
                    "valid"
                    if config_ok
                    else (
                        (config.stderr or config.stdout).strip()
                        if config
                        else "not validated"
                    )
                ),
                "Fix compose.yaml or .env.",
            )
        )

    kvm = Path("/dev/kvm")
    kvm_ok = kvm.exists() and os.access(kvm, os.R_OK | os.W_OK)
    checks.append(
        Check(
            "KVM",
            "ok" if kvm_ok else "fail",
            "readable and writable" if kvm_ok else "missing or not accessible",
            "Enable virtualization in the BIOS and make /dev/kvm writable for your user.",
        )
    )

    uid = os.getuid()
    socket_path = Path(
        values.get("PODMAN_SOCKET") or f"/run/user/{uid}/podman/podman.sock"
    )
    checks.append(
        Check(
            "Podman socket",
            "ok" if socket_path.exists() else "fail",
            str(socket_path),
            "Run 'systemctl --user enable --now podman.socket'.",
        )
    )

    if shutil.which("podman") is not None:
        network = _run(
            ["podman", "network", "exists", settings.goose_job_network], timeout=10
        )
        checks.append(
            Check(
                "Job network",
                "ok" if network is not None and network.returncode == 0 else "warn",
                settings.goose_job_network,
                "Run 'make network' once on the host.",
            )
        )
        image = _run(["podman", "image", "exists", settings.goose_image], timeout=10)
        checks.append(
            Check(
                "Goose job image",
                "ok" if image is not None and image.returncode == 0 else "warn",
                settings.goose_image,
                "Run 'make images'.",
            )
        )
        proxy_image = _run(
            ["podman", "image", "exists", settings.llm_proxy_image],
            timeout=10,
        )
        checks.append(
            Check(
                "Proxy image",
                (
                    "ok"
                    if proxy_image is not None and proxy_image.returncode == 0
                    else "warn"
                ),
                settings.llm_proxy_image,
                "Run 'make llm-proxy-image'.",
            )
        )

    job_root = Path(values.get("GAENSESTALL_JOB_ROOT") or default_job_root())
    root_ok = job_root.is_dir() and os.access(job_root, os.W_OK)
    checks.append(
        Check(
            "Job root",
            "ok" if root_ok else "warn",
            str(job_root),
            "It is created by 'make up'. Check permissions if it already exists.",
        )
    )

    return checks


def service_checks(settings: Settings, offline: bool = False) -> list[Check]:
    if offline:
        return [Check("Network checks", "ok", "skipped (--offline)")]

    checks: list[Check] = []

    gateway_url = f"http://127.0.0.1:{settings.mlflow_ui_port}/gateway/mlflow/v1/models"
    gateway_status, gateway_models = _http_models(gateway_url)
    if gateway_status is None:
        checks.append(
            Check(
                "LLM gateway",
                "warn",
                "not reachable",
                "Run 'make up', then check 'make logs'.",
            )
        )
    elif gateway_status in {200, 204}:
        endpoint = settings.mlflow_gateway_endpoint
        if gateway_models and endpoint not in gateway_models:
            checks.append(
                Check(
                    "LLM gateway",
                    "warn",
                    f"endpoint {endpoint!r} not provisioned",
                    "Run 'make up' to provision the MLflow AI Gateway endpoint.",
                )
            )
        else:
            checks.append(
                Check(
                    "LLM gateway",
                    "ok",
                    f"MLflow endpoint {endpoint!r} on port {settings.mlflow_ui_port}",
                )
            )
    else:
        checks.append(
            Check(
                "LLM gateway",
                "fail",
                f"HTTP {gateway_status}",
                "Check 'make logs' for MLflow errors.",
            )
        )

    gui_url = f"http://127.0.0.1:{settings.gui_port}/healthz"
    gui_status = _http_status(gui_url)
    checks.append(
        Check(
            "GUI",
            "ok" if gui_status == 200 else "warn",
            f"ready on port {settings.gui_port}"
            if gui_status == 200
            else "not reachable",
            "Run 'make up', then check 'make logs'.",
        )
    )

    provider_key = api_key_from_env(os.environ)
    base_url = base_url_from_env(os.environ)
    upstream_url = ""
    if base_url:
        upstream_url = base_url.replace("host.containers.internal", "127.0.0.1")
        upstream_url = upstream_url.rstrip("/")
        if not upstream_url.endswith("/models"):
            upstream_url += "/models"

    if not upstream_url:
        checks.append(
            Check(
                "LLM upstream",
                "warn",
                "not probed (no base URL)",
                "Set LLM_UPSTREAM_BASE_URL to enable a reachability check.",
            )
        )
    else:
        status = _http_status(upstream_url, provider_key)
        if status in {200, 204}:
            checks.append(Check("LLM upstream", "ok", "endpoint accepted the request"))
        elif status in {401, 403}:
            checks.append(
                Check(
                    "LLM upstream",
                    "fail",
                    "endpoint rejected the provider key",
                    "Create a new key and update LLM_UPSTREAM_API_KEY in .env.",
                )
            )
        elif status == 404:
            checks.append(
                Check(
                    "LLM upstream",
                    "warn",
                    "endpoint does not expose /models",
                    "The endpoint may still work for chat completions.",
                )
            )
        elif status is None:
            checks.append(
                Check(
                    "LLM upstream",
                    "warn",
                    "could not reach the endpoint",
                    "Check LLM_UPSTREAM_BASE_URL and your network, or run "
                    "with --offline.",
                )
            )
        else:
            checks.append(
                Check(
                    "LLM upstream",
                    "warn",
                    f"endpoint returned HTTP {status}",
                    "Check the endpoint settings and 'make logs'.",
                )
            )

    return checks


def format_report(checks: list[Check]) -> str:
    width = max((len(check.name) for check in checks), default=0)
    lines = ["Gänsestall doctor", ""]
    for check in checks:
        line = f"{LABELS.get(check.status, check.status.upper()):4}  {check.name:<{width}}  {check.detail}"
        lines.append(line)
        if check.hint and check.status != "ok":
            lines.append(f"      {'':<{width}}  -> {check.hint}")

    counts = {
        status: sum(check.status == status for check in checks) for status in STATUSES
    }
    lines.append("")
    lines.append(
        f"Summary: {counts['ok']} passed, {counts['warn']} warnings, "
        f"{counts['fail']} failed"
    )
    return "\n".join(lines)


def run_doctor(
    settings: Settings | None = None,
    *,
    offline: bool = False,
    as_json: bool = False,
) -> int:
    settings = settings or Settings.from_env()
    checks = [
        *configuration_checks(settings),
        *host_checks(settings),
        *service_checks(settings, offline=offline),
    ]
    exit_code = 1 if any(check.status == "fail" for check in checks) else 0

    if as_json:
        print(
            json.dumps(
                {
                    "checks": [asdict(check) for check in checks],
                    "exit_code": exit_code,
                },
                indent=2,
            )
        )
    else:
        print(format_report(checks))
    return exit_code


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="gaensestall-doctor",
        description="Check host prerequisites, configuration, and services.",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Skip network checks against the gateway and LLM provider.",
    )
    parser.add_argument(
        "--json", action="store_true", help="Print machine-readable JSON."
    )
    args = parser.parse_args(argv)
    load_dotenv()
    return run_doctor(offline=args.offline, as_json=args.json)


if __name__ == "__main__":
    raise SystemExit(main())
