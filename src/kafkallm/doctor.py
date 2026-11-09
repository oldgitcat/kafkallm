"""Host and configuration checks for ``kafkallm doctor``.

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

from kafkallm.dotenv import load_dotenv
from kafkallm.settings import Settings

STATUSES = ("ok", "warn", "fail")
LABELS = {"ok": "PASS", "warn": "WARN", "fail": "FAIL"}
PLACEHOLDER_KEYS = {
    "",
    "change-me-to-a-long-random-string",
    "replace-with-openssl-rand-hex-32",
}


@dataclass(frozen=True, slots=True)
class Check:
    name: str
    status: str
    detail: str
    hint: str = ""


def valid_virtual_key(value: str) -> bool:
    return value.strip() not in PLACEHOLDER_KEYS and len(value.strip()) >= 32


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


def configuration_checks(
    settings: Settings, env: Mapping[str, str] | None = None
) -> list[Check]:
    """Checks that only depend on the environment, not on the host."""
    values = os.environ if env is None else env
    checks: list[Check] = []

    model = settings.goose_model.strip()
    checks.append(
        Check(
            "Goose model",
            "ok" if model else "fail",
            model or "not set",
            "Set GOOSE_MODEL to a tool-calling model.",
        )
    )

    provider_key = values.get("OPENROUTER_API_KEY", "").strip()
    checks.append(
        Check(
            "OpenRouter API key",
            "ok" if provider_key else "fail",
            "configured" if provider_key else "not set",
            "Get a key at https://openrouter.ai/keys and put it in .env.",
        )
    )

    virtual_key = settings.llm_gateway_virtual_key.strip()
    key_ok = valid_virtual_key(virtual_key)
    checks.append(
        Check(
            "Gateway virtual key",
            "ok" if key_ok else "fail",
            "configured" if key_ok else "missing or placeholder",
            "Run 'make setup' or set LLM_GATEWAY_VIRTUAL_KEY in .env.",
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

    checks.append(_command_check("Docker", "docker", ["--version"]))
    checks.append(_command_check("Podman", "podman", ["--version"]))

    if shutil.which("docker") is not None:
        compose = _run(["docker", "compose", "version"], timeout=10)
        checks.append(
            Check(
                "Docker Compose",
                "ok" if compose is not None and compose.returncode == 0 else "fail",
                (
                    (compose.stdout or compose.stderr).strip()
                    if compose
                    else "not available"
                ),
                "Install the Docker Compose plugin.",
            )
        )
        config = _run(["docker", "compose", "config", "--quiet"], timeout=15)
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

    job_root = Path(values.get("KAFKALLM_JOB_ROOT", "/var/lib/kafkallm"))
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

    gateway_url = f"http://127.0.0.1:{settings.llm_gateway_port}/v1/models"
    gateway_status = _http_status(gateway_url, settings.llm_gateway_virtual_key)
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
        checks.append(
            Check("LLM gateway", "ok", f"ready on port {settings.llm_gateway_port}")
        )
    elif gateway_status in {401, 403}:
        checks.append(
            Check(
                "LLM gateway",
                "fail",
                "virtual key rejected",
                "Restart the stack after changing LLM_GATEWAY_VIRTUAL_KEY.",
            )
        )
    else:
        checks.append(
            Check(
                "LLM gateway",
                "fail",
                f"HTTP {gateway_status}",
                "Check 'make logs' for agentgateway errors.",
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

    provider_key = os.getenv("OPENROUTER_API_KEY", "").strip()
    if not provider_key:
        checks.append(
            Check(
                "OpenRouter reachability",
                "warn",
                "skipped (no provider key)",
            )
        )
    else:
        status = _http_status("https://openrouter.ai/api/v1/key", provider_key)
        if status == 200:
            checks.append(
                Check("OpenRouter reachability", "ok", "provider key accepted")
            )
        elif status in {401, 403}:
            checks.append(
                Check(
                    "OpenRouter reachability",
                    "fail",
                    "provider key rejected",
                    "Create a new key and update OPENROUTER_API_KEY in .env.",
                )
            )
        else:
            checks.append(
                Check(
                    "OpenRouter reachability",
                    "warn",
                    "could not reach OpenRouter",
                    "Check your internet connection or run with --offline.",
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
        prog="kafkallm-doctor",
        description="Check host prerequisites, configuration, and services.",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Skip network checks against the gateway and OpenRouter.",
    )
    parser.add_argument(
        "--json", action="store_true", help="Print machine-readable JSON."
    )
    args = parser.parse_args(argv)
    load_dotenv()
    return run_doctor(offline=args.offline, as_json=args.json)


if __name__ == "__main__":
    raise SystemExit(main())
