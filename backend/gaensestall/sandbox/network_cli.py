"""Standalone entry point for creating the host-side job network.

This module deliberately imports only the standard library and
``gaensestall.core.settings``/``gaensestall.sandbox.network``. It runs before the Python
package is installed, so ``make network`` works on a fresh checkout.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from gaensestall.core.dotenv import load_dotenv
from gaensestall.core.settings import Settings
from gaensestall.sandbox.network import ensure_job_network


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="gaensestall-network",
        description="Create the isolated IPv6-only Podman network used by jobs.",
    )
    parser.add_argument("--name", help="Network name (default: GOOSE_JOB_NETWORK)")
    parser.add_argument(
        "--subnet", help="IPv6 CIDR (default: GOOSE_JOB_NETWORK_SUBNET)"
    )
    parser.add_argument(
        "--interface",
        help="Podman bridge interface (default: next free podmanN)",
    )
    parser.add_argument(
        "--directory",
        type=Path,
        help="Override the rootless Podman network directory",
    )
    args = parser.parse_args(argv)
    load_dotenv()

    settings = Settings.from_env()
    directory = args.directory
    if directory is None and settings.goose_job_network_dir.strip():
        directory = Path(settings.goose_job_network_dir)

    path = ensure_job_network(
        name=args.name or settings.goose_job_network,
        subnet=args.subnet or settings.goose_job_network_subnet,
        interface=(
            args.interface
            if args.interface is not None
            else settings.goose_job_network_interface
        ),
        directory=directory,
    )
    print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
