"""The isolated learning pass for a completed goose job."""

from __future__ import annotations

import logging
import os
import tempfile
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from kafkallm.audit import JobAudit
from kafkallm.goose_errors import JobCancelledError, WorkerShutdown
from kafkallm.goose_output import NO_OUTPUT_ERROR, parse_goose_run
from kafkallm.goose_scripts import _REFLECT_PROMPT, _REFLECT_RUN_SCRIPT
from kafkallm.models import Job
from kafkallm.skills import copy_skills, merge_skills

logger = logging.getLogger(__name__)


def run_reflect(
    runner: Any,
    *,
    job: Job,
    image: str,
    workdir: Path,
    outputs: Path,
    deadline: float,
    reflect_reserve: int,
    labels: dict[str, str],
    proxy_ipv6: str,
    audit: JobAudit | None,
    heartbeat: Callable[[], bool] | None,
) -> None:
    """Run the learning pass in a fresh microVM and merge its skill changes.

    The writable skill tree is created here, after the main microVM has
    exited, and only this tree is merged. A compromised main job therefore
    cannot forge a reflect success or write the skill tree that the worker
    persists. Nothing in the reflect workspace is taken from the main
    microVM; the learning pass sees only the already-collected outputs.
    """
    reflect_workdir: Path | None = None
    container: Any | None = None
    try:
        # Keep the reflect workspace next to, not inside, the main job
        # directory. The guest root may take ownership of the main
        # workspace root, which would leave the worker unable to create a
        # subdirectory there. The shared job root stays writable for the
        # worker, the name is random, and the main microVM never sees it.
        reflect_workdir = Path(
            tempfile.mkdtemp(
                prefix=".reflect-",
                dir=Path(runner.settings.goose_job_workspace_root),
            )
        )
        skills_dir = reflect_workdir / ".claude" / "skills"
        skills_dir.mkdir(parents=True, exist_ok=True)
        control_dir = reflect_workdir / ".kafkallm"
        control_dir.mkdir(parents=True, exist_ok=True)
        bundle = reflect_workdir / "bundle"
        bundle.mkdir(parents=True, exist_ok=True)
        (bundle / "run-reflect.sh").write_text(_REFLECT_RUN_SCRIPT, encoding="utf-8")
        (bundle / "reflect-prompt.txt").write_text(_REFLECT_PROMPT, encoding="utf-8")
        os.chmod(bundle / "run-reflect.sh", 0o755)
        for directory in (
            reflect_workdir,
            reflect_workdir / ".claude",
            control_dir,
        ):
            os.chmod(directory, 0o777)
        os.chmod(skills_dir, 0o777)
        os.chmod(bundle, 0o755)
        copied = copy_skills(Path(runner.settings.goose_job_skills_dir), skills_dir)
        if audit and (copied.rejected or copied.copied):
            audit.emit(
                "skills.seeded",
                attributes={
                    "copied": copied.copied,
                    "rejected": copied.rejected,
                    "files": copied.files,
                },
            )
        remaining = max(0.0, deadline - time.monotonic())
        reflect_seconds = int(min(reflect_reserve, remaining) - 5)
        if reflect_seconds < 10:
            if audit:
                audit.emit(
                    "goose.reflect",
                    attributes={"status": "skipped", "reason": "no time budget"},
                )
            return
        environment = runner._reflect_environment(reflect_seconds)
        container = runner._create_reflect_container(
            image=image,
            environment=environment,
            labels=labels,
            reflect_workdir=reflect_workdir,
            outputs=outputs,
            proxy_ipv6=proxy_ipv6,
        )
        runner._set_active(container)
        container.start()
        reflect_deadline = min(deadline, time.monotonic() + reflect_seconds)
        reflect_exit, timed_out = runner._wait(
            container, reflect_deadline, heartbeat=heartbeat
        )
        if timed_out:
            if audit:
                audit.emit(
                    "goose.reflect",
                    "failed",
                    attributes={"status": "timeout"},
                    error="The reflect run exceeded its time budget.",
                )
            return
        if reflect_exit not in (None, 0):
            if audit:
                audit.emit(
                    "goose.reflect",
                    "failed",
                    attributes={"status": "failed"},
                    error=(f"The reflect microVM exited with code {reflect_exit}."),
                )
            return
        if not audit_reflect(runner, reflect_workdir, audit):
            return
        merged = merge_skills(
            reflect_workdir / ".claude" / "skills",
            Path(runner.settings.goose_job_skills_dir),
        )
        if audit:
            if merged.persisted:
                audit.emit(
                    "skills.persisted",
                    "succeeded",
                    attributes={
                        "skills": merged.persisted,
                        "files": merged.files,
                        "bytes": merged.bytes,
                    },
                )
            if merged.rejected:
                audit.emit(
                    "skills.rejected",
                    "failed",
                    attributes={"skills": [name for name, _ in merged.rejected]},
                    error="; ".join(
                        f"{name}: {reason}" for name, reason in merged.rejected
                    )[:4000],
                )
    except (JobCancelledError, WorkerShutdown):
        raise
    except Exception as exc:
        logger.exception("Learning pass failed for job %s", job.job_id)
        if audit:
            audit.emit("goose.reflect", "failed", error=str(exc))
    finally:
        if container is not None:
            with runner._active_lock:
                if runner._active_container is container:
                    runner._active_container = None
            runner._remove_container(container)
        if reflect_workdir is not None:
            runner._remove_workdir(reflect_workdir)


def audit_reflect(runner: Any, workdir: Path, audit: JobAudit | None) -> bool:
    """Audit the learning pass and report whether its changes are trusted.

    The caller merges skills into the persistent store only when this
    returns true. The reflect container runs in a separate microVM and its
    writable skill tree is created by the worker after the main VM exits.
    """
    text, usage, steps, error = parse_goose_run(workdir, "reflect")
    exit_raw = runner._read_text(workdir, "reflect.exit").strip()
    if error == NO_OUTPUT_ERROR:
        stderr = runner._read_text(workdir, "reflect.err").strip()
        if exit_raw == "124":
            if audit:
                audit.emit(
                    "goose.reflect",
                    "failed",
                    attributes={"status": "timeout"},
                    error="The reflect run exceeded its time budget.",
                )
        elif stderr:
            if audit:
                audit.emit(
                    "goose.reflect",
                    "failed",
                    attributes={"status": "failed"},
                    error=stderr[:4000],
                )
        elif audit:
            audit.emit(
                "goose.reflect",
                attributes={"status": "skipped"},
            )
        return False
    if error or (exit_raw and exit_raw != "0"):
        if audit:
            audit.emit(
                "goose.reflect",
                "failed",
                attributes={"status": "failed", "usage": usage, "steps": steps},
                error=(error or text or f"Reflect exited with code {exit_raw}.")[:4000],
            )
        return False
    if audit:
        audit.emit(
            "goose.reflect",
            "succeeded",
            attributes={"status": "succeeded", "usage": usage, "steps": steps},
        )
    return True
