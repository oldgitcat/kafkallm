from __future__ import annotations

import argparse
import json
import logging
import sys
import threading
import time
from pathlib import Path

from confluent_kafka import KafkaException

from kafkallm.core.dotenv import load_dotenv
from kafkallm.core.models import AgentSpec, FileRef, Job, SandboxSpec
from kafkallm.core.settings import Settings
from kafkallm.core.telemetry import (
    configure_telemetry,
    get_tracer,
    set_content,
    set_session,
    shutdown_telemetry,
)
from kafkallm.kafka.consumer import iter_messages
from kafkallm.kafka.jobs import build_job, submit_job
from kafkallm.kafka.kafka import KafkaProducer, ensure_topics
from kafkallm.sandbox.sandboxes import resolve_sandbox
from kafkallm.worker.worker import run_workers


def _file_ref(value: str) -> FileRef:
    bucket, separator, key = value.partition("/")
    if not separator or not bucket or not key:
        raise argparse.ArgumentTypeError("File reference must use the form bucket/key.")
    return FileRef(bucket=bucket, key=key)


def _add_consumer_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--from-beginning", action="store_true")
    parser.add_argument("--max-messages", type=int, default=0)
    parser.add_argument(
        "--idle-timeout",
        type=float,
        default=0,
        help="Stop after this many seconds without a message; 0 = unlimited",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="kafkallm", description="Run goose jobs through Apache Kafka"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("worker", help="Start the agent worker")
    subparsers.add_parser("gui", help="Start the job GUI")
    subparsers.add_parser(
        "init-job-network",
        help="Create the isolated IPv6-only Podman network for jobs",
    )
    doctor = subparsers.add_parser(
        "doctor",
        help="Check prerequisites, configuration, and running services",
    )
    doctor.add_argument(
        "--offline",
        action="store_true",
        help="Skip network checks against the gateway and LLM provider",
    )
    doctor.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON",
    )

    submit = subparsers.add_parser("submit", help="Send a job to jobs.in")
    submit.add_argument("instruction", nargs="?", help="Task for the agent")
    submit.add_argument(
        "--job-json",
        metavar="FILE",
        help=(
            "Read a complete Job JSON payload from FILE, or '-' for stdin. "
            "The payload defines the whole job."
        ),
    )
    submit.add_argument("--input", default="{}", help="JSON object with input data")
    submit.add_argument(
        "--input-file",
        action="append",
        default=[],
        type=_file_ref,
        help="S3 reference in bucket/key form; may be repeated",
    )
    submit.add_argument(
        "--upload",
        action="append",
        default=[],
        type=Path,
        help="Upload a local file into the job inputs; may be repeated",
    )
    submit.add_argument(
        "--sandbox",
        default="default",
        help="Sandbox ID from the GUI environment registry (default: default)",
    )
    submit.add_argument(
        "--image",
        help=(
            "Run this OCI image reference instead of the registry image; the "
            "worker pulls it from its registry if it is not local"
        ),
    )
    submit.add_argument("--job-id")
    submit.add_argument("--run-id")
    submit.add_argument("--timeout", type=int, default=300)
    submit.add_argument(
        "--unlimited",
        action="store_true",
        help=(
            "Ignore the job timeout and turn cap and preserve oversized results "
            "as a result-full.md artifact."
        ),
    )
    submit.add_argument(
        "--no-reflect",
        action="store_true",
        help="Skip the learning pass (skill improvement) for this job",
    )

    schema = subparsers.add_parser(
        "schema",
        help="Print the JSON schema for a job, agent, or sandbox record",
    )
    schema.add_argument(
        "model",
        nargs="?",
        choices=("job", "agent", "sandbox"),
        default="job",
        help="Model to describe (default: job)",
    )

    results = subparsers.add_parser("results", help="Read results from jobs.out")
    _add_consumer_arguments(results)
    events = subparsers.add_parser("events", help="Read audit events from agent.events")
    _add_consumer_arguments(events)
    logs = subparsers.add_parser("logs", help="Read live activity from jobs.logs")
    _add_consumer_arguments(logs)
    states = subparsers.add_parser("states", help="Read job state from jobs.state")
    _add_consumer_arguments(states)
    return parser


def _job_payload(args: argparse.Namespace) -> dict:
    if args.job_json:
        if args.instruction is not None:
            raise ValueError("Provide either an instruction or --job-json, not both.")
        conflicts = [
            name
            for name, value, default in (
                ("--input", args.input, "{}"),
                ("--input-file", args.input_file, []),
                ("--upload", args.upload, []),
                ("--sandbox", args.sandbox, "default"),
                ("--image", args.image, None),
                ("--job-id", args.job_id, None),
                ("--run-id", args.run_id, None),
                ("--timeout", args.timeout, 300),
                ("--unlimited", args.unlimited, False),
                ("--no-reflect", args.no_reflect, False),
            )
            if value != default
        ]
        if conflicts:
            raise ValueError(
                "--job-json defines the complete job; do not combine it with "
                + ", ".join(conflicts)
                + "."
            )
        try:
            if args.job_json == "-":
                raw = sys.stdin.read()
            else:
                raw = Path(args.job_json).read_text(encoding="utf-8")
        except OSError as exc:
            raise ValueError(f"Could not read --job-json: {exc}") from exc
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise ValueError("--job-json must contain a JSON object.")
        return payload

    if args.instruction is None:
        raise ValueError("Provide an instruction or --job-json.")
    input_data = json.loads(args.input)
    if not isinstance(input_data, dict):
        raise ValueError("--input must be a JSON object.")
    payload = {
        "instruction": args.instruction,
        "input": input_data,
        "input_files": list(args.input_file),
        "sandbox": args.sandbox,
        "unlimited": args.unlimited,
        "timeout_seconds": args.timeout,
    }
    if args.job_id:
        payload["job_id"] = args.job_id
    if args.run_id:
        payload["run_id"] = args.run_id
    if args.no_reflect:
        payload["reflect"] = False
    return payload


def _attach_uploads(
    settings: Settings,
    args: argparse.Namespace,
    payload: dict,
) -> None:
    if not args.upload:
        return
    from kafkallm.core.storage import S3Storage

    input_files = list(payload.get("input_files") or [])
    if len(input_files) + len(args.upload) > settings.upload_max_files:
        raise ValueError(
            f"At most {settings.upload_max_files} input files are allowed."
        )
    storage = S3Storage(settings)
    total = 0
    for path in args.upload:
        data = path.read_bytes()
        if len(data) > settings.upload_max_file_bytes:
            raise ValueError(f"{path} exceeds the per-file upload size limit.")
        total += len(data)
        if total > settings.upload_max_total_bytes:
            raise ValueError("The uploads exceed the total size limit.")
        input_files.append(storage.upload_input(path.name, data))
    payload["input_files"] = input_files


def _attach_sandbox_image(
    settings: Settings,
    payload: dict,
    image: str | None = None,
) -> None:
    """Record the OCI image the worker must run for this job.

    The image reference is part of the Job message so the worker can pull it
    from a registry when it is not in local Podman storage.
    """
    if payload.get("sandbox_image"):
        return
    sandbox_id = str(payload.get("sandbox", "default"))
    sandbox = resolve_sandbox(settings, sandbox_id)
    if sandbox is not None and not sandbox.enabled:
        raise ValueError(f"Sandbox {sandbox_id!r} is disabled.")
    if image is not None:
        payload["sandbox_image"] = image
        return
    if sandbox is None:
        raise ValueError(
            f"Unknown sandbox: {sandbox_id}. Add it in the GUI sandbox registry "
            "or pass --image."
        )
    payload["sandbox_image"] = sandbox.image


def submit(settings: Settings, args: argparse.Namespace) -> None:
    payload = _job_payload(args)
    _attach_uploads(settings, args, payload)
    ensure_topics(settings)
    _attach_sandbox_image(settings, payload, args.image)
    producer = KafkaProducer(settings)
    try:
        with get_tracer().start_as_current_span(
            "job.submit",
            attributes={"openinference.span.kind": "CHAIN"},
        ) as span:
            job = build_job(settings, payload)
            span.set_attribute("job.id", job.job_id)
            span.set_attribute("job.run_id", job.run_id)
            set_session(span, job.job_id)
            set_content(span, "input.value", job.instruction)
            submit_job(settings, producer, job, worker_id=settings.worker_id)
    finally:
        producer.close()
    print(job.model_dump_json(indent=2))


_SCHEMA_MODELS = {
    "job": Job,
    "agent": AgentSpec,
    "sandbox": SandboxSpec,
}


def print_schema(model: str) -> None:
    print(json.dumps(_SCHEMA_MODELS[model].model_json_schema(), indent=2))


def consume(settings: Settings, args: argparse.Namespace, topic: str) -> None:
    ensure_topics(settings)
    stop = threading.Event()
    received = 0
    last_message = time.monotonic()

    def idle() -> bool:
        return (
            args.idle_timeout > 0
            and time.monotonic() - last_message >= args.idle_timeout
        )

    try:
        for message in iter_messages(
            settings,
            [topic],
            "kafkallm-topic-viewer",
            stop,
            offset_reset="earliest" if args.from_beginning else "latest",
            on_idle=idle,
        ):
            value = message.value()
            print(value.decode("utf-8") if value is not None else "null", flush=True)
            received += 1
            last_message = time.monotonic()
            if args.max_messages and received >= args.max_messages:
                break
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = build_parser()
    args = parser.parse_args()
    load_dotenv()
    settings = Settings.from_env()
    configure_telemetry(settings)
    exit_code = 0
    try:
        if args.command == "worker":
            run_workers(settings)
        elif args.command == "submit":
            submit(settings, args)
        elif args.command == "schema":
            print_schema(args.model)
        elif args.command == "results":
            consume(settings, args, settings.results_topic)
        elif args.command == "events":
            consume(settings, args, settings.events_topic)
        elif args.command == "logs":
            consume(settings, args, settings.logs_topic)
        elif args.command == "states":
            consume(settings, args, settings.state_topic)
        elif args.command == "gui":
            from tools.gui.gui import run_gui

            run_gui(settings)
        elif args.command == "doctor":
            from kafkallm.setup.doctor import run_doctor

            exit_code = run_doctor(settings, offline=args.offline, as_json=args.json)
        elif args.command == "init-job-network":
            from kafkallm.sandbox.network import ensure_job_network

            directory = (
                Path(settings.goose_job_network_dir)
                if settings.goose_job_network_dir.strip()
                else None
            )
            path = ensure_job_network(
                name=settings.goose_job_network,
                subnet=settings.goose_job_network_subnet,
                interface=settings.goose_job_network_interface,
                directory=directory,
            )
            print(path)
    except (KafkaException, RuntimeError, ValueError, json.JSONDecodeError) as exc:
        parser.exit(1, f"Error: {exc}\n")
    finally:
        shutdown_telemetry()
    if exit_code:
        raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
