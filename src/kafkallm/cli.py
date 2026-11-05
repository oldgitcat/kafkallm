from __future__ import annotations

import argparse
import json
import logging
import time
import uuid
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, KafkaException

from kafkallm.kafka import KafkaProducer, ensure_topics
from kafkallm.models import FileRef, Job
from kafkallm.settings import Settings
from kafkallm.state import StatePublisher
from kafkallm.telemetry import (
    configure_telemetry,
    current_trace_id,
    get_tracer,
    inject_trace_headers,
    new_trace_id,
    set_content,
    shutdown_telemetry,
)
from kafkallm.worker import Worker


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
        prog="kafkallm", description="Run goose jobs through Redpanda"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("worker", help="Start the agent worker")
    subparsers.add_parser("gui", help="Start the job GUI")
    subparsers.add_parser(
        "init-job-network",
        help="Create the isolated IPv6-only Podman network for jobs",
    )

    submit = subparsers.add_parser("submit", help="Send a job to jobs.in")
    submit.add_argument("instruction", help="Task for the agent")
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
        choices=("default", "reverse"),
        default="default",
        help="Job sandbox image: default or reverse (REA + Ghidra)",
    )
    submit.add_argument("--job-id")
    submit.add_argument("--run-id")
    submit.add_argument("--timeout", type=int, default=300)
    submit.add_argument("--max-steps", type=int, default=6)

    results = subparsers.add_parser("results", help="Read results from jobs.out")
    _add_consumer_arguments(results)
    events = subparsers.add_parser("events", help="Read audit events from agent.events")
    _add_consumer_arguments(events)
    states = subparsers.add_parser("states", help="Read job state from jobs.state")
    _add_consumer_arguments(states)
    return parser


def submit(settings: Settings, args: argparse.Namespace) -> None:
    input_data = json.loads(args.input)
    if not isinstance(input_data, dict):
        raise ValueError("--input must be a JSON object.")
    input_files = list(args.input_file)
    if args.upload:
        from kafkallm.storage import S3Storage

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
    ensure_topics(settings)
    producer = KafkaProducer(settings)
    state = StatePublisher(settings, producer)
    try:
        with get_tracer().start_as_current_span(
            "job.submit",
            attributes={"openinference.span.kind": "CHAIN"},
        ) as span:
            values = {
                "instruction": args.instruction,
                "input": input_data,
                "input_files": input_files,
                "sandbox": args.sandbox,
                "timeout_seconds": args.timeout,
                "max_steps": args.max_steps,
                "trace_id": current_trace_id() or new_trace_id(),
            }
            if args.job_id:
                values["job_id"] = args.job_id
            if args.run_id:
                values["run_id"] = args.run_id
            job = Job.model_validate(values)
            span.set_attribute("job.id", job.job_id)
            span.set_attribute("job.run_id", job.run_id)
            set_content(span, "input.value", job.instruction)
            producer.send(
                settings.jobs_topic,
                job.job_id,
                job,
                headers=inject_trace_headers(),
            )
            state.queued(job, worker_id=settings.worker_id)
    finally:
        producer.close()
    print(job.model_dump_json(indent=2))


def consume(settings: Settings, args: argparse.Namespace, topic: str) -> None:
    ensure_topics(settings)
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": f"kafkallm-topic-viewer-{uuid.uuid4()}",
            "auto.offset.reset": "earliest" if args.from_beginning else "latest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    received = 0
    last_message = time.monotonic()
    try:
        while True:
            message = consumer.poll(timeout=1.0)
            if message is None:
                if (
                    args.idle_timeout > 0
                    and time.monotonic() - last_message >= args.idle_timeout
                ):
                    break
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(message.error())
            value = message.value()
            print(value.decode("utf-8") if value is not None else "null", flush=True)
            received += 1
            last_message = time.monotonic()
            if args.max_messages and received >= args.max_messages:
                break
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = build_parser()
    args = parser.parse_args()
    settings = Settings.from_env()
    configure_telemetry(settings)
    try:
        if args.command == "worker":
            Worker(settings).run()
        elif args.command == "submit":
            submit(settings, args)
        elif args.command == "results":
            consume(settings, args, settings.results_topic)
        elif args.command == "events":
            consume(settings, args, settings.events_topic)
        elif args.command == "states":
            consume(settings, args, settings.state_topic)
        elif args.command == "gui":
            from kafkallm.gui import run_gui

            run_gui(settings)
        elif args.command == "init-job-network":
            from kafkallm.job_network import ensure_job_network

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


if __name__ == "__main__":
    main()
