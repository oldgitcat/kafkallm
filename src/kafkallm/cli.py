from __future__ import annotations

import argparse
import json
import logging
import time
import uuid

from confluent_kafka import Consumer, KafkaError, KafkaException

from kafkallm.kafka import KafkaProducer, ensure_topics
from kafkallm.models import FileRef, Job
from kafkallm.settings import Settings
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
        raise argparse.ArgumentTypeError("Dateireferenz muss die Form bucket/key haben.")
    return FileRef(bucket=bucket, key=key)


def _add_consumer_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--from-beginning", action="store_true")
    parser.add_argument("--max-messages", type=int, default=0)
    parser.add_argument(
        "--idle-timeout",
        type=float,
        default=0,
        help="Nach dieser Anzahl Sekunden ohne Nachricht beenden; 0 = unbegrenzt",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="kafkallm", description="LLM-Jobs über Redpanda verarbeiten"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("worker", help="Agent-Worker starten")

    submit = subparsers.add_parser("submit", help="Job an jobs.in senden")
    submit.add_argument("instruction", help="Aufgabe für den Agenten")
    submit.add_argument("--input", default="{}", help="JSON-Objekt mit Eingabedaten")
    submit.add_argument(
        "--input-file",
        action="append",
        default=[],
        type=_file_ref,
        help="S3-Referenz im Format bucket/key; mehrfach verwendbar",
    )
    submit.add_argument("--job-id")
    submit.add_argument("--run-id")
    submit.add_argument("--timeout", type=int, default=300)
    submit.add_argument("--max-steps", type=int, default=6)

    results = subparsers.add_parser("results", help="Ergebnisse aus jobs.out lesen")
    _add_consumer_arguments(results)
    events = subparsers.add_parser("events", help="Audit-Events aus agent.events lesen")
    _add_consumer_arguments(events)
    return parser


def submit(settings: Settings, args: argparse.Namespace) -> None:
    input_data = json.loads(args.input)
    if not isinstance(input_data, dict):
        raise ValueError("--input muss ein JSON-Objekt sein.")
    ensure_topics(settings)
    producer = KafkaProducer(settings)
    try:
        with get_tracer().start_as_current_span(
            "job.submit",
            attributes={"openinference.span.kind": "CHAIN"},
        ) as span:
            values = {
                "instruction": args.instruction,
                "input": input_data,
                "input_files": args.input_file,
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
    except (KafkaException, RuntimeError, ValueError, json.JSONDecodeError) as exc:
        parser.exit(1, f"Fehler: {exc}\n")
    finally:
        shutdown_telemetry()


if __name__ == "__main__":
    main()
