"""End-to-end Kafka checks. Skipped unless a real broker is configured.

Set ``KAFKA_INTEGRATION_BOOTSTRAP`` (for example ``localhost:9092``) to run
them. CI starts Apache Kafka as a service container.
"""

from __future__ import annotations

import contextlib
import os
import time
import uuid
from dataclasses import replace

import pytest
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient
from gaensestall.core.models import Job, JobState
from gaensestall.core.settings import Settings
from gaensestall.kafka.client import KafkaProducer, wait_for_kafka
from gaensestall.kafka.state import StatePublisher

BOOTSTRAP = os.getenv("KAFKA_INTEGRATION_BOOTSTRAP")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not BOOTSTRAP,
        reason="KAFKA_INTEGRATION_BOOTSTRAP is not set",
    ),
]


def _settings() -> Settings:
    suffix = uuid.uuid4().hex[:8]
    return replace(
        Settings.from_env(),
        kafka_bootstrap_servers=BOOTSTRAP or "",
        jobs_topic=f"test.jobs.{suffix}",
        results_topic=f"test.results.{suffix}",
        state_topic=f"test.state.{suffix}",
        events_topic=f"test.events.{suffix}",
        control_topic=f"test.controls.{suffix}",
        logs_topic=f"test.logs.{suffix}",
        agents_topic=f"test.agents.{suffix}",
        sandboxes_topic=f"test.sandboxes.{suffix}",
        kafka_startup_timeout_seconds=60,
        kafka_topic_partitions=1,
    )


def _consume_one(settings: Settings, topic: str) -> bytes:
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": f"gaensestall-integration-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    deadline = time.monotonic() + 30
    try:
        while time.monotonic() < deadline:
            message = consumer.poll(timeout=0.5)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(message.error())
            value = message.value()
            if value is not None:
                return value
    finally:
        consumer.close()
    raise AssertionError(f"No message arrived on {topic}")


def _delete_topics(settings: Settings) -> None:
    names = [
        settings.jobs_topic,
        settings.results_topic,
        settings.state_topic,
        settings.events_topic,
        settings.control_topic,
        settings.logs_topic,
        settings.agents_topic,
        settings.sandboxes_topic,
    ]
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    for future in admin.delete_topics(names, operation_timeout=10).values():
        with contextlib.suppress(Exception):
            future.result(timeout=10)


def test_job_and_state_round_trip() -> None:
    settings = _settings()
    wait_for_kafka(settings)
    producer = KafkaProducer(settings)
    job = Job(
        instruction="Integration test",
        sandbox_image="localhost/integration:1",
        multi_agent=False,
    )
    try:
        producer.send(settings.jobs_topic, job.job_id, job)
        assert StatePublisher(settings, producer).queued(job) is True
        raw_job = _consume_one(settings, settings.jobs_topic)
        raw_state = _consume_one(settings, settings.state_topic)
    finally:
        producer.close()
        _delete_topics(settings)

    stored_job = Job.model_validate_json(raw_job)
    stored_state = JobState.model_validate_json(raw_state)

    assert stored_job.job_id == job.job_id
    assert stored_job.sandbox_image == "localhost/integration:1"
    assert stored_state.job_id == job.job_id
    assert stored_state.status == "queued"
    assert stored_state.instruction == job.instruction
