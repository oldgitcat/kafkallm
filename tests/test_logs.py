from __future__ import annotations

from dataclasses import replace

from kafkallm.core.models import JobLog
from kafkallm.core.settings import Settings
from kafkallm.kafka.logs import LogPublisher


class FakeProducer:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str, JobLog]] = []

    def send_async(self, topic: str, key: str, model: JobLog) -> None:
        self.messages.append((topic, key, model))


def test_log_publisher_sequences_and_truncates() -> None:
    settings = replace(Settings.from_env(), logs_topic="jobs.logs")
    producer = FakeProducer()
    publisher = LogPublisher(settings, producer)  # type: ignore[arg-type]

    publisher.publish("job-1", "thinking", "")
    publisher.publish("job-1", "thinking", "x" * 30_000)
    publisher.publish("job-1", "goose.tool", '{"name": "shell"}')

    assert len(producer.messages) == 2
    first_topic, first_key, first = producer.messages[0]
    assert first_topic == "jobs.logs"
    assert first_key == "job-1"
    assert first.sequence == 0
    assert len(first.text) == 20_000
    assert first.text.endswith("…")
    assert producer.messages[1][2].sequence == 1
