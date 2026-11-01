from dataclasses import replace

from kafkallm.audit import AuditLogger
from kafkallm.models import AgentEvent, Job
from kafkallm.settings import Settings


class RecordingProducer:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str, AgentEvent]] = []

    def send_async(self, topic, key, model, headers=None) -> None:
        del headers
        self.messages.append((topic, key, model))


class FailingProducer:
    def send_async(self, topic, key, model, headers=None) -> None:
        del topic, key, model, headers
        raise BufferError("Kafka queue unavailable")


def test_audit_events_keep_trace_and_parent_relationship() -> None:
    producer = RecordingProducer()
    settings = replace(
        Settings.from_env(),
        agent_name="test-agent",
        events_topic="agent.events",
    )
    job = Job(instruction="Hallo", trace_id="trace-123")
    audit = AuditLogger(settings, producer).for_job(job)

    llm_event_id = audit.emit(
        "llm.completed", "succeeded", attributes={"model": "test-model"}
    )
    tool_event_id = audit.emit(
        "tool.completed",
        "failed",
        parent_event_id=llm_event_id,
        duration_ms=12.5,
        attributes={"tool": "example"},
        error="Fehler",
    )

    job_event_id = audit.emit("job.completed", "succeeded")

    assert len(producer.messages) == 3
    llm_event = producer.messages[0][2]
    tool_event = producer.messages[1][2]
    job_event = producer.messages[2][2]
    assert llm_event.event_id == llm_event_id
    assert tool_event.event_id == tool_event_id
    assert tool_event.trace_id == "trace-123"
    assert tool_event.parent_event_id == llm_event_id
    assert tool_event.status == "failed"
    assert tool_event.duration_ms == 12.5
    assert tool_event.error == "Fehler"
    assert producer.messages[1][0] == "agent.events"
    assert producer.messages[1][1] == job.job_id
    assert job_event.event_id == job_event_id
    assert job_event.parent_event_id == tool_event_id


def test_audit_failure_does_not_raise() -> None:
    settings = replace(Settings.from_env(), agent_name="test-agent")
    audit = AuditLogger(settings, FailingProducer()).for_job(
        Job(instruction="Hallo", trace_id="trace-123")
    )

    assert audit.emit("job.received") is None
    assert audit.last_event_id is None
