from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from kafkallm.kafka import KafkaProducer
from kafkallm.models import AgentEvent, Job
from kafkallm.settings import Settings
from kafkallm.telemetry import inject_trace_headers, new_trace_id


logger = logging.getLogger(__name__)


class AuditLogger:
    def __init__(self, settings: Settings, producer: KafkaProducer) -> None:
        self.settings = settings
        self.producer = producer

    def for_job(self, job: Job) -> "JobAudit":
        return JobAudit(
            producer=self.producer,
            topic=self.settings.events_topic,
            agent=self.settings.agent_name,
            job_id=job.job_id,
            run_id=job.run_id,
            trace_id=job.trace_id or new_trace_id(),
        )


@dataclass(slots=True)
class JobAudit:
    producer: KafkaProducer
    topic: str
    agent: str
    job_id: str
    run_id: str
    trace_id: str
    last_event_id: str | None = None

    def emit(
        self,
        event_type: str,
        status: str = "info",
        *,
        parent_event_id: str | None = None,
        duration_ms: float | None = None,
        attributes: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> str | None:
        parent = parent_event_id or self.last_event_id
        event = AgentEvent(
            trace_id=self.trace_id,
            job_id=self.job_id,
            run_id=self.run_id,
            parent_event_id=parent,
            type=event_type,
            agent=self.agent,
            status=status,
            duration_ms=duration_ms,
            attributes=attributes or {},
            error=error[:4000] if error else None,
        )
        try:
            self.producer.send_async(
                topic=self.topic,
                key=self.job_id,
                model=event,
                headers=inject_trace_headers(),
            )
        except Exception:
            logger.exception(
                "Audit-Event %s für Job %s konnte nicht eingereiht werden",
                event.type,
                self.job_id,
            )
            return None
        self.last_event_id = event.event_id
        return event.event_id
