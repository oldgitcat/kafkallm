from __future__ import annotations

import os
import socket
from dataclasses import dataclass


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value else default


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True, slots=True)
class Settings:
    kafka_bootstrap_servers: str
    jobs_topic: str
    results_topic: str
    events_topic: str
    kafka_group_id: str
    kafka_topic_partitions: int
    kafka_retention_ms: int
    kafka_startup_timeout_seconds: int
    worker_id: str
    workspace_root: str
    max_job_timeout_seconds: int
    max_agent_steps: int
    hermes_model: str
    hermes_provider: str
    hermes_startup_timeout_seconds: int
    hermes_image: str
    hermes_job_tmpfs_size: str
    hermes_job_max_turns: int
    hermes_job_memory_limit: str
    hermes_job_pids_limit: int
    hermes_job_cpus: int
    hermes_job_ram_mib: int
    hermes_job_use_passt: bool
    hermes_job_docker_host: str
    hermes_job_workspace_root: str
    hermes_job_skills_dir: str
    hermes_job_reflect_enabled: bool
    hermes_job_reflect_timeout_seconds: int
    s3_endpoint_url: str
    s3_region: str
    s3_bucket: str
    s3_access_key_id: str
    s3_secret_access_key: str
    agent_name: str
    phoenix_enabled: bool
    phoenix_collector_endpoint: str
    phoenix_project_name: str
    phoenix_capture_content: bool

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            kafka_bootstrap_servers=os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"
            ),
            jobs_topic=os.getenv("JOBS_TOPIC", "jobs.in"),
            results_topic=os.getenv("RESULTS_TOPIC", "jobs.out"),
            events_topic=os.getenv("EVENTS_TOPIC", "agent.events"),
            kafka_group_id=os.getenv("KAFKA_GROUP_ID", "agent-workers"),
            kafka_topic_partitions=_int_env("KAFKA_TOPIC_PARTITIONS", 3),
            kafka_retention_ms=_int_env(
                "KAFKA_TOPIC_RETENTION_MS", 7 * 24 * 60 * 60 * 1000
            ),
            kafka_startup_timeout_seconds=_int_env(
                "KAFKA_STARTUP_TIMEOUT_SECONDS", 300
            ),
            worker_id=os.getenv("WORKER_ID", socket.gethostname()),
            workspace_root=os.getenv("WORKSPACE_ROOT", "/tmp/hermes-workspace"),
            max_job_timeout_seconds=_int_env("MAX_JOB_TIMEOUT_SECONDS", 900),
            max_agent_steps=_int_env("MAX_AGENT_STEPS", 8),
            hermes_model=os.getenv("HERMES_MODEL", ""),
            hermes_provider=os.getenv("HERMES_PROVIDER", ""),
            hermes_startup_timeout_seconds=_int_env(
                "HERMES_STARTUP_TIMEOUT_SECONDS", 300
            ),
            hermes_image=os.getenv(
                "HERMES_IMAGE",
                "nousresearch/hermes-agent@sha256:"
                "9c5561579f3ffdf333401797094ac3a743738d6c25d40f4331c7bf77b694238d",
            ),
            hermes_job_tmpfs_size=os.getenv("HERMES_JOB_TMPFS_SIZE", "1g"),
            hermes_job_max_turns=_int_env("HERMES_JOB_MAX_TURNS", 100),
            hermes_job_memory_limit=os.getenv("HERMES_JOB_MEMORY_LIMIT", "4g"),
            hermes_job_pids_limit=_int_env("HERMES_JOB_PIDS_LIMIT", 512),
            hermes_job_cpus=_int_env("HERMES_JOB_CPUS", 2),
            hermes_job_ram_mib=_int_env("HERMES_JOB_RAM_MIB", 2048),
            hermes_job_use_passt=_bool_env("HERMES_JOB_USE_PASST", False),
            hermes_job_docker_host=os.getenv(
                "HERMES_JOB_DOCKER_HOST", "tcp://podman-proxy:2375"
            ),
            hermes_job_workspace_root=os.getenv(
                "HERMES_JOB_WORKSPACE_ROOT", "/var/lib/hermes/jobs"
            ),
            hermes_job_skills_dir=os.getenv(
                "HERMES_JOB_SKILLS_DIR", "/var/lib/hermes/skills"
            ),
            hermes_job_reflect_enabled=_bool_env(
                "HERMES_JOB_REFLECT_ENABLED", True
            ),
            hermes_job_reflect_timeout_seconds=_int_env(
                "HERMES_JOB_REFLECT_TIMEOUT_SECONDS", 300
            ),
            s3_endpoint_url=os.getenv("S3_ENDPOINT_URL", ""),
            s3_region=os.getenv("S3_REGION", "local"),
            s3_bucket=os.getenv("S3_BUCKET", "jobs"),
            s3_access_key_id=os.getenv("S3_ACCESS_KEY_ID", ""),
            s3_secret_access_key=os.getenv("S3_SECRET_ACCESS_KEY", ""),
            agent_name=os.getenv("AGENT_NAME", "agent-worker"),
            phoenix_enabled=_bool_env("PHOENIX_ENABLED", False),
            phoenix_collector_endpoint=os.getenv(
                "PHOENIX_COLLECTOR_ENDPOINT", "http://localhost:6006"
            ),
            phoenix_project_name=os.getenv("PHOENIX_PROJECT_NAME", "hermes"),
            phoenix_capture_content=_bool_env("PHOENIX_CAPTURE_CONTENT", False),
        )

    @property
    def storage_configured(self) -> bool:
        return bool(self.s3_endpoint_url)
