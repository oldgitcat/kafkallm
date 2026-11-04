from __future__ import annotations

import os
import socket
from dataclasses import dataclass, field


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
    state_topic: str
    events_topic: str
    kafka_group_id: str
    kafka_topic_partitions: int
    kafka_retention_ms: int
    kafka_startup_timeout_seconds: int
    worker_id: str
    workspace_root: str
    max_job_timeout_seconds: int
    max_agent_steps: int
    goose_model: str
    goose_provider: str
    goose_startup_timeout_seconds: int
    goose_image: str
    goose_job_tmpfs_size: str
    goose_job_max_turns: int
    goose_job_subagent_max_turns: int
    goose_job_memory_limit: str
    goose_job_pids_limit: int
    goose_job_cpus: int
    goose_job_ram_mib: int
    goose_job_use_passt: bool
    goose_job_docker_host: str
    goose_job_workspace_root: str
    goose_job_skills_dir: str
    goose_job_tools_dir: str
    goose_job_config_dir: str
    goose_job_reflect_enabled: bool
    goose_job_reflect_timeout_seconds: int
    llm_gateway_url: str
    llm_gateway_virtual_key: str = field(repr=False)
    llm_gateway_ready_url: str
    llm_gateway_port: int
    goose_job_network: str
    goose_job_network_subnet: str
    goose_job_network_interface: str
    goose_job_network_dir: str
    goose_job_network_autocreate: bool
    goose_job_proxy_network: str
    goose_job_proxy_upstream_host: str
    goose_job_proxy_upstream_port: int
    gui_host: str
    gui_port: int
    upload_max_files: int
    upload_max_file_bytes: int
    upload_max_total_bytes: int
    s3_endpoint_url: str
    s3_region: str
    s3_bucket: str
    s3_access_key_id: str
    s3_secret_access_key: str
    agent_name: str
    telemetry_capture_content: bool
    phoenix_enabled: bool
    phoenix_collector_endpoint: str
    phoenix_project_name: str
    mlflow_enabled: bool
    mlflow_tracking_uri: str
    mlflow_experiment_id: str

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            kafka_bootstrap_servers=os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"
            ),
            jobs_topic=os.getenv("JOBS_TOPIC", "jobs.in"),
            results_topic=os.getenv("RESULTS_TOPIC", "jobs.out"),
            state_topic=os.getenv("STATE_TOPIC", "jobs.state"),
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
            workspace_root=os.getenv("WORKSPACE_ROOT", "/var/lib/kafkallm/staging"),
            max_job_timeout_seconds=_int_env("MAX_JOB_TIMEOUT_SECONDS", 1800),
            max_agent_steps=_int_env("MAX_AGENT_STEPS", 8),
            goose_model=os.getenv("GOOSE_MODEL", ""),
            goose_provider=os.getenv("GOOSE_PROVIDER", ""),
            goose_startup_timeout_seconds=_int_env(
                "GOOSE_STARTUP_TIMEOUT_SECONDS", 300
            ),
            goose_image=os.getenv("GOOSE_IMAGE", "kafkallm-goose-job:latest"),
            goose_job_tmpfs_size=os.getenv("GOOSE_JOB_TMPFS_SIZE", "1g"),
            goose_job_max_turns=_int_env("GOOSE_JOB_MAX_TURNS", 60),
            goose_job_subagent_max_turns=_int_env("GOOSE_JOB_SUBAGENT_MAX_TURNS", 40),
            goose_job_memory_limit=os.getenv("GOOSE_JOB_MEMORY_LIMIT", "4g"),
            goose_job_pids_limit=_int_env("GOOSE_JOB_PIDS_LIMIT", 512),
            goose_job_cpus=_int_env("GOOSE_JOB_CPUS", 2),
            goose_job_ram_mib=_int_env("GOOSE_JOB_RAM_MIB", 2048),
            goose_job_use_passt=_bool_env("GOOSE_JOB_USE_PASST", False),
            goose_job_docker_host=os.getenv(
                "GOOSE_JOB_DOCKER_HOST", "tcp://podman-proxy:2375"
            ),
            goose_job_workspace_root=os.getenv(
                "GOOSE_JOB_WORKSPACE_ROOT", "/var/lib/kafkallm/jobs"
            ),
            goose_job_skills_dir=os.getenv(
                "GOOSE_JOB_SKILLS_DIR", "/var/lib/kafkallm/skills"
            ),
            goose_job_tools_dir=os.getenv(
                "GOOSE_JOB_TOOLS_DIR", "/var/lib/kafkallm/tools"
            ),
            goose_job_config_dir=os.getenv(
                "GOOSE_JOB_CONFIG_DIR", "/var/lib/kafkallm/config"
            ),
            goose_job_reflect_enabled=_bool_env("GOOSE_JOB_REFLECT_ENABLED", True),
            goose_job_reflect_timeout_seconds=_int_env(
                "GOOSE_JOB_REFLECT_TIMEOUT_SECONDS", 300
            ),
            llm_gateway_url=os.getenv(
                "LLM_GATEWAY_URL", "http://host.containers.internal:8091"
            ),
            llm_gateway_virtual_key=os.getenv("LLM_GATEWAY_VIRTUAL_KEY", ""),
            llm_gateway_ready_url=os.getenv(
                "LLM_GATEWAY_READY_URL",
                "http://llm-gateway:15021/healthz/ready",
            ),
            llm_gateway_port=_int_env("LLM_GATEWAY_PORT", 8091),
            goose_job_network=os.getenv("GOOSE_JOB_NETWORK", "kafkallm-jobs"),
            goose_job_network_subnet=os.getenv(
                "GOOSE_JOB_NETWORK_SUBNET", "fd00:6b61:666b:616c::/64"
            ),
            goose_job_network_interface=os.getenv("GOOSE_JOB_NETWORK_INTERFACE", ""),
            goose_job_network_dir=os.getenv("GOOSE_JOB_NETWORK_DIR", ""),
            goose_job_network_autocreate=_bool_env(
                "GOOSE_JOB_NETWORK_AUTOCREATE", False
            ),
            goose_job_proxy_network=os.getenv("GOOSE_JOB_PROXY_NETWORK", "podman"),
            goose_job_proxy_upstream_host=os.getenv(
                "GOOSE_JOB_PROXY_UPSTREAM_HOST", "host.containers.internal"
            ),
            goose_job_proxy_upstream_port=_int_env(
                "GOOSE_JOB_PROXY_UPSTREAM_PORT", 8091
            ),
            gui_host=os.getenv("GUI_HOST", "0.0.0.0"),
            gui_port=_int_env("GUI_PORT", 8090),
            upload_max_files=_int_env("UPLOAD_MAX_FILES", 16),
            upload_max_file_bytes=_int_env("UPLOAD_MAX_FILE_BYTES", 50 * 1024 * 1024),
            upload_max_total_bytes=_int_env(
                "UPLOAD_MAX_TOTAL_BYTES", 100 * 1024 * 1024
            ),
            s3_endpoint_url=os.getenv("S3_ENDPOINT_URL", ""),
            s3_region=os.getenv("S3_REGION", "local"),
            s3_bucket=os.getenv("S3_BUCKET", "jobs"),
            s3_access_key_id=os.getenv("S3_ACCESS_KEY_ID", ""),
            s3_secret_access_key=os.getenv("S3_SECRET_ACCESS_KEY", ""),
            agent_name=os.getenv("AGENT_NAME", "agent-worker"),
            telemetry_capture_content=_bool_env(
                "TELEMETRY_CAPTURE_CONTENT",
                _bool_env("PHOENIX_CAPTURE_CONTENT", False),
            ),
            phoenix_enabled=_bool_env("PHOENIX_ENABLED", False),
            phoenix_collector_endpoint=os.getenv(
                "PHOENIX_COLLECTOR_ENDPOINT", "http://localhost:6006"
            ),
            phoenix_project_name=os.getenv("PHOENIX_PROJECT_NAME", "kafkallm"),
            mlflow_enabled=_bool_env("MLFLOW_ENABLED", True),
            mlflow_tracking_uri=os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"),
            mlflow_experiment_id=os.getenv("MLFLOW_EXPERIMENT_ID", "0"),
        )

    @property
    def storage_configured(self) -> bool:
        return bool(self.s3_endpoint_url)

    @property
    def gateway_configured(self) -> bool:
        return bool(self.llm_gateway_url and self.llm_gateway_virtual_key)
