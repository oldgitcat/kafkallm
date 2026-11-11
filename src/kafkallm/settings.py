from __future__ import annotations

import os
import socket
from dataclasses import dataclass, field


class SettingsError(ValueError):
    """An environment variable is missing or malformed."""


def _str_env(name: str, default: str) -> str:
    """Return a non-empty environment value, or the default.

    Compose passes known variables through even when they are unset, so an
    empty string must behave like a missing variable. Keeping the defaults in
    this module makes it the single source of truth for the control plane.
    """
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return value


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise SettingsError(f"{name} must be an integer, got {value!r}.") from exc


_TRUE_VALUES = {"1", "true", "yes", "on"}
_FALSE_VALUES = {"0", "false", "no", "off"}


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    normalized = value.strip().lower()
    if normalized in _TRUE_VALUES:
        return True
    if normalized in _FALSE_VALUES:
        return False
    raise SettingsError(
        f"{name} must be a boolean such as true or false, got {value!r}."
    )


@dataclass(frozen=True, slots=True)
class Settings:
    kafka_bootstrap_servers: str
    jobs_topic: str
    results_topic: str
    state_topic: str
    events_topic: str
    agents_topic: str
    sandboxes_topic: str
    cancels_topic: str
    logs_topic: str
    kafka_group_id: str
    kafka_topic_partitions: int
    kafka_retention_ms: int
    kafka_startup_timeout_seconds: int
    worker_id: str
    worker_concurrency: int
    workspace_root: str
    max_job_timeout_seconds: int
    allow_unlimited_jobs: bool
    max_result_bytes: int
    max_output_files: int
    unlimited_max_turns: int
    unlimited_max_subagent_turns: int
    unlimited_max_poll_interval_ms: int
    goose_model: str
    goose_provider: str
    goose_startup_timeout_seconds: int
    goose_image: str
    goose_re_image: str
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
    goose_job_agents_dir: str
    goose_job_default_agents_dir: str
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
    mlflow_enabled: bool
    mlflow_tracking_uri: str
    mlflow_experiment_id: str
    gui_max_request_bytes: int = 2 * 1024 * 1024

    def __post_init__(self) -> None:
        limits = {
            "KAFKA_TOPIC_PARTITIONS": (self.kafka_topic_partitions, 1),
            "KAFKA_STARTUP_TIMEOUT_SECONDS": (self.kafka_startup_timeout_seconds, 0),
            "WORKER_CONCURRENCY": (self.worker_concurrency, 1),
            "MAX_JOB_TIMEOUT_SECONDS": (self.max_job_timeout_seconds, 1),
            "MAX_RESULT_BYTES": (self.max_result_bytes, 1),
            "MAX_OUTPUT_FILES": (self.max_output_files, 0),
            "UPLOAD_MAX_FILES": (self.upload_max_files, 1),
            "UPLOAD_MAX_FILE_BYTES": (self.upload_max_file_bytes, 1),
            "UPLOAD_MAX_TOTAL_BYTES": (self.upload_max_total_bytes, 1),
            "GUI_MAX_REQUEST_BYTES": (self.gui_max_request_bytes, 1024),
        }
        for name, (value, minimum) in limits.items():
            if value < minimum:
                raise SettingsError(f"{name} must be at least {minimum}, got {value}.")
        for name, port in (
            ("GUI_PORT", self.gui_port),
            ("LLM_GATEWAY_PORT", self.llm_gateway_port),
        ):
            if not 0 <= port <= 65535:
                raise SettingsError(
                    f"{name} must be a TCP port between 0 and 65535, got {port}."
                )

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            kafka_bootstrap_servers=_str_env(
                "KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:19092"
            ),
            jobs_topic=_str_env("JOBS_TOPIC", "jobs.in"),
            results_topic=_str_env("RESULTS_TOPIC", "jobs.out"),
            state_topic=_str_env("STATE_TOPIC", "jobs.state"),
            events_topic=_str_env("EVENTS_TOPIC", "agent.events"),
            agents_topic=_str_env("AGENTS_TOPIC", "agents.state"),
            sandboxes_topic=_str_env("SANDBOXES_TOPIC", "sandboxes.state"),
            cancels_topic=_str_env("CANCELS_TOPIC", "jobs.cancel"),
            logs_topic=_str_env("LOGS_TOPIC", "jobs.logs"),
            kafka_group_id=_str_env("KAFKA_GROUP_ID", "agent-workers"),
            kafka_topic_partitions=_int_env("KAFKA_TOPIC_PARTITIONS", 3),
            kafka_retention_ms=_int_env(
                "KAFKA_TOPIC_RETENTION_MS", 7 * 24 * 60 * 60 * 1000
            ),
            kafka_startup_timeout_seconds=_int_env(
                "KAFKA_STARTUP_TIMEOUT_SECONDS", 300
            ),
            worker_id=_str_env("WORKER_ID", socket.gethostname()),
            worker_concurrency=max(1, _int_env("WORKER_CONCURRENCY", 1)),
            workspace_root=_str_env("WORKSPACE_ROOT", "/var/lib/kafkallm/staging"),
            max_job_timeout_seconds=_int_env("MAX_JOB_TIMEOUT_SECONDS", 1800),
            allow_unlimited_jobs=_bool_env("ALLOW_UNLIMITED_JOBS", False),
            max_result_bytes=_int_env("MAX_RESULT_BYTES", 500_000),
            max_output_files=_int_env("MAX_OUTPUT_FILES", 100),
            unlimited_max_turns=_int_env("UNLIMITED_MAX_TURNS", 100_000),
            unlimited_max_subagent_turns=_int_env(
                "UNLIMITED_MAX_SUBAGENT_TURNS", 100_000
            ),
            unlimited_max_poll_interval_ms=_int_env(
                "UNLIMITED_MAX_POLL_INTERVAL_MS", 86_400_000
            ),
            goose_model=_str_env("GOOSE_MODEL", ""),
            goose_provider=_str_env(
                "GOOSE_PROVIDER", _str_env("LLM_UPSTREAM_PROVIDER", "")
            ),
            goose_startup_timeout_seconds=_int_env(
                "GOOSE_STARTUP_TIMEOUT_SECONDS", 300
            ),
            goose_image=_str_env("GOOSE_IMAGE", "kafkallm-goose-job:latest"),
            goose_re_image=_str_env("GOOSE_RE_IMAGE", "kafkallm-rea-job:latest"),
            goose_job_tmpfs_size=_str_env("GOOSE_JOB_TMPFS_SIZE", "1g"),
            goose_job_max_turns=_int_env("GOOSE_JOB_MAX_TURNS", 60),
            goose_job_subagent_max_turns=_int_env("GOOSE_JOB_SUBAGENT_MAX_TURNS", 40),
            goose_job_memory_limit=_str_env("GOOSE_JOB_MEMORY_LIMIT", "4g"),
            goose_job_pids_limit=_int_env("GOOSE_JOB_PIDS_LIMIT", 512),
            goose_job_cpus=_int_env("GOOSE_JOB_CPUS", 2),
            goose_job_ram_mib=_int_env("GOOSE_JOB_RAM_MIB", 2048),
            goose_job_use_passt=_bool_env("GOOSE_JOB_USE_PASST", False),
            goose_job_docker_host=_str_env(
                "GOOSE_JOB_DOCKER_HOST", "tcp://podman-proxy:2375"
            ),
            goose_job_workspace_root=_str_env(
                "GOOSE_JOB_WORKSPACE_ROOT", "/var/lib/kafkallm/jobs"
            ),
            goose_job_skills_dir=_str_env(
                "GOOSE_JOB_SKILLS_DIR", "/var/lib/kafkallm/skills"
            ),
            goose_job_agents_dir=_str_env(
                "GOOSE_JOB_AGENTS_DIR", "/var/lib/kafkallm/agents"
            ),
            goose_job_default_agents_dir=_str_env(
                "GOOSE_JOB_DEFAULT_AGENTS_DIR", "agents"
            ),
            goose_job_tools_dir=_str_env(
                "GOOSE_JOB_TOOLS_DIR", "/var/lib/kafkallm/tools"
            ),
            goose_job_config_dir=_str_env(
                "GOOSE_JOB_CONFIG_DIR", "/var/lib/kafkallm/config"
            ),
            goose_job_reflect_enabled=_bool_env("GOOSE_JOB_REFLECT_ENABLED", True),
            goose_job_reflect_timeout_seconds=_int_env(
                "GOOSE_JOB_REFLECT_TIMEOUT_SECONDS", 300
            ),
            llm_gateway_url=_str_env(
                "LLM_GATEWAY_URL", "http://host.containers.internal:8091"
            ),
            llm_gateway_virtual_key=_str_env("LLM_GATEWAY_VIRTUAL_KEY", ""),
            llm_gateway_ready_url=_str_env(
                "LLM_GATEWAY_READY_URL",
                "http://agentgateway:15021/healthz/ready",
            ),
            llm_gateway_port=_int_env("LLM_GATEWAY_PORT", 8091),
            goose_job_network=_str_env("GOOSE_JOB_NETWORK", "kafkallm-jobs"),
            goose_job_network_subnet=_str_env(
                "GOOSE_JOB_NETWORK_SUBNET", "fd00:6b61:666b:616c::/64"
            ),
            goose_job_network_interface=_str_env("GOOSE_JOB_NETWORK_INTERFACE", ""),
            goose_job_network_dir=_str_env("GOOSE_JOB_NETWORK_DIR", ""),
            goose_job_network_autocreate=_bool_env(
                "GOOSE_JOB_NETWORK_AUTOCREATE", False
            ),
            goose_job_proxy_network=_str_env("GOOSE_JOB_PROXY_NETWORK", "podman"),
            goose_job_proxy_upstream_host=_str_env(
                "GOOSE_JOB_PROXY_UPSTREAM_HOST", "host.containers.internal"
            ),
            goose_job_proxy_upstream_port=_int_env(
                "GOOSE_JOB_PROXY_UPSTREAM_PORT", 8091
            ),
            gui_host=_str_env("GUI_HOST", "0.0.0.0"),
            gui_port=_int_env("GUI_PORT", 8090),
            upload_max_files=_int_env("UPLOAD_MAX_FILES", 16),
            upload_max_file_bytes=_int_env("UPLOAD_MAX_FILE_BYTES", 50 * 1024 * 1024),
            upload_max_total_bytes=_int_env(
                "UPLOAD_MAX_TOTAL_BYTES", 100 * 1024 * 1024
            ),
            s3_endpoint_url=_str_env("S3_ENDPOINT_URL", ""),
            s3_region=_str_env("S3_REGION", "local"),
            s3_bucket=_str_env("S3_BUCKET", "jobs"),
            s3_access_key_id=_str_env("S3_ACCESS_KEY_ID", "rustfs"),
            s3_secret_access_key=_str_env("S3_SECRET_ACCESS_KEY", "kafkallm-secret"),
            agent_name=_str_env("AGENT_NAME", "agent-worker"),
            telemetry_capture_content=_bool_env("TELEMETRY_CAPTURE_CONTENT", False),
            mlflow_enabled=_bool_env("MLFLOW_ENABLED", True),
            mlflow_tracking_uri=_str_env("MLFLOW_TRACKING_URI", "http://mlflow:5000"),
            mlflow_experiment_id=_str_env("MLFLOW_EXPERIMENT_ID", "0"),
            gui_max_request_bytes=_int_env("GUI_MAX_REQUEST_BYTES", 2 * 1024 * 1024),
        )

    @property
    def storage_configured(self) -> bool:
        return bool(self.s3_endpoint_url)

    @property
    def gateway_configured(self) -> bool:
        return bool(self.llm_gateway_url and self.llm_gateway_virtual_key)
