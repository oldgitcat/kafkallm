import pytest

from kafkallm.core.settings import Settings, SettingsError


def test_invalid_integer_setting_names_the_variable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GUI_PORT", "not-a-port")

    with pytest.raises(SettingsError, match="GUI_PORT must be an integer"):
        Settings.from_env()


def test_invalid_boolean_setting_names_the_variable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ALLOW_UNLIMITED_JOBS", "sometimes")

    with pytest.raises(SettingsError, match="ALLOW_UNLIMITED_JOBS must be a boolean"):
        Settings.from_env()


def test_boolean_settings_accept_common_spellings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ALLOW_UNLIMITED_JOBS", "YES")
    assert Settings.from_env().allow_unlimited_jobs is True

    monkeypatch.setenv("ALLOW_UNLIMITED_JOBS", "off")
    assert Settings.from_env().allow_unlimited_jobs is False


def test_empty_values_fall_back_to_the_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GUI_PORT", "")
    monkeypatch.setenv("ALLOW_UNLIMITED_JOBS", "")

    settings = Settings.from_env()

    assert settings.gui_port == 8090
    assert settings.allow_unlimited_jobs is False


def test_out_of_range_limits_are_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MAX_RESULT_BYTES", "0")

    with pytest.raises(SettingsError, match="MAX_RESULT_BYTES"):
        Settings.from_env()


def test_gui_port_must_be_a_valid_tcp_port(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GUI_PORT", "70000")

    with pytest.raises(SettingsError, match="GUI_PORT"):
        Settings.from_env()


def test_job_directories_derive_from_the_job_root(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    monkeypatch.setenv("KAFKALLM_JOB_ROOT", str(tmp_path))
    for name in (
        "WORKSPACE_ROOT",
        "GOOSE_JOB_WORKSPACE_ROOT",
        "GOOSE_JOB_SKILLS_DIR",
        "GOOSE_JOB_AGENTS_DIR",
        "GOOSE_JOB_TOOLS_DIR",
        "GOOSE_JOB_CONFIG_DIR",
    ):
        monkeypatch.delenv(name, raising=False)

    settings = Settings.from_env()

    assert settings.workspace_root == str(tmp_path / "staging")
    assert settings.goose_job_workspace_root == str(tmp_path / "jobs")
    assert settings.goose_job_skills_dir == str(tmp_path / "skills")
    assert settings.goose_job_agents_dir == str(tmp_path / "agents")
    assert settings.goose_job_tools_dir == str(tmp_path / "tools")
    assert settings.goose_job_config_dir == str(tmp_path / "config")


def test_podman_socket_becomes_a_unix_docker_host(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PODMAN_SOCKET", "/run/user/1234/podman/podman.sock")
    monkeypatch.delenv("GOOSE_JOB_DOCKER_HOST", raising=False)

    settings = Settings.from_env()

    assert settings.goose_job_docker_host == (
        "unix:///run/user/1234/podman/podman.sock"
    )
