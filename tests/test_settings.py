import pytest

from kafkallm.settings import Settings, SettingsError


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
