from __future__ import annotations

import os
from pathlib import Path

from kafkallm.dotenv import load_dotenv


def test_load_dotenv_reads_values_and_keeps_existing_environment(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setenv("KAFKALLM_DOTENV_EXISTING", "from-shell")
    monkeypatch.delenv("KAFKALLM_DOTENV_NEW", raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "# comment",
                "export KAFKALLM_DOTENV_NEW='from-file'",
                'KAFKALLM_DOTENV_QUOTED="two words"',
                "KAFKALLM_DOTENV_EXISTING=from-file",
                "not a valid line",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    assert load_dotenv(env_file)
    try:
        assert os.environ["KAFKALLM_DOTENV_NEW"] == "from-file"
        assert os.environ["KAFKALLM_DOTENV_QUOTED"] == "two words"
        assert os.environ["KAFKALLM_DOTENV_EXISTING"] == "from-shell"
    finally:
        os.environ.pop("KAFKALLM_DOTENV_NEW", None)
        os.environ.pop("KAFKALLM_DOTENV_QUOTED", None)


def test_load_dotenv_returns_false_for_a_missing_file(tmp_path: Path) -> None:
    assert not load_dotenv(tmp_path / "missing.env")
