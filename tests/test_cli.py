from __future__ import annotations

import io
import json
import sys
from pathlib import Path

import pytest

from kafkallm.cli import _job_payload, build_parser, print_schema


def _args(*argv: str):
    return build_parser().parse_args(["submit", *argv])


def _write_job(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_job_json_payload_is_loaded_from_a_file(tmp_path: Path) -> None:
    payload = {
        "instruction": "Analyze this",
        "sandbox": "reverse",
        "multi_agent": False,
    }
    path = _write_job(tmp_path / "job.json", payload)

    assert _job_payload(_args("--job-json", str(path))) == payload


def test_job_json_payload_is_loaded_from_stdin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = {"instruction": "Say hello"}
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(payload)))

    assert _job_payload(_args("--job-json", "-")) == payload


def test_job_json_rejects_an_instruction(tmp_path: Path) -> None:
    path = _write_job(tmp_path / "job.json", {"instruction": "Build it"})

    with pytest.raises(ValueError, match="either an instruction or --job-json"):
        _job_payload(_args("Build it", "--job-json", str(path)))


def test_job_json_rejects_extra_job_options(tmp_path: Path) -> None:
    path = _write_job(tmp_path / "job.json", {"instruction": "Build it"})

    with pytest.raises(ValueError, match="do not combine"):
        _job_payload(_args("--job-json", str(path), "--sandbox", "reverse"))


def test_job_json_rejects_a_non_object(tmp_path: Path) -> None:
    path = tmp_path / "job.json"
    path.write_text("[]", encoding="utf-8")

    with pytest.raises(ValueError, match="JSON object"):
        _job_payload(_args("--job-json", str(path)))


def test_submit_requires_an_instruction_or_job_json() -> None:
    with pytest.raises(ValueError, match="Provide an instruction or --job-json"):
        _job_payload(_args())


def test_instruction_payload_keeps_the_existing_cli_shape() -> None:
    payload = _job_payload(
        _args(
            "Do the job",
            "--input",
            '{"key": "value"}',
            "--sandbox",
            "reverse",
            "--timeout",
            "120",
            "--max-steps",
            "4",
            "--job-id",
            "job-1",
        )
    )

    assert payload == {
        "instruction": "Do the job",
        "input": {"key": "value"},
        "input_files": [],
        "sandbox": "reverse",
        "timeout_seconds": 120,
        "max_steps": 4,
        "job_id": "job-1",
    }


def test_schema_parser_defaults_to_the_job_model() -> None:
    args = build_parser().parse_args(["schema"])

    assert args.command == "schema"
    assert args.model == "job"


def test_print_schema_emits_json_schema(
    capsys: pytest.CaptureFixture[str],
) -> None:
    print_schema("job")
    schema = json.loads(capsys.readouterr().out)

    assert {"instruction", "sandbox", "agents"} <= set(schema["properties"])
