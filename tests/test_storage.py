from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from kafkallm.core.storage import S3Storage, _safe_filename


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: list[tuple[str, str, bytes]] = []

    def put_object(self, Bucket: str, Key: str, Body: bytes) -> None:
        self.objects.append((Bucket, Key, Body))

    def upload_file(self, Filename: str, Bucket: str, Key: str) -> None:
        self.objects.append((Bucket, Key, Path(Filename).read_bytes()))


def test_safe_filename_removes_paths_and_control_characters() -> None:
    assert _safe_filename("../../evil\x00name.bin") == "evil_name.bin"
    assert _safe_filename("") == "upload.bin"
    assert _safe_filename("..") == "upload.bin"


def test_upload_input_stores_binary_data() -> None:
    storage = S3Storage.__new__(S3Storage)
    storage.bucket = "jobs"
    storage.client = FakeS3Client()
    payload = b"\x00\xffbinary"

    reference = storage.upload_input("../../tool.bin", payload)

    assert reference.bucket == "jobs"
    assert reference.key.startswith("uploads/")
    assert reference.key.endswith("/tool.bin")
    client: Any = storage.client
    assert client.objects == [("jobs", reference.key, payload)]


def test_upload_outputs_skips_symlinked_and_special_entries(tmp_path: Path) -> None:
    storage = S3Storage.__new__(S3Storage)
    storage.bucket = "jobs"
    storage.client = FakeS3Client()

    outputs = tmp_path / "outputs"
    outputs.mkdir()
    (outputs / "result.txt").write_text("artifact", encoding="utf-8")
    nested = outputs / "nested"
    nested.mkdir()
    (nested / "deep.bin").write_bytes(b"deep")
    secret = tmp_path / "host-secret"
    secret.write_text("host secret", encoding="utf-8")
    (outputs / "leak.txt").symlink_to(secret)
    (outputs / "leakdir").symlink_to(tmp_path)

    references = storage.upload_outputs(
        outputs,
        "job-1",
        "run-1",
        deadline=time.monotonic() + 30,
        max_files=10,
    )

    assert [reference.key for reference in references] == [
        "job-1/run-1/nested/deep.bin",
        "job-1/run-1/result.txt",
    ]
    client: Any = storage.client
    assert [body for _, _, body in client.objects] == [b"deep", b"artifact"]
