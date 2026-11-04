from __future__ import annotations

from typing import Any

from kafkallm.storage import S3Storage, _safe_filename


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: list[tuple[str, str, bytes]] = []

    def put_object(self, Bucket: str, Key: str, Body: bytes) -> None:
        self.objects.append((Bucket, Key, Body))


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
