from __future__ import annotations

import io
import os
import tarfile
from pathlib import Path
from typing import Any

from kafkallm.sandbox.archive import (
    copy_from_container,
    copy_into_container,
    read_container_file,
)


class ArchiveContainer:
    def __init__(self, archive: bytes | None = None) -> None:
        self.archive = archive or b""
        self.puts: list[tuple[str, bytes]] = []

    def put_archive(self, path: str, data: Any) -> bool:
        self.puts.append((path, data.read()))
        return True

    def get_archive(self, path: str) -> tuple[Any, dict[str, Any]]:
        del path
        return iter([self.archive]), {}


def _archive(entries: list[tuple[str, bytes | None, bytes | None]]) -> bytes:
    """Build an archive from (name, file data, symlink target) entries."""
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as archive:
        for name, data, link in entries:
            info = tarfile.TarInfo(name)
            if link is not None:
                info.type = tarfile.SYMTYPE
                info.linkname = link.decode()
                archive.addfile(info)
            elif data is not None:
                info.size = len(data)
                archive.addfile(info, io.BytesIO(data))
            else:
                info.type = tarfile.DIRTYPE
                archive.addfile(info)
    return buffer.getvalue()


def test_copy_from_container_rejects_symlinks_and_traversal(tmp_path: Path) -> None:
    secret = tmp_path / "host-secret.txt"
    secret.write_text("host secret", encoding="utf-8")
    container = ArchiveContainer(
        _archive(
            [
                ("workspace", None, None),
                ("workspace/result.txt", b"artifact", None),
                ("workspace/link.txt", None, os.fsencode(secret)),
                ("workspace/../../escape.txt", b"escaped", None),
                ("workspace/sub", None, None),
                ("workspace/sub/../../escape2.txt", b"escaped", None),
            ]
        )
    )
    target = tmp_path / "target"

    copy_from_container(container, "/workspace", target)

    assert (target / "result.txt").read_text() == "artifact"
    assert not (target / "link.txt").exists()
    assert not (target / "link.txt").is_symlink()
    assert not (tmp_path / "escape.txt").exists()
    assert not (tmp_path / "escape2.txt").exists()


def test_copy_into_container_skips_symlinks_and_excluded_entries(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "input.txt").write_text("input", encoding="utf-8")
    (source / "bundle").mkdir()
    (source / "bundle" / "run.sh").write_text("#!/bin/sh\n", encoding="utf-8")
    secret = tmp_path / "secret.txt"
    secret.write_text("secret", encoding="utf-8")
    (source / "leak.txt").symlink_to(secret)
    container = ArchiveContainer()

    assert copy_into_container(
        container,
        "/workspace",
        source,
        exclude={"bundle"},
    )

    target, payload = container.puts[0]
    assert target == "/workspace"
    with tarfile.open(fileobj=io.BytesIO(payload), mode="r:*") as archive:
        names = {member.name for member in archive}
        assert "input.txt" in names
        assert "leak.txt" not in names
        assert not any(name.startswith("bundle/") for name in names)


def test_read_container_file_honours_the_limit() -> None:
    container = ArchiveContainer(_archive([("main.jsonl", b"x" * 100, None)]))

    assert (
        read_container_file(
            container,
            "/workspace/.kafkallm/main.jsonl",
            max_bytes=10,
        )
        == b"x" * 10
    )


def test_copy_from_container_rejects_special_files(tmp_path: Path) -> None:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as archive:
        info = tarfile.TarInfo("workspace/device")
        info.type = tarfile.CHRTYPE
        archive.addfile(info)
    container = ArchiveContainer(buffer.getvalue())

    copy_from_container(container, "/workspace", tmp_path / "target")

    assert not (tmp_path / "target" / "device").exists()
