from __future__ import annotations

import os
from pathlib import Path

from kafkallm.safe_fs import (
    iter_regular_files,
    open_text_no_follow,
    read_text_no_follow,
)


def test_iter_regular_files_never_follows_symlinks(tmp_path: Path) -> None:
    root = tmp_path / "outputs"
    root.mkdir()
    (root / "result.txt").write_text("artifact", encoding="utf-8")
    nested = root / "nested"
    nested.mkdir()
    (nested / "deep.txt").write_text("deep", encoding="utf-8")
    secret = tmp_path / "host-secret"
    secret.write_text("host secret", encoding="utf-8")
    (root / "leak.txt").symlink_to(secret)
    (root / "leakdir").symlink_to(tmp_path)

    files = {relative.as_posix(): path for path, relative in iter_regular_files(root)}

    assert files == {
        "nested/deep.txt": nested / "deep.txt",
        "result.txt": root / "result.txt",
    }


def test_iter_regular_files_returns_nothing_for_a_symlinked_root(
    tmp_path: Path,
) -> None:
    real = tmp_path / "real"
    real.mkdir()
    (real / "file.txt").write_text("data", encoding="utf-8")
    link = tmp_path / "link"
    link.symlink_to(real)

    assert list(iter_regular_files(link)) == []


def test_read_text_no_follow_reads_regular_files(tmp_path: Path) -> None:
    path = tmp_path / "control"
    path.mkdir()
    (path / "main.jsonl").write_text("hello", encoding="utf-8")

    assert read_text_no_follow(path / "main.jsonl") == "hello"
    assert read_text_no_follow(path / "main.jsonl", max_bytes=2) == "he"


def test_read_text_no_follow_rejects_symlinks(tmp_path: Path) -> None:
    secret = tmp_path / "secret"
    secret.write_text("host secret", encoding="utf-8")

    control = tmp_path / "control"
    control.mkdir()
    (control / "main.jsonl").symlink_to(secret)

    assert read_text_no_follow(control / "main.jsonl") == ""

    linked_parent = tmp_path / "linked"
    linked_parent.symlink_to(tmp_path)

    assert read_text_no_follow(linked_parent / "secret") == ""


def test_read_text_no_follow_rejects_special_files_without_blocking(
    tmp_path: Path,
) -> None:
    control = tmp_path / "control"
    control.mkdir()
    fifo = control / "main.jsonl"
    os.mkfifo(fifo)

    assert read_text_no_follow(fifo) == ""


def test_open_text_no_follow_streams_regular_files(tmp_path: Path) -> None:
    path = tmp_path / "main.jsonl"
    path.write_text("one\ntwo\n", encoding="utf-8")
    secret = tmp_path / "secret"
    secret.write_text("host secret", encoding="utf-8")
    link = tmp_path / "link.jsonl"
    link.symlink_to(secret)

    stream = open_text_no_follow(path)
    assert stream is not None
    with stream:
        assert stream.readline() == "one\n"
        assert stream.readline() == "two\n"
    assert open_text_no_follow(link) is None
