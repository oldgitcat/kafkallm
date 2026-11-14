"""Filesystem helpers that never follow untrusted symlinks.

Job output trees come back from an isolated microVM, where a job can create
symlinks whose targets would resolve on the *host* when the worker reads or
copies files back. These helpers use ``O_NOFOLLOW`` and ``follow_symlinks=False``
so untrusted entries can never make the worker read or copy a host file.
"""

from __future__ import annotations

import os
import stat
from collections.abc import Iterator
from pathlib import Path
from typing import TextIO

# A malicious job could create a very deep directory tree. Bounding the depth
# keeps the worker's walk finite even if the guest never terminates.
_MAX_DEPTH = 64

# The worker only ever reads small control files from a job workspace. Capping
# the read keeps a hostile file from exhausting worker memory.
_MAX_TEXT_BYTES = 16 * 1024 * 1024


def iter_regular_files(root: Path) -> Iterator[tuple[Path, Path]]:
    """Yield ``(absolute, relative)`` for regular files below ``root``.

    Symlinks (to files or directories), special files, unreadable directories,
    and paths deeper than :data:`_MAX_DEPTH` are skipped. The walk never follows
    a symlink, so a job cannot point the worker at a host path.
    """
    root = Path(root)
    if root.is_symlink() or not root.is_dir():
        return
    stack: list[tuple[Path, int]] = [(root, 0)]
    while stack:
        directory, depth = stack.pop()
        if depth > _MAX_DEPTH:
            continue
        try:
            entries = sorted(os.scandir(directory), key=lambda entry: entry.name)
        except OSError:
            continue
        for entry in entries:
            path = Path(entry.path)
            try:
                if entry.is_symlink():
                    continue
                if entry.is_dir(follow_symlinks=False):
                    stack.append((path, depth + 1))
                elif entry.is_file(follow_symlinks=False):
                    yield path, path.relative_to(root)
            except OSError:
                continue


def _open_no_follow(path: Path, flags: int = os.O_RDONLY) -> int | None:
    """Open ``path`` without following a symlink in its parent or final name."""
    path = Path(path)
    try:
        parent_fd = os.open(
            path.parent,
            os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0),
        )
    except OSError:
        return None
    try:
        try:
            # O_PATH opens the directory entry without triggering device or
            # FIFO semantics; the fstat below then rejects non-regular files.
            path_fd = os.open(
                path.name,
                getattr(os, "O_PATH", flags) | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=parent_fd,
            )
        except OSError:
            return None
    finally:
        os.close(parent_fd)
    try:
        if not stat.S_ISREG(os.fstat(path_fd).st_mode):
            os.close(path_fd)
            return None
        # /proc/self/fd is a magic link to the inode we already verified; the
        # O_NONBLOCK opener can never wait on a FIFO or device node.
        fd = os.open(
            f"/proc/self/fd/{path_fd}",
            flags | getattr(os, "O_NONBLOCK", 0),
        )
    except OSError:
        os.close(path_fd)
        return None
    else:
        os.close(path_fd)
    return fd


def read_text_no_follow(path: Path, max_bytes: int = _MAX_TEXT_BYTES) -> str:
    """Read a UTF-8 text file without following symlinks in the path.

    Both the parent directory and the file itself are opened with
    ``O_NOFOLLOW``. A job can replace either with a symlink to a host path; a
    plain :meth:`Path.read_text` would follow it and disclose that file to the
    worker, so control files from a job must always go through this helper.
    """
    fd = _open_no_follow(path)
    if fd is None:
        return ""
    try:
        with os.fdopen(fd, "r", encoding="utf-8", errors="replace") as stream:
            return stream.read(max_bytes)
    except OSError:
        return ""


def open_text_no_follow(path: Path) -> TextIO | None:
    """Open a regular UTF-8 text file for streaming without following symlinks."""
    fd = _open_no_follow(path)
    if fd is None:
        return None
    try:
        return os.fdopen(fd, "r", encoding="utf-8", errors="replace")
    except OSError:
        os.close(fd)
        return None
