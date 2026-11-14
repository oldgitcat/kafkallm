"""Copy job trees into and out of an isolated microVM container.

The job microVM must not see the host filesystem. Everything the job needs is
copied into the container's writable layer before it starts, and every result
is read back through the Podman archive API afterwards. No bind mounts are
created, so a compromised guest can only modify its own disposable root
filesystem.

Archives from the guest are untrusted. Extraction never follows symlinks,
rejects special files, path traversal, excessive depth, and oversized trees.
"""

from __future__ import annotations

import logging
import os
import stat
import tarfile
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from kafkallm.sandbox.errors import GooseJobRuntimeError

logger = logging.getLogger(__name__)

_MAX_TREE_BYTES = 4 * 1024**3
_MAX_TREE_FILES = 500_000
_MAX_ARCHIVE_BYTES = 2 * 1024**3
_MAX_ARCHIVE_FILES = 200_000
_MAX_DEPTH = 64
_CHUNK_SIZE = 1024 * 1024


def _iter_tree(
    root: Path,
    *,
    exclude: set[str] | None = None,
) -> Iterator[tuple[Path, Path, bool]]:
    """Yield ``(path, relative, is_dir)`` without following symlinks."""
    root = Path(root)
    if root.is_symlink() or not root.is_dir():
        return
    stack: list[tuple[Path, Path, int]] = [(root, Path(), 0)]
    while stack:
        directory, relative_dir, depth = stack.pop()
        if depth > _MAX_DEPTH:
            continue
        try:
            entries = sorted(os.scandir(directory), key=lambda entry: entry.name)
        except OSError:
            continue
        for entry in entries:
            name = entry.name
            if relative_dir == Path() and exclude and name in exclude:
                continue
            relative = relative_dir / name
            path = Path(entry.path)
            try:
                if entry.is_symlink():
                    continue
                if entry.is_dir(follow_symlinks=False):
                    yield path, relative, True
                    stack.append((path, relative, depth + 1))
                elif entry.is_file(follow_symlinks=False):
                    yield path, relative, False
            except OSError:
                continue


def _add_tree(
    archive: tarfile.TarFile,
    root: Path,
    *,
    prefix: str = "",
    exclude: set[str] | None = None,
) -> None:
    total_bytes = 0
    total_files = 0
    if prefix:
        info = tarfile.TarInfo(prefix)
        info.type = tarfile.DIRTYPE
        info.mode = 0o700
        archive.addfile(info)
    for path, relative, is_dir in _iter_tree(root, exclude=exclude):
        name = f"{prefix}/{relative.as_posix()}" if prefix else relative.as_posix()
        try:
            mode = stat.S_IMODE(path.stat(follow_symlinks=False).st_mode)
        except OSError:
            continue
        info = tarfile.TarInfo(name)
        info.mode = (mode & 0o777) | (0o700 if is_dir else 0o600)
        info.uid = 0
        info.gid = 0
        info.uname = ""
        info.gname = ""
        if is_dir:
            info.type = tarfile.DIRTYPE
            archive.addfile(info)
            continue
        try:
            handle = path.open("rb")
        except OSError:
            continue
        with handle:
            try:
                stat_result = os.fstat(handle.fileno())
            except OSError:
                continue
            if not stat.S_ISREG(stat_result.st_mode):
                continue
            info.size = stat_result.st_size
            total_bytes += info.size
            total_files += 1
            if total_bytes > _MAX_TREE_BYTES or total_files > _MAX_TREE_FILES:
                raise GooseJobRuntimeError(
                    "The sandbox input tree is too large to copy."
                )
            archive.addfile(info, handle)


def copy_into_container(
    container: Any,
    target: str,
    source: Path | str,
    *,
    prefix: str = "",
    exclude: set[str] | None = None,
) -> bool:
    """Copy *source* into *target* inside the container.

    No bind mount is created. ``prefix`` places the tree below a directory in
    the archive, which is useful for targets such as ``/``.
    """
    source = Path(source)
    if source.is_symlink() or not source.is_dir():
        return False
    with tempfile.SpooledTemporaryFile(max_size=16 * 1024 * 1024) as archive_file:
        with tarfile.open(fileobj=archive_file, mode="w") as archive:
            _add_tree(archive, source, prefix=prefix, exclude=exclude)
        archive_file.seek(0)
        if not container.put_archive(target, archive_file):
            raise GooseJobRuntimeError(
                f"Could not copy sandbox data into the container at {target}."
            )
    return True


class _ChunkReader:
    """Minimal read-only file object over an iterable of byte chunks."""

    def __init__(self, chunks: Any) -> None:
        self._chunks = iter(chunks)
        self._buffer = b""

    def read(self, size: int = -1) -> bytes:
        if size is None or size < 0:
            data = self._buffer + b"".join(self._chunks)
            self._buffer = b""
            return data
        while len(self._buffer) < size:
            try:
                self._buffer += next(self._chunks)
            except StopIteration:
                break
        data, self._buffer = self._buffer[:size], self._buffer[size:]
        return data


def _safe_relative(member: tarfile.TarInfo, top: str) -> Path | None:
    parts = Path(member.name).parts
    if not parts or Path(member.name).is_absolute():
        return None
    if parts and parts[0] == top:
        parts = parts[1:]
    relative = Path(*parts) if parts else Path()
    if any(part in {"", ".", ".."} for part in relative.parts):
        return None
    if len(relative.parts) > _MAX_DEPTH:
        return None
    return relative


def _write_member(
    archive: tarfile.TarFile,
    member: tarfile.TarInfo,
    destination: Path,
    *,
    remaining: int,
) -> int:
    if member.size > remaining:
        raise GooseJobRuntimeError("The sandbox output tree is too large to copy.")
    source = archive.extractfile(member)
    if source is None:
        return 0
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(
        destination,
        os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    written = 0
    try:
        with os.fdopen(fd, "wb") as handle:
            while True:
                chunk = source.read(_CHUNK_SIZE)
                if not chunk:
                    break
                written += len(chunk)
                if written > remaining:
                    raise GooseJobRuntimeError(
                        "The sandbox output tree is too large to copy."
                    )
                handle.write(chunk)
    finally:
        source.close()
    return written


def copy_from_container(
    container: Any,
    source: str,
    target: Path | str,
    *,
    include: set[str] | None = None,
) -> None:
    """Copy *source* from the container into *target*, safely.

    Only regular files and directories are accepted. ``include`` limits the
    copy to the named top-level entries inside *source*.
    """
    target = Path(target)
    target.mkdir(parents=True, exist_ok=True)
    stream, _ = container.get_archive(source)
    top = Path(source).name
    total_bytes = 0
    total_files = 0
    try:
        with tarfile.open(fileobj=_ChunkReader(stream), mode="r|*") as archive:
            for member in archive:
                if member.isdir():
                    relative = _safe_relative(member, top)
                    if relative is None or not relative.parts:
                        continue
                    if include is not None and relative.parts[0] not in include:
                        continue
                    (target / relative).mkdir(parents=True, exist_ok=True)
                    continue
                if not member.isfile():
                    continue
                relative = _safe_relative(member, top)
                if relative is None or not relative.parts:
                    continue
                if include is not None and relative.parts[0] not in include:
                    continue
                total_files += 1
                if total_files > _MAX_ARCHIVE_FILES:
                    raise GooseJobRuntimeError(
                        "The sandbox output tree contains too many files."
                    )
                total_bytes += _write_member(
                    archive,
                    member,
                    target / relative,
                    remaining=_MAX_ARCHIVE_BYTES - total_bytes,
                )
    except tarfile.TarError as exc:
        raise GooseJobRuntimeError(
            f"Could not read sandbox data from {source}: {exc}"
        ) from exc


def read_container_file(
    container: Any,
    path: str,
    *,
    max_bytes: int,
) -> bytes | None:
    """Read one regular file from the container, capped at *max_bytes*.

    Returns ``None`` when the file does not exist yet.
    """
    try:
        stream, _ = container.get_archive(path)
    except Exception as exc:
        if getattr(exc, "status_code", None) == 404 or exc.__class__.__name__ in {
            "NotFound",
            "FileNotFoundError",
        }:
            return None
        raise
    try:
        with tarfile.open(fileobj=_ChunkReader(stream), mode="r|*") as archive:
            for member in archive:
                if not member.isfile():
                    continue
                source = archive.extractfile(member)
                if source is None:
                    return None
                try:
                    data = source.read(max_bytes + 1)
                finally:
                    source.close()
                return data[:max_bytes]
    except tarfile.TarError as exc:
        logger.debug("Could not read sandbox file %s: %s", path, exc)
        return None
    return None
