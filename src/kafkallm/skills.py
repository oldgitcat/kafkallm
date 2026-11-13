"""Validated handling of the persistent goosetown skill store.

The persistent store is never copied writable into a job. Every job gets a
disposable copy, and a separate learning-pass microVM may propose changes. The
worker validates and merges those changes; this module owns both operations.

Validation is structural, not semantic: a skill must be a directory with a
regular, UTF-8 ``SKILL.md`` and only regular files below it. Symlinks, special
files, oversized trees, and invalid names are rejected. Existing files are never
deleted, so a job cannot remove a defensive skill from the store.
"""

from __future__ import annotations

import os
import re
import stat
import tempfile
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

_SKILL_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")

_MAX_DEPTH = 16
_MAX_DIRECTORIES = 1000
_MAX_FILES_PER_SKILL = 200
_MAX_FILE_BYTES = 256 * 1024
_MAX_SKILL_BYTES = 2 * 1024 * 1024
_MAX_COPY_SKILLS = 64
_MAX_COPY_BYTES = 16 * 1024 * 1024
_MAX_PERSISTED_SKILLS = 3
_MAX_MERGE_BYTES = 2 * 1024 * 1024
_MAX_SCANNED_SKILLS = 256

_SKILL_ENTRY = "SKILL.md"


class SkillValidationError(ValueError):
    """A skill tree is not safe to copy or persist."""


@dataclass(slots=True)
class CopyResult:
    copied: list[str] = field(default_factory=list)
    rejected: list[tuple[str, str]] = field(default_factory=list)
    files: int = 0
    bytes: int = 0


@dataclass(slots=True)
class MergeResult:
    persisted: list[str] = field(default_factory=list)
    rejected: list[tuple[str, str]] = field(default_factory=list)
    files: int = 0
    bytes: int = 0

    @property
    def changed(self) -> bool:
        return bool(self.persisted)


def _skill_entries(directory: Path) -> Iterator[tuple[Path, Path, int]]:
    """Yield ``(absolute, relative, size)`` for a validated skill tree."""
    if directory.is_symlink() or not directory.is_dir():
        raise SkillValidationError("skill directory is missing or a symlink")
    skill_file = directory / _SKILL_ENTRY
    if skill_file.is_symlink() or not skill_file.is_file():
        raise SkillValidationError(f"missing regular {_SKILL_ENTRY}")
    skill_bytes = _read_regular_file(skill_file)
    if not skill_bytes.strip():
        raise SkillValidationError(f"{_SKILL_ENTRY} is empty")
    try:
        skill_text = skill_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise SkillValidationError(f"{_SKILL_ENTRY} is not UTF-8") from exc
    if "\x00" in skill_text:
        raise SkillValidationError(f"{_SKILL_ENTRY} contains a NUL byte")

    total = 0
    count = 0
    directories = 0
    stack: list[tuple[Path, int]] = [(directory, 0)]
    while stack:
        current, depth = stack.pop()
        if depth > _MAX_DEPTH:
            raise SkillValidationError("skill tree is too deep")
        try:
            entries = sorted(os.scandir(current), key=lambda entry: entry.name)
        except OSError as exc:
            raise SkillValidationError(f"cannot read skill directory: {exc}") from exc
        for entry in entries:
            path = Path(entry.path)
            relative = path.relative_to(directory)
            try:
                if entry.is_symlink():
                    raise SkillValidationError(
                        f"symlink in skill tree: {relative.as_posix()}"
                    )
                if entry.is_dir(follow_symlinks=False):
                    directories += 1
                    if directories > _MAX_DIRECTORIES:
                        raise SkillValidationError(
                            f"skill has more than {_MAX_DIRECTORIES} directories"
                        )
                    stack.append((path, depth + 1))
                    continue
                if not entry.is_file(follow_symlinks=False):
                    raise SkillValidationError(
                        f"special file in skill tree: {relative.as_posix()}"
                    )
                size = entry.stat(follow_symlinks=False).st_size
            except OSError as exc:
                raise SkillValidationError(
                    f"cannot inspect {relative.as_posix()}: {exc}"
                ) from exc
            if size > _MAX_FILE_BYTES:
                raise SkillValidationError(
                    f"{relative.as_posix()} exceeds {_MAX_FILE_BYTES} bytes"
                )
            count += 1
            total += size
            if count > _MAX_FILES_PER_SKILL:
                raise SkillValidationError(
                    f"skill has more than {_MAX_FILES_PER_SKILL} files"
                )
            if total > _MAX_SKILL_BYTES:
                raise SkillValidationError(f"skill exceeds {_MAX_SKILL_BYTES} bytes")
            yield path, relative, size


def _read_regular_file(path: Path) -> bytes:
    """Read a regular file without following a final symlink."""
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except OSError as exc:
        raise SkillValidationError(f"cannot open {path.name}: {exc}") from exc
    try:
        file_stat = os.fstat(fd)
        if not stat.S_ISREG(file_stat.st_mode) or file_stat.st_size > _MAX_FILE_BYTES:
            raise SkillValidationError(f"{path.name} is not a small regular file")
        with os.fdopen(fd, "rb") as stream:
            fd = -1
            return stream.read(_MAX_FILE_BYTES + 1)[:_MAX_FILE_BYTES]
    finally:
        if fd >= 0:
            os.close(fd)


def _symlinked_parent(path: Path, root: Path) -> str | None:
    """Return a relative symlinked component below ``root`` for ``path``."""
    try:
        relative = path.relative_to(root)
    except ValueError:
        return "."
    current = root
    for part in relative.parts[:-1]:
        current = current / part
        if current.is_symlink():
            return current.relative_to(root).as_posix()
    return None


def _chmod_tree(directory: Path, file_bits: int, directory_bits: int) -> None:
    """Add write/execute bits to a disposable copy without rewriting the tree."""
    os.chmod(directory, os.stat(directory).st_mode & 0o777 | directory_bits)
    for root, dirs, files in os.walk(directory):
        for name in dirs:
            try:
                path = Path(root) / name
                os.chmod(path, path.stat().st_mode & 0o777 | directory_bits)
            except OSError:
                pass
        for name in files:
            try:
                path = Path(root) / name
                os.chmod(path, path.stat().st_mode & 0o777 | file_bits)
            except OSError:
                pass


def copy_skills(source: Path, target: Path) -> CopyResult:
    """Copy validated skills from ``source`` into the disposable ``target``.

    Existing entries in ``target`` are left alone. The copy is made writable by
    its owner so the job microVM, which runs as the same host user, can edit
    it; only the worker can write the persistent store.
    """
    source = Path(source)
    target = Path(target)
    result = CopyResult()
    if source.is_symlink() or not source.is_dir():
        return result
    try:
        target.mkdir(parents=True, exist_ok=True)
    except OSError:
        return result
    os.chmod(target, os.stat(target).st_mode & 0o777 | 0o700)
    for index, entry in enumerate(
        sorted(os.scandir(source), key=lambda item: item.name)
    ):
        if index >= _MAX_SCANNED_SKILLS:
            break
        if result.bytes >= _MAX_COPY_BYTES or len(result.copied) >= _MAX_COPY_SKILLS:
            break
        name = entry.name
        path = Path(entry.path)
        if (
            not _SKILL_NAME.fullmatch(name)
            or entry.is_symlink()
            or not entry.is_dir(follow_symlinks=False)
        ):
            result.rejected.append((name, "not a safe skill directory"))
            continue
        destination = target / name
        if destination.is_symlink():
            result.rejected.append((name, "target skill is a symlink"))
            continue
        try:
            files = list(_skill_entries(path))
        except SkillValidationError as exc:
            result.rejected.append((name, str(exc)))
            continue
        skill_bytes = sum(size for _, _, size in files)
        if result.bytes + skill_bytes > _MAX_COPY_BYTES:
            result.rejected.append((name, "copy budget exhausted"))
            continue
        try:
            for file_path, relative, _ in files:
                destination_file = destination / relative
                destination_file.parent.mkdir(parents=True, exist_ok=True)
                data = _read_regular_file(file_path)
                destination_file.write_bytes(data)
                source_mode = file_path.stat(follow_symlinks=False).st_mode & 0o777
                os.chmod(destination_file, source_mode | 0o600)
                result.files += 1
            if destination.is_dir():
                _chmod_tree(destination, 0o600, 0o700)
        except (OSError, SkillValidationError) as exc:
            result.rejected.append((name, f"copy failed: {exc}"))
            continue
        result.copied.append(name)
        result.bytes += skill_bytes
    return result


def merge_skills(source: Path, target: Path) -> MergeResult:
    """Merge validated skill changes into the persistent store.

    Files are only added or replaced, never deleted. A skill is accepted as a
    whole or skipped as a whole, so a rejected tree cannot leave a partial
    update behind. The caller must only invoke this after a successful learning
    pass; the source directory must not be reachable from the main job microVM.
    """
    source = Path(source)
    target = Path(target)
    result = MergeResult()
    if source.is_symlink() or not source.is_dir():
        return result
    try:
        target.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        result.rejected.append(("", f"cannot create skill store: {exc}"))
        return result
    for index, entry in enumerate(
        sorted(os.scandir(source), key=lambda item: item.name)
    ):
        if index >= _MAX_SCANNED_SKILLS:
            result.rejected.append(("", "too many skill directories to scan"))
            break
        name = entry.name
        path = Path(entry.path)
        if (
            not _SKILL_NAME.fullmatch(name)
            or entry.is_symlink()
            or not entry.is_dir(follow_symlinks=False)
        ):
            result.rejected.append((name, "not a safe skill directory"))
            continue
        if len(result.persisted) >= _MAX_PERSISTED_SKILLS:
            result.rejected.append((name, "per-job skill budget exhausted"))
            continue
        destination = target / name
        if destination.is_symlink():
            result.rejected.append((name, "target skill is a symlink"))
            continue
        if destination.exists() and not destination.is_dir():
            result.rejected.append((name, "target skill is not a directory"))
            continue
        try:
            files = list(_skill_entries(path))
        except SkillValidationError as exc:
            result.rejected.append((name, str(exc)))
            continue
        changes: list[tuple[Path, bytes, int, int]] = []
        invalid = False
        for file_path, relative, size in files:
            destination_file = destination / relative
            linked_parent = _symlinked_parent(destination_file, destination)
            if linked_parent is not None:
                result.rejected.append(
                    (name, f"target parent is a symlink: {linked_parent}")
                )
                invalid = True
                break
            if destination_file.is_symlink():
                result.rejected.append(
                    (name, f"target file is a symlink: {relative.as_posix()}")
                )
                invalid = True
                break
            if destination_file.is_dir():
                result.rejected.append(
                    (name, f"target path is a directory: {relative.as_posix()}")
                )
                invalid = True
                break
            try:
                data = _read_regular_file(file_path)
            except SkillValidationError as exc:
                result.rejected.append((name, str(exc)))
                invalid = True
                break
            try:
                if destination_file.is_file() and destination_file.read_bytes() == data:
                    continue
            except OSError:
                pass
            mode = file_path.stat(follow_symlinks=False).st_mode & 0o777
            changes.append((destination_file, data, size, mode or 0o644))
        if invalid:
            continue
        changed_bytes = sum(size for _, _, size, _ in changes)
        if len(changes) > _MAX_FILES_PER_SKILL or (
            result.bytes + changed_bytes > _MAX_MERGE_BYTES
        ):
            result.rejected.append((name, "merge budget exhausted"))
            continue
        if not changes:
            continue
        try:
            destination.mkdir(parents=True, exist_ok=True)
            for destination_file, data, _, mode in changes:
                destination_file.parent.mkdir(parents=True, exist_ok=True)
                fd, temporary = tempfile.mkstemp(
                    prefix=".kafkallm-", dir=destination_file.parent
                )
                try:
                    with os.fdopen(fd, "wb") as stream:
                        stream.write(data)
                    os.chmod(temporary, mode)
                    os.replace(temporary, destination_file)
                finally:
                    if os.path.exists(temporary):
                        os.unlink(temporary)
        except OSError as exc:
            result.rejected.append((name, f"merge failed: {exc}"))
            continue
        result.persisted.append(name)
        result.files += len(changes)
        result.bytes += changed_bytes
    return result
