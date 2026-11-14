from __future__ import annotations

import os
import stat
from pathlib import Path

from kafkallm.sandbox.skills import copy_skills, merge_skills


def _skill(root: Path, name: str, body: str = "# Skill\n") -> Path:
    directory = root / name
    directory.mkdir(parents=True)
    (directory / "SKILL.md").write_text(body, encoding="utf-8")
    return directory


def test_copy_skills_copies_only_valid_skills_and_makes_them_writable(
    tmp_path: Path,
) -> None:
    source = tmp_path / "store"
    target = tmp_path / "copy"
    skill = _skill(source, "good", "# Good\n")
    (skill / "references").mkdir()
    (skill / "references" / "notes.md").write_text("notes", encoding="utf-8")
    _skill(source, "no-entry").rename(source / "no-entry-moved")
    (source / "no-entry-moved" / "SKILL.md").unlink()
    (source / "linked").symlink_to(skill)
    (source / "bad name").mkdir()
    (source / "bad name" / "SKILL.md").write_text("x", encoding="utf-8")
    _skill(source, "empty", "   \n")

    result = copy_skills(source, target)

    assert result.copied == ["good"]
    assert result.files == 2
    assert (target / "good" / "SKILL.md").read_text() == "# Good\n"
    assert (target / "good" / "references" / "notes.md").read_text() == "notes"
    assert stat.S_IMODE((target / "good" / "SKILL.md").stat().st_mode) & 0o200
    assert stat.S_IMODE((target / "good").stat().st_mode) & 0o200
    assert not (target / "linked").exists()
    assert not (target / "bad name").exists()
    assert not (target / "empty").exists()
    assert not (target / "no-entry-moved").exists()
    assert {name for name, _ in result.rejected} == {
        "linked",
        "bad name",
        "empty",
        "no-entry-moved",
    }


def test_copy_skills_rejects_symlinks_special_files_and_oversized_entries(
    tmp_path: Path,
) -> None:
    source = tmp_path / "store"
    _skill(source, "linked-file")
    (source / "linked-file" / "alias").symlink_to("/etc/passwd")
    fifo_skill = _skill(source, "fifo")
    os.mkfifo(fifo_skill / "pipe")
    oversized = _skill(source, "oversized")
    (oversized / "big.bin").write_bytes(b"x" * (256 * 1024 + 1))
    deep = _skill(source, "deep")
    current = deep
    for _ in range(18):
        current = current / "nested"
        current.mkdir()

    result = copy_skills(source, tmp_path / "copy")

    assert result.copied == []
    assert {name for name, _ in result.rejected} == {
        "linked-file",
        "fifo",
        "oversized",
        "deep",
    }
    target = tmp_path / "copy"
    assert list(target.iterdir()) == []


def test_copy_skills_refuses_a_symlinked_target(tmp_path: Path) -> None:
    source = tmp_path / "store"
    _skill(source, "evil", "# Evil\n")
    outside = tmp_path / "outside"
    outside.mkdir()
    target = tmp_path / "copy"
    target.mkdir()
    (target / "evil").symlink_to(outside)

    result = copy_skills(source, target)

    assert result.copied == []
    assert result.rejected == [("evil", "target skill is a symlink")]
    assert list(outside.iterdir()) == []


def test_merge_skills_adds_and_updates_without_deleting(tmp_path: Path) -> None:
    target = tmp_path / "store"
    existing = _skill(target, "guard", "# Guard\nold\n")
    (existing / "keep.md").write_text("keep me", encoding="utf-8")
    (existing / "rules.md").write_text("old rules", encoding="utf-8")

    source = tmp_path / "proposal"
    changed = _skill(source, "guard", "# Guard\nnew\n")
    (changed / "rules.md").write_text("new rules", encoding="utf-8")
    _skill(source, "learned", "# Learned\n")

    result = merge_skills(source, target)

    assert result.persisted == ["guard", "learned"]
    assert result.files == 3
    assert (target / "guard" / "SKILL.md").read_text() == "# Guard\nnew\n"
    assert (target / "guard" / "rules.md").read_text() == "new rules"
    assert (target / "guard" / "keep.md").read_text() == "keep me"
    assert (target / "learned" / "SKILL.md").read_text() == "# Learned\n"
    assert stat.S_IMODE((target / "learned" / "SKILL.md").stat().st_mode) == 0o644


def test_merge_skills_is_idempotent(tmp_path: Path) -> None:
    target = tmp_path / "store"
    source = tmp_path / "proposal"
    _skill(source, "learned", "# Learned\n")

    first = merge_skills(source, target)
    second = merge_skills(source, target)

    assert first.persisted == ["learned"]
    assert second.persisted == []
    assert second.rejected == []


def test_merge_skills_rejects_unsafe_trees_and_target_symlinks(
    tmp_path: Path,
) -> None:
    target = tmp_path / "store"
    outside = tmp_path / "outside"
    outside.mkdir()
    target.mkdir()
    (target / "linked").symlink_to(outside)
    existing = _skill(target, "guard", "# Guard\ntrusted\n")
    (existing / "rules.md").symlink_to(outside / "rules.md")

    source = tmp_path / "proposal"
    _skill(source, "linked", "# Linked\n")
    guarded = _skill(source, "guard", "# Guard\nchanged\n")
    (guarded / "rules.md").write_text("changed rules", encoding="utf-8")
    _skill(source, "empty", "  ")
    linked_tree = _skill(source, "tree")
    (linked_tree / "alias").symlink_to("/etc/passwd")
    (source / "bad name").mkdir()
    (source / "bad name" / "SKILL.md").write_text("x", encoding="utf-8")

    result = merge_skills(source, target)

    assert result.persisted == []
    assert {name for name, _ in result.rejected} == {
        "linked",
        "guard",
        "empty",
        "tree",
        "bad name",
    }
    assert (target / "guard" / "SKILL.md").read_text() == "# Guard\ntrusted\n"
    assert (target / "guard" / "rules.md").is_symlink()
    assert list(outside.iterdir()) == []


def test_merge_skills_caps_the_number_of_changes(tmp_path: Path) -> None:
    source = tmp_path / "proposal"
    for name in ("alpha", "bravo", "charlie", "delta"):
        _skill(source, name, f"# {name}\n")

    result = merge_skills(source, tmp_path / "store")

    assert result.persisted == ["alpha", "bravo", "charlie"]
    assert result.rejected == [("delta", "per-job skill budget exhausted")]
    assert not (tmp_path / "store" / "delta").exists()


def test_skill_tree_directory_bomb_is_rejected(tmp_path: Path) -> None:
    source = tmp_path / "store"
    skill = _skill(source, "wide")
    for index in range(1001):
        (skill / f"dir-{index}").mkdir()

    result = copy_skills(source, tmp_path / "copy")

    assert result.copied == []
    assert result.rejected
    assert "more than" in result.rejected[0][1]


def test_merge_skills_rejects_symlinked_target_parents(tmp_path: Path) -> None:
    target = tmp_path / "store"
    skill = _skill(target, "guard", "# Guard\ntrusted\n")
    outside = tmp_path / "outside"
    outside.mkdir()
    (skill / "references").symlink_to(outside)

    source = tmp_path / "proposal"
    changed = _skill(source, "guard", "# Guard\nchanged\n")
    (changed / "references").mkdir()
    (changed / "references" / "rules.md").write_text("changed rules", encoding="utf-8")

    result = merge_skills(source, target)

    assert result.persisted == []
    assert ("guard", "target parent is a symlink: references") in result.rejected
    assert (skill / "SKILL.md").read_text() == "# Guard\ntrusted\n"
    assert list(outside.iterdir()) == []
