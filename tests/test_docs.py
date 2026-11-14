from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SKIP_DIRS = {".git", ".venv", "__pycache__", ".pytest_cache", ".ruff_cache"}
LINK = re.compile(r"\[[^\]]*\]\(([^)]+)\)")
CODE = re.compile(r"`([^`]+)`")
ROOTED_PATHS = ("src/", "stall/", "gaense/", "infra/", "tools/", "scripts/")
ROOT_FILES = {"compose.yaml", "Makefile", "pyproject.toml", "constraints.txt"}


def _markdown_files() -> list[Path]:
    return sorted(
        path
        for path in ROOT.rglob("*.md")
        if not any(part in SKIP_DIRS for part in path.parts)
    )


def test_relative_documentation_links_exist() -> None:
    missing: list[str] = []
    for path in _markdown_files():
        for target in LINK.findall(path.read_text(encoding="utf-8")):
            target = target.strip()
            if not target or target.startswith(
                ("http://", "https://", "mailto:", "#", "tel:", "data:")
            ):
                continue
            local = target.split("#", 1)[0].split("?", 1)[0]
            if not local or "{{" in local or "<" in local:
                continue
            resolved = (path.parent / local).resolve()
            if not resolved.exists():
                missing.append(f"{path.relative_to(ROOT)} -> {target}")
    assert not missing, "Broken documentation links:\n" + "\n".join(missing)


def test_documented_repository_paths_exist() -> None:
    missing: list[str] = []
    for path in _markdown_files():
        for value in CODE.findall(path.read_text(encoding="utf-8")):
            value = value.strip()
            if value in ROOT_FILES:
                if not (ROOT / value).exists():
                    missing.append(f"{path.relative_to(ROOT)} -> {value}")
                continue
            if not value.startswith(ROOTED_PATHS):
                continue
            if any(char in value for char in ("*", "<", ">", "{", "}", " ")):
                continue
            it = value.split()[0]
            if not (ROOT / it).exists():
                missing.append(f"{path.relative_to(ROOT)} -> {it}")
    assert not missing, "Documented paths do not exist:\n" + "\n".join(missing)
