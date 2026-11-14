"""Stable slug helpers shared by agents and sandboxes."""

from __future__ import annotations

import uuid


def slugify(value: str, *, prefix: str, max_length: int = 64) -> str:
    slug = "".join(
        character if character.isalnum() else "-" for character in value.strip().lower()
    ).strip("-")
    slug = "-".join(part for part in slug.split("-") if part)[:max_length]
    return slug or f"{prefix}-{uuid.uuid4().hex[:8]}"
