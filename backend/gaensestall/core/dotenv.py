"""Minimal ``.env`` loader for host-side CLIs.

Docker Compose loads ``.env`` automatically. When the CLI runs directly on the
host, load the same file so ``gaensestall doctor`` and ``gaensestall submit`` behave
like the containerized services. Existing environment variables win; explicit
shell exports are never overwritten.
"""

from __future__ import annotations

import os
from pathlib import Path


def load_dotenv(path: str | Path = ".env", *, override: bool = False) -> bool:
    """Load key/value pairs from *path* into ``os.environ``.

    Returns ``True`` if the file existed. The parser is deliberately small and
    supports comments, blank lines, an optional ``export`` prefix, and single or
    double quoted values.
    """
    env_path = Path(path)
    if not env_path.is_file():
        return False

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        key, separator, value = line.partition("=")
        if not separator:
            continue
        key = key.strip()
        if not key or not (key[0].isalpha() or key[0] == "_"):
            continue
        if not all(character.isalnum() or character == "_" for character in key):
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        if override or key not in os.environ:
            os.environ[key] = value
    return True
