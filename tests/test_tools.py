from pathlib import Path

import pytest

from kafkallm.tools import ToolError, Workspace


def test_workspace_file_roundtrip(tmp_path: Path) -> None:
    workspace = Workspace(tmp_path)

    written = workspace.write("outputs/result.txt", "Hello")

    assert written == {"path": "outputs/result.txt", "bytes": 5}
    assert workspace.read("outputs/result.txt") == "Hello"
    assert workspace.list_files() == ["outputs/result.txt"]


@pytest.mark.parametrize("path", ["../outside.txt", "/absolute.txt"])
def test_workspace_blocks_unsafe_paths(tmp_path: Path, path: str) -> None:
    workspace = Workspace(tmp_path)

    with pytest.raises(ToolError):
        workspace.write(path, "not allowed")
