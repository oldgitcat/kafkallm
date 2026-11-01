from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field, ValidationError


class ToolError(RuntimeError):
    pass


class Workspace:
    def __init__(self, root: Path) -> None:
        self.root = root.resolve()

    def resolve(self, relative_path: str) -> Path:
        candidate = Path(relative_path)
        if candidate.is_absolute():
            raise ToolError("Nur relative Workspace-Pfade sind erlaubt.")

        resolved = (self.root / candidate).resolve()
        try:
            resolved.relative_to(self.root)
        except ValueError as exc:
            raise ToolError("Der Pfad verlässt den Workspace.") from exc
        return resolved

    def read(self, relative_path: str) -> str:
        path = self.resolve(relative_path)
        if not path.is_file():
            raise ToolError(f"Datei nicht gefunden: {relative_path}")
        if path.stat().st_size > 200_000:
            raise ToolError("Die Datei ist größer als das Leselimit von 200.000 Bytes.")
        return path.read_text(encoding="utf-8")

    def write(self, relative_path: str, content: str) -> dict[str, Any]:
        path = self.resolve(relative_path)
        if path == self.root:
            raise ToolError("Der Workspace selbst ist keine Datei.")
        encoded = content.encode("utf-8")
        if len(encoded) > 1_000_000:
            raise ToolError("Die Datei überschreitet das Schreiblimit von 1 MB.")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(encoded)
        return {"path": path.relative_to(self.root).as_posix(), "bytes": len(encoded)}

    def list_files(self, relative_path: str = ".") -> list[str]:
        path = self.resolve(relative_path)
        if not path.is_dir():
            raise ToolError(f"Verzeichnis nicht gefunden: {relative_path}")
        files = [
            item.relative_to(self.root).as_posix()
            for item in path.rglob("*")
            if item.is_file()
        ]
        return sorted(files)[:200]


class ReadFileArgs(BaseModel):
    path: str = Field(min_length=1)


class WriteFileArgs(BaseModel):
    path: str = Field(min_length=1)
    content: str


class ListFilesArgs(BaseModel):
    path: str = "."


class ToolRegistry:
    def __init__(self, workspace: Workspace) -> None:
        self.workspace = workspace
        self._args = {
            "read_workspace_file": ReadFileArgs,
            "write_workspace_file": WriteFileArgs,
            "list_workspace_files": ListFilesArgs,
        }

    @property
    def specifications(self) -> list[dict[str, Any]]:
        descriptions = {
            "read_workspace_file": "Liest eine UTF-8-Textdatei aus dem Job-Workspace.",
            "write_workspace_file": (
                "Schreibt eine Textdatei. Ergebnisdateien gehören in outputs/."
            ),
            "list_workspace_files": "Listet Dateien im Job-Workspace auf.",
        }
        return [
            {
                "type": "function",
                "function": {
                    "name": name,
                    "description": descriptions[name],
                    "parameters": model.model_json_schema(),
                },
            }
            for name, model in self._args.items()
        ]

    def execute(self, name: str, arguments_json: str) -> dict[str, Any]:
        if name not in self._args:
            raise ToolError(f"Unbekanntes Tool: {name}")
        try:
            arguments = json.loads(arguments_json)
            args = self._args[name].model_validate(arguments)
        except (json.JSONDecodeError, ValidationError) as exc:
            raise ToolError(f"Ungültige Tool-Argumente: {exc}") from exc

        if name == "read_workspace_file":
            return {"content": self.workspace.read(args.path)}
        if name == "write_workspace_file":
            return self.workspace.write(args.path, args.content)
        return {"files": self.workspace.list_files(args.path)}
