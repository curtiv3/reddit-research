from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class Config:
    raw: dict[str, Any]
    path: Path

    @property
    def outputs(self) -> dict[str, str]:
        return self.raw.get("outputs", {})

    @property
    def limits(self) -> dict[str, Any]:
        return self.raw.get("limits", {})

    @property
    def queries(self) -> list[str]:
        return list(self.raw.get("queries", []))

    @property
    def engines(self) -> list[dict[str, Any]]:
        return list(self.raw.get("engines", []))

    @property
    def reddit(self) -> dict[str, Any]:
        return self.raw.get("reddit", {})

    @property
    def clustering(self) -> dict[str, Any]:
        return self.raw.get("clustering", {})


def load_config(path: str | Path) -> Config:
    config_path = Path(path)
    data = yaml.safe_load(config_path.read_text()) or {}
    return Config(raw=data, path=config_path)


def resolve_path(base: Path, value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return (base / path).resolve()
