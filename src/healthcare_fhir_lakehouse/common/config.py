from __future__ import annotations

import os
import tomllib
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

CONFIG_ENV_VAR = "HEALTHCARE_FHIR_LAKEHOUSE_CONFIG"


class PathSettings(BaseModel):
    source_dataset_dir: str = "mimic-iv-clinical-database-demo-on-fhir-2.1.0"
    source_fhir_dir: str = "mimic-iv-clinical-database-demo-on-fhir-2.1.0/fhir"
    output_dir: str = "output"


class DatasetSettings(BaseModel):
    name: str = "mimic-iv-clinical-database-demo-on-fhir"
    version: str = "2.1.0"


class ProjectConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    repo_root: Path = Field(default_factory=lambda: find_repo_root())
    paths: PathSettings = Field(default_factory=PathSettings)
    dataset: DatasetSettings = Field(default_factory=DatasetSettings)

    @property
    def source_dataset_dir(self) -> Path:
        return resolve_from_root(self.repo_root, self.paths.source_dataset_dir)

    @property
    def source_fhir_dir(self) -> Path:
        return resolve_from_root(self.repo_root, self.paths.source_fhir_dir)

    @property
    def output_dir(self) -> Path:
        return resolve_from_root(self.repo_root, self.paths.output_dir)


def find_repo_root(start: Path | None = None) -> Path:
    current = (start or Path.cwd()).resolve()

    for candidate in (current, *current.parents):
        if (candidate / "pyproject.toml").exists():
            return candidate

    return current


def resolve_from_root(repo_root: Path, path_value: str) -> Path:
    path = Path(path_value).expanduser()
    if path.is_absolute():
        return path
    return repo_root / path


def load_config(config_path: str | Path | None = None) -> ProjectConfig:
    selected_config_path = config_path or os.environ.get(CONFIG_ENV_VAR)
    data: dict[str, Any] = {}

    if selected_config_path:
        path = Path(selected_config_path).expanduser()
        if not path.is_absolute():
            path = find_repo_root() / path
        with path.open("rb") as config_file:
            data = tomllib.load(config_file)

    repo_root = find_repo_root()
    return ProjectConfig(repo_root=repo_root, **data)


__all__ = [
    "CONFIG_ENV_VAR",
    "DatasetSettings",
    "PathSettings",
    "ProjectConfig",
    "find_repo_root",
    "load_config",
    "resolve_from_root",
]
