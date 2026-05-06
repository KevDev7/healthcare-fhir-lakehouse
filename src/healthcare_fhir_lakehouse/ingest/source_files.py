from __future__ import annotations

import gzip
import json
from collections.abc import Iterator
from dataclasses import dataclass
from json import JSONDecodeError
from pathlib import Path
from typing import Any


class FhirNdjsonReadError(ValueError):
    """Raised when a compressed NDJSON FHIR source file cannot be parsed."""


@dataclass(frozen=True)
class FhirSourceFile:
    path: Path

    @property
    def filename(self) -> str:
        return self.path.name

    @property
    def resource_family(self) -> str:
        return self.path.name.removesuffix(".ndjson.gz")


def discover_fhir_source_files(source_fhir_dir: Path) -> list[FhirSourceFile]:
    if not source_fhir_dir.is_dir():
        raise FileNotFoundError(
            f"FHIR source directory does not exist: {source_fhir_dir}"
        )

    return [
        FhirSourceFile(path=path)
        for path in sorted(source_fhir_dir.glob("*.ndjson.gz"))
        if path.is_file()
    ]


def iter_fhir_ndjson(source_file: FhirSourceFile | Path) -> Iterator[dict[str, Any]]:
    path = source_file.path if isinstance(source_file, FhirSourceFile) else source_file

    with gzip.open(path, "rt", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            if not line.strip():
                continue

            try:
                resource = json.loads(line)
            except JSONDecodeError as error:
                raise FhirNdjsonReadError(
                    f"Invalid JSON in {path} at line {line_number}: {error.msg}"
                ) from error

            if not isinstance(resource, dict):
                raise FhirNdjsonReadError(
                    f"Expected JSON object in {path} at line {line_number}"
                )

            yield resource


__all__ = [
    "FhirNdjsonReadError",
    "FhirSourceFile",
    "discover_fhir_source_files",
    "iter_fhir_ndjson",
]
