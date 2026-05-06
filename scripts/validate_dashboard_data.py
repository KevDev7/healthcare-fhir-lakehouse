from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_JSON = ROOT / "docs" / "data" / "dashboard.json"

REQUIRED_TOP_LEVEL_KEYS = {
    "meta",
    "table_counts",
    "quality_checks",
    "relationship_audit",
    "encounters",
    "conditions",
    "measurements",
    "medications",
    "procedures",
}

REQUIRED_MEASUREMENT_KEYS = {
    "vitals_volume",
    "labs_volume",
    "vitals_trend",
    "labs_trend",
}


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(message)


def require_non_empty_list(data: dict[str, Any], key: str) -> None:
    value = data.get(key)
    require(
        isinstance(value, list) and len(value) > 0,
        f"{key} must be a non-empty list",
    )


def main() -> None:
    require(DASHBOARD_JSON.exists(), f"{DASHBOARD_JSON} does not exist")
    data = json.loads(DASHBOARD_JSON.read_text(encoding="utf-8"))

    missing = REQUIRED_TOP_LEVEL_KEYS - data.keys()
    require(not missing, f"dashboard.json missing keys: {sorted(missing)}")

    meta = data["meta"]
    require(meta["source_resources"] == 928935, "source resource count changed")
    require(meta["patients"] == 100, "patient count changed")
    require(meta["medication_events"] == 91873, "medication event count changed")
    require(meta["procedures"] == 3450, "procedure count changed")
    require(meta["failed_checks"] == 0, "dashboard should not publish failed checks")

    require_non_empty_list(data, "table_counts")
    require_non_empty_list(data, "quality_checks")

    relationship = data["relationship_audit"]
    orphan_count = sum(
        value
        for key, value in relationship.items()
        if "_orphan_" in key and isinstance(value, int)
    )
    require(relationship["passed"], "relationship audit should pass")
    require(orphan_count == 0, "populated references must resolve")

    measurements = data["measurements"]
    missing_measurements = REQUIRED_MEASUREMENT_KEYS - measurements.keys()
    require(
        not missing_measurements,
        f"measurements missing keys: {sorted(missing_measurements)}",
    )
    for key in REQUIRED_MEASUREMENT_KEYS:
        require_non_empty_list(measurements, key)

    require_non_empty_list(data["medications"], "by_activity_type")
    require_non_empty_list(data["medications"], "top_activity")
    require_non_empty_list(data["medications"], "fulfillment_paths")
    require_non_empty_list(data["procedures"], "top")

    print(f"Validated {DASHBOARD_JSON.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
