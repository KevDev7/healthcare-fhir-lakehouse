from __future__ import annotations

import hashlib


def stable_key(namespace: str, value: str | None) -> str | None:
    if value is None:
        return None
    return hashlib.md5(f"{namespace}:{value}".encode()).hexdigest()


def timestamp_from_iso_text_sql(column_name: str) -> str:
    escaped = duckdb_escape_identifier(column_name)
    return f"try_cast(substr({escaped}, 1, 19) as timestamp)"


def duckdb_escape_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


__all__ = [
    "duckdb_escape_identifier",
    "stable_key",
    "timestamp_from_iso_text_sql",
]
