"""Primary keys SAP BDC publishes on a catalog-linked database.

Since 2026-10-30 a catalog-linked database answers SHOW PRIMARY KEYS with one
row per key column (key_sequence, constraint_name SYS_CONSTRAINT_<uuid>,
rely=true). RELY means SAP asserts the key; Snowflake does not enforce it, so
verify_grain.py still counts rows against distinct keys. Custom data products
publish none. Column names are case-sensitive camel case: always quote them.
"""
from __future__ import annotations

from conn import Session, fqn


def declared_keys(s: Session, database: str, schema: str | None = None) -> dict[tuple[str, str], dict]:
    """{(schema, table): {"columns": [...in key_sequence order], "constraint": str, "rely": bool}}."""
    scope = f"SCHEMA {fqn(database, schema)}" if schema else f"DATABASE {fqn(database)}"
    try:
        rows = s.rows(f"SHOW PRIMARY KEYS IN {scope}")
    except Exception:
        return {}
    acc: dict[tuple[str, str], dict] = {}
    for r in rows:
        k = (r["schema_name"], r["table_name"])
        e = acc.setdefault(k, {"constraint": r["constraint_name"],
                               "rely": str(r.get("rely")).lower() == "true", "parts": []})
        e["parts"].append((int(r["key_sequence"]), r["column_name"]))
    return {k: {"columns": [c for _, c in sorted(v["parts"])], "constraint": v["constraint"],
                "rely": v["rely"]} for k, v in acc.items()}
