"""Detect masking policies that would silently break de-duplication.

Catalog-linked databases can carry tag-based masking policies (for example a
PII_MASK_STRING policy attached through a PII tag). If a KEY column is masked
for the role that runs capture, every key reads as the same constant, so
QUALIFY ROW_NUMBER() OVER (PARTITION BY <key>) keeps one row per masked value
and the Silver table collapses - with no error.
"""
from __future__ import annotations

MASK_SENTINELS = ("********", "***MASKED***")


def masked_columns(s, source_fqn: str) -> dict[str, str]:
    """Column -> policy name for every masking policy on the object."""
    db = source_fqn.split(".")[0]
    lit = source_fqn.replace("'", "''")
    try:
        rows = s.rows(
            f"SELECT REF_COLUMN_NAME AS C, POLICY_NAME AS P FROM TABLE({db}.INFORMATION_SCHEMA.POLICY_REFERENCES("
            f"REF_ENTITY_NAME => '{lit}', REF_ENTITY_DOMAIN => 'table')) WHERE POLICY_KIND = 'MASKING_POLICY'")
    except Exception:
        return {}
    return {str(r["C"]): str(r["P"]) for r in rows if r.get("C")}


def key_masked_for_current_role(s, source_fqn: str, keys: list[str]) -> list[str]:
    """Key columns that the CURRENT role sees masked (sampled)."""
    out = []
    for k in keys:
        v = s.one(f'SELECT TO_VARCHAR("{k}") AS V FROM {source_fqn} WHERE "{k}" IS NOT NULL LIMIT 1').get("V")
        if v is not None and (str(v) in MASK_SENTINELS or set(str(v)) == {"*"}):
            out.append(k)
    return out
