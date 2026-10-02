#!/usr/bin/env python3
"""Verify that every object's declared keys really are its grain.

    python3 verify_grain.py <config.yaml> --connection <name>

Read-only. For each object: total rows, distinct key combinations, and rows
with a NULL in any key part. PASS means rows == distinct keys. A FAIL is not
cosmetic: de-duplicating on a short key does not error, it silently discards
rows (on the reference tenant, an intuitive four-part key collapsed 39,462 rows
to 14,017). Exit code is non-zero if any populated object fails.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent / "readiness"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from conn import Session  # noqa: E402
from render_sql import build_context  # noqa: E402
from masking import key_masked_for_current_role, masked_columns  # noqa: E402


def _int(v) -> int:
    try:
        return 0 if v is None or v != v else int(v)   # v != v catches NaN from pandas
    except (TypeError, ValueError):
        return 0


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("config")
    ap.add_argument("--connection")
    ap.add_argument("--role", help="role capture will run as (must see unmasked keys)")
    a = ap.parse_args()
    path = Path(a.config).resolve()
    ctx = build_context(yaml.safe_load(path.read_text()), path)
    s = Session(a.connection)
    if a.role:
        s.execute(f"USE ROLE {a.role}")
    role = s.one("SELECT CURRENT_ROLE() AS R").get("R")
    print(f"role: {role}")

    failed = 0
    print(f"{'object':<30} {'rows':>12} {'distinct keys':>14} {'null-key rows':>14}  verdict")
    for o in ctx["objects"]:
        keys = [f'"{k}"' for k in o["key_cols"]]
        distinct = ", ".join(f"COALESCE(TO_VARCHAR({k}), '~')" for k in keys)
        undeclared = [f'"{k}"' for k in o["key_cols"] if k not in o["nullable_keys"]]
        nulls = " OR ".join(f"{k} IS NULL" for k in undeclared) or "FALSE"
        r = s.one(f"SELECT COUNT(*) AS N, COUNT(DISTINCT {distinct}) AS D, "
                  f"COUNT_IF({nulls}) AS NK FROM {o['source_fqn']}")
        n, d, nk = (_int(r.get(k)) for k in ("N", "D", "NK"))
        pol = masked_columns(s, o["source_fqn"])
        masked_keys = [k for k in o["key_cols"] if k in pol]
        hidden = key_masked_for_current_role(s, o["source_fqn"], masked_keys) if n and masked_keys else []
        if hidden:
            failed += 1
            print(f"{o['name']:<30} {n:>12,} {d:>14,} {nk:>14,}  BLOCKED - key column(s) {', '.join(hidden)} "
                  f"are MASKED for {role} ({pol[hidden[0]]}). Silver would collapse. See references/masking.md")
            continue
        if n == 0:
            verdict = "EMPTY (class should be EMPTY)" if o["cls"] != "EMPTY" else "EMPTY"
        elif o["cls"] == "EMPTY":
            verdict = f"POPULATED - class is EMPTY but the object now has {n:,} rows; set class MASTER/TRANSACTION"
        elif n == d:
            verdict = "PASS" + (f" (masked keys {masked_keys} visible to this role)" if masked_keys else "") + (f" - {nk} row(s) have a NULL key part: list it under nullable_keys"
                                if nk else "")
        else:
            failed += 1
            verdict = f"FAIL - key too short, would discard {n - d:,} row(s)"
        print(f"{o['name']:<30} {n:>12,} {d:>14,} {nk:>14,}  {verdict}")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
