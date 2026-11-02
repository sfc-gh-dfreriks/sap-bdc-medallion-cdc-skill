#!/usr/bin/env python3
"""Verify that every object's config keys really are its grain.

    python3 verify_grain.py <config.yaml> --connection <name> [--role <capture_role>]

Read-only. For each object: total rows, distinct key combinations, rows with a
NULL in any key part, masking of key columns for the current role, and the
config keys against the primary key SAP declares on the share (SHOW PRIMARY
KEYS). The declaration is RELY - asserted by SAP, not enforced by Snowflake -
so it is counted too, never trusted blindly.

Verdicts:
  PASS       rows == distinct keys; says whether the key is declared or inferred
  DRIFT      config keys differ from the declared key (failure unless the object
             sets key_override: true with a key_override_reason)
  VIOLATED   the declared key itself is not unique (failure; counts reported)
  FAIL       config key too short: de-duplicating on it silently discards rows
             (an intuitive four-part key once collapsed 39,462 rows to 14,017)
  BLOCKED    a key column is masked for this role (failure)
  POPULATED  class EMPTY but the object has rows
  EMPTY      no rows

Exit code is non-zero if any object fails.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent / "readiness"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from conn import Session  # noqa: E402
from declared_keys import declared_keys  # noqa: E402
from render_sql import build_context  # noqa: E402
from masking import key_masked_for_current_role, masked_columns  # noqa: E402


def _int(v) -> int:
    try:
        return 0 if v is None or v != v else int(v)   # v != v catches NaN from pandas
    except (TypeError, ValueError):
        return 0


def _distinct(cols: list[str]) -> str:
    return ", ".join(f"COALESCE(TO_VARCHAR(\"{k}\"), '~')" for k in cols)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("config")
    ap.add_argument("--connection")
    ap.add_argument("--role", help="role capture will run as (must see unmasked keys)")
    a = ap.parse_args()
    path = Path(a.config).resolve()
    cfg = yaml.safe_load(path.read_text())
    ctx = build_context(cfg, path)
    s = Session(a.connection)
    if a.role:
        s.execute(f"USE ROLE {a.role}")
    role = s.one("SELECT CURRENT_ROLE() AS R").get("R")
    print(f"role: {role}")
    schema = cfg["source"]["schema"]
    pks = declared_keys(s, cfg["source"]["database"].strip('"'), schema)
    print(f"declared primary keys on {cfg['source']['database']}.{schema}: {len(pks)}")

    failed = 0
    print(f"{'object':<30} {'rows':>12} {'distinct keys':>14} {'null-key rows':>14}  verdict")
    for o in ctx["objects"]:
        pk = pks.get((schema, o["table"]))
        declared = pk["columns"] if pk else None
        differs = declared is not None and set(declared) != set(o["key_cols"])
        undeclared = [f'"{k}"' for k in o["key_cols"] if k not in o["nullable_keys"]]
        nulls = " OR ".join(f"{k} IS NULL" for k in undeclared) or "FALSE"
        extra = f", COUNT(DISTINCT {_distinct(declared)}) AS DD" if differs else ""
        r = s.one(f"SELECT COUNT(*) AS N, COUNT(DISTINCT {_distinct(o['key_cols'])}) AS D, "
                  f"COUNT_IF({nulls}) AS NK{extra} FROM {o['source_fqn']}")
        n, d, nk = (_int(r.get(k)) for k in ("N", "D", "NK"))
        dd = _int(r.get("DD")) if differs else d
        pol = masked_columns(s, o["source_fqn"])
        masked_keys = [k for k in o["key_cols"] if k in pol]
        hidden = key_masked_for_current_role(s, o["source_fqn"], masked_keys) if n and masked_keys else []
        row = f"{o['name']:<30} {n:>12,} {d:>14,} {nk:>14,}  "
        if hidden:
            failed += 1
            print(row + f"BLOCKED - key column(s) {', '.join(hidden)} are MASKED for {role} "
                        f"({pol[hidden[0]]}). Silver would collapse. See references/masking-and-privileges.md")
            continue
        if n == 0:
            print(row + ("EMPTY (class should be EMPTY)" if o["cls"] != "EMPTY" else "EMPTY"))
            continue
        if o["cls"] == "EMPTY":
            print(row + f"POPULATED - class is EMPTY but the object now has {n:,} rows; set class MASTER/TRANSACTION")
            continue
        if declared is not None and dd != n:
            failed += 1
            print(row + f"VIOLATED - declared key {pk['constraint']} ({', '.join(declared)}) is not unique: "
                        f"{n:,} rows / {dd:,} distinct ({n - dd:,} duplicate row(s)). RELY is not enforced; "
                        "raise it with SAP and do not build on it")
            continue
        if differs and not o["key_override"]:
            failed += 1
            print(row + f"DRIFT - config keys ({', '.join(o['key_cols'])}) differ from declared key "
                        f"{pk['constraint']} ({', '.join(declared)})"
                        + (f"; config key would discard {n - d:,} row(s)" if d != n else "")
                        + ". Use the declared key, or set key_override: true with key_override_reason")
            continue
        if d != n:
            failed += 1
            print(row + f"FAIL - key too short, would discard {n - d:,} row(s)")
            continue
        if differs:
            note = f"override of declared {pk['constraint']} ({o['key_override_reason']})"
        elif declared is not None:
            note = f"declared {pk['constraint']}" + ("" if o["key_source"] == "declared"
                                                     else " - set key_source: declared")
            if list(declared) != list(o["key_cols"]):
                note += f"; declared order is {', '.join(declared)}"
        else:
            note = "inferred - no declared key" + (" (config says key_source: declared!)"
                                                     if o["key_source"] == "declared" else "")
        verdict = f"PASS ({note})"
        if masked_keys:
            verdict += f" (masked keys {masked_keys} visible to this role)"
        if nk:
            verdict += f" - {nk} row(s) have a NULL key part: list it under nullable_keys"
        print(row + verdict)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
