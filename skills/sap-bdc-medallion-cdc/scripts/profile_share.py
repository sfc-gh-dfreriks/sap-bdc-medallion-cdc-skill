#!/usr/bin/env python3
"""Profile one SAP BDC catalog-linked database and draft a share config.

    python3 profile_share.py --connection <name>                    # list catalog-linked DBs
    python3 profile_share.py --connection <name> --database CUSTOMER_V1 \
        --target SAP_CUSTOMER_360 --warehouse COMPUTE_WH --out config.yaml [--guess-keys]

Read-only. For each object it records row count, column types, the CDC track
(STANDARD / CUSTOM / NONE), the flow hash, and the indicator-style columns
whose TYPE you must check before writing a flag (TEXT vs BOOLEAN). It writes a
draft config.yaml with `keys` left for you to fill and verify - a
catalog-linked table declares no primary key, so keys are never inferred
silently. --guess-keys proposes the shortest leading-column prefix that is
unique; treat it as a suggestion and confirm with verify_grain.py.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "readiness"))
from conn import Session, fqn  # noqa: E402
from masking import masked_columns  # noqa: E402
from discover import (catalog_linked_databases, describe, find_standard_signal,  # noqa: E402
                      find_watermark, objects_in, track_of)

META = {"__operation_type", "__timestamp"}
INDICATOR_HINTS = ("is", "block", "indicator", "flag", "deletion")


def indicator_columns(o) -> list[tuple[str, str]]:
    out = []
    for c in o.columns:
        n = c.name.lower()
        if n in META or n.startswith(("load_type_", "run_id_")):
            continue
        if c.type.upper().startswith("BOOLEAN") or (
                c.is_varchar and any(n.startswith(h) or h in n for h in INDICATOR_HINTS[1:])
                or n.startswith("is") and c.name[2:3].isupper()):
            out.append((c.name, c.type.split("(")[0]))
    return out


def guess_keys(s: Session, o, rows: int, max_parts: int = 6) -> list[str] | None:
    cols = [c.name for c in o.columns if c.name.lower() not in META
            and not c.name.lower().startswith(("load_type_", "run_id_"))][:max_parts]
    for i in range(1, len(cols) + 1):
        expr = ", ".join(f'COALESCE(TO_VARCHAR({fqn(c)}), \'~\')' for c in cols[:i])
        n = s.one(f"SELECT COUNT(DISTINCT {expr}) AS N FROM {o.fqn}").get("N")
        if int(n or 0) == rows:
            return cols[:i]
    return None


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--connection")
    ap.add_argument("--database", help="catalog-linked database to profile")
    ap.add_argument("--target", default="SAP_BDC_MEDALLION", help="target database name for the draft config")
    ap.add_argument("--warehouse", default="COMPUTE_WH")
    ap.add_argument("--out", help="write a draft config.yaml here")
    ap.add_argument("--role", help="USE ROLE first (use the role capture will run as)")
    ap.add_argument("--guess-keys", action="store_true",
                    help="propose a unique leading-column prefix per object (runs COUNT DISTINCT queries)")
    a = ap.parse_args()
    s = Session(a.connection)
    if a.role:
        s.execute(f"USE ROLE {a.role}")

    clds = catalog_linked_databases(s)
    if not a.database:
        print("Catalog-linked databases on this account:" if clds else "No catalog-linked databases found.")
        for d in clds:
            print(f"  {d}")
        return
    if a.database not in clds:
        print(f"WARNING: {a.database} is not a catalog-linked database. If it is a COPY of one, stop: "
              "connector metadata on copies was found corrupted. Point at the live share.")

    objs = [describe(s, o) for o in objects_in(s, a.database)]
    schemas = sorted({o.schema for o in objs})
    tracks, hashes, lines, cfg_objs = set(), set(), [], []
    print(f"\n{a.database}: {len(objs)} object(s) in schema(s) {', '.join(schemas)}\n")
    for o in objs:
        if o.error:
            print(f"  [skip] {o.schema}.{o.name}: {o.error}")
            continue
        rows = int(s.one(f"SELECT COUNT(*) AS N FROM {o.fqn}").get("N") or 0)
        tr = track_of(o)
        tracks.add(tr)
        sig = find_standard_signal(o)
        if sig and sig.flow_hash:
            hashes.add(sig.flow_hash)
        wm = None if sig else find_watermark(o)
        inds = indicator_columns(o)
        keys = guess_keys(s, o, rows) if a.guess_keys and rows else None
        print(f"  {o.name:<34} rows={rows:<10,} track={tr:<8} "
              f"{'hash=' + sig.flow_hash[:8] + '…' if sig and sig.flow_hash else ''}"
              f"{'watermark=' + wm.name if wm else ''}")
        pol = masked_columns(s, o.fqn)
        if pol:
            print(f"      MASKED columns ({len(pol)}, policy {sorted(set(pol.values()))[0]}): "
                  + ", ".join(sorted(pol)[:12]) + (" ..." if len(pol) > 12 else ""))
        if inds:
            print("      indicator columns (check type before COALESCE): "
                  + ", ".join(f"{n}:{t}" for n, t in inds))
        if keys:
            print(f"      candidate key (verify!): {', '.join(keys)}")
        cfg_objs.append({"name": o.name.upper(), "table": o.name, "rows": rows,
                         "keys": keys, "cls": "EMPTY" if rows == 0 else "MASTER", "wm": wm.name if wm else None})

    print()
    if len(schemas) > 1:
        print("NOTE: more than one schema - generate one config per schema.")
    if len(hashes) > 1:
        print(f"NOTE: {len(hashes)} different flow hashes - objects come from different products/flows.")
    if "NONE" in tracks:
        print("NOTE: some objects carry no change signal; they can only be captured by full-refresh comparison.")

    if a.out:
        track = "STANDARD" if "STANDARD" in tracks else "CUSTOM"
        y = ["# DRAFT generated by profile_share.py - fill/verify every `keys` with verify_grain.py,",
             "# set class (MASTER | TRANSACTION | EMPTY), add Silver flags after checking column types.",
             f"target_database: {a.target}", f"warehouse: {a.warehouse}", "", "source:",
             f"  database: {a.database}", f"  schema: {schemas[0] if schemas else ''}", f"  track: {track}"]
        if track == "STANDARD":
            y.append(f"  flow_hash: {sorted(hashes)[0] if hashes else 'TODO'}")
        else:
            wms = sorted({o['wm'] for o in cfg_objs if o['wm']})
            y.append(f"  watermark_column: {wms[0] if wms else 'TODO'}")
        y += ["", "objects:"]
        for o in cfg_objs:
            y += [f"  - name: {o['name']}", f"    table: {o['table']}",
                  f"    keys: [{', '.join(o['keys'])}]" if o["keys"] else "    keys: []   # TODO verify grain",
                  f"    class: {o['cls']}", f"    # rows at profile: {o['rows']:,}"]
        y += ["", "gold:", "  dynamic_tables: []   # list Gold DTs once written (see references/gold-patterns.md)"]
        Path(a.out).write_text("\n".join(y) + "\n")
        print(f"wrote draft config {a.out}")


if __name__ == "__main__":
    main()
