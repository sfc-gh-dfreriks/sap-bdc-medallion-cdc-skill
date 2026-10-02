#!/usr/bin/env python3
"""Render the medallion + CDC SQL for one SAP BDC catalog-linked share.

    python3 render_sql.py <config.yaml> [--out <dir>]

Reads a share config (see config/example.yaml), validates it, and writes
10_control.sql, 20_bronze.sql, 30_silver.sql, 40_gold.sql and 50_ops.sql to
--out (default: ./generated next to the config). No Snowflake connection is
needed, and the output is deterministic: the same config always renders the
same SQL, so the generated files can be reviewed and diffed like code.
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

try:
    import yaml
    from jinja2 import Environment, FileSystemLoader, StrictUndefined
except ImportError:  # pragma: no cover
    sys.exit("pip install -r requirements.txt  (needs pyyaml and jinja2)")

HERE = Path(__file__).resolve().parent
TEMPLATES = HERE.parent / "templates"

CLASSES = {"MASTER", "TRANSACTION", "EMPTY"}
DEFAULT_LAG = {"MASTER": "24 hours", "TRANSACTION": "1 hour", "EMPTY": None}
# Fixed-width, zero-padded text form of a TIMESTAMP, so text order is time order.
CUSTOM_TEXT_FMT = "YYYY-MM-DD\"T\"HH24:MI:SS.FF9"
IDENT = re.compile(r"^[A-Z_][A-Z0-9_$]*$")


def quote_db(name: str) -> str:
    """Leave an upper-case identifier bare; quote anything else exactly once."""
    if name.startswith('"'):
        return name
    return name if IDENT.match(name) else f'"{name}"'


def q(name: str) -> str:
    return name if name.startswith('"') else f'"{name}"'


def sqlstr(value: str) -> str:
    return str(value).replace("'", "''")


def fail(msg: str) -> None:
    sys.exit(f"config error: {msg}")


def build_context(cfg: dict, config_path: Path) -> dict:
    for key in ("target_database", "warehouse", "source", "objects"):
        if key not in cfg:
            fail(f"missing '{key}'")
    s = cfg["source"]
    track = str(s.get("track", "")).upper()
    if track not in {"STANDARD", "CUSTOM"}:
        fail("source.track must be STANDARD or CUSTOM (run readiness.py / profile_share.py to find out)")

    src = {
        "database": quote_db(s["database"]),
        "schema_q": q(s["schema"]),
        "track": track,
        "operation_column": None,
        "load_type_col": None,
        "run_id_col": None,
        "hash_suffix_ok": False,
    }
    if track == "STANDARD":
        flow_hash = str(s.get("flow_hash") or "")
        if not re.fullmatch(r"[0-9a-f]{32}", flow_hash):
            fail("source.flow_hash must be the 32-hex suffix of load_type_<hash>. A share without it "
                 "is probably a COPY of a catalog-linked database - point at the live share instead.")
        src.update(watermark_column="__TIMESTAMP", operation_column="__OPERATION_TYPE",
                   load_type_col=f"load_type_{flow_hash}", run_id_col=f"run_id_{flow_hash}",
                   hash_suffix_ok=True)
    else:
        wm = s.get("watermark_column")
        if not wm:
            fail("custom track needs source.watermark_column (e.g. ZZLASTCHGDATE)")
        src["watermark_column"] = wm

    objects, seen = [], set()
    for raw in cfg["objects"]:
        name = str(raw["name"]).upper()
        if not IDENT.match(name) or name in seen:
            fail(f"object name '{name}' must be a unique upper-case identifier")
        seen.add(name)
        keys = raw.get("keys") or []
        if not keys:
            fail(f"{name}: 'keys' is required - verify the grain with verify_grain.py first")
        cls = str(raw.get("class", "MASTER")).upper()
        if cls not in CLASSES:
            fail(f"{name}: class must be one of {sorted(CLASSES)}")
        nullable = set(raw.get("nullable_keys") or [])
        parts = [f"COALESCE({q(k)}, '~')" if k in nullable else q(k) for k in keys]
        wm = q(src["watermark_column"])
        if track == "STANDARD":
            wm_text, pmax, pmin = wm, f"MAX({wm})", f"MIN({wm})"
        else:
            wm_text = f"TO_VARCHAR({wm}, '{CUSTOM_TEXT_FMT}')"
            pmax = f"TO_VARCHAR(MAX({wm}), '{CUSTOM_TEXT_FMT}')"
            pmin = f"TO_VARCHAR(MIN({wm}), '{CUSTOM_TEXT_FMT}')"
        objects.append({
            "name": name,
            "table": raw["table"],
            "source_fqn": f'{src["database"]}.{src["schema_q"]}.{q(raw["table"])}',
            "key_cols": keys,
            "nullable_keys": sorted(nullable),
            "partition_by": ",\n                                        ".join(parts),
            "cls": cls,
            "target_lag": raw.get("target_lag") or DEFAULT_LAG[cls],
            "flags": raw.get("flags") or [],
            "comment": raw.get("comment"),
            "verified": raw.get("verified"),
            "wm_text_expr": wm_text,
            "poll_max_expr": pmax,
            "poll_min_expr": pmin,
        })

    names = {o["name"] for o in objects}
    ri = None
    if cfg.get("referential_integrity"):
        r = cfg["referential_integrity"]
        master, keys = str(r["master"]).upper(), r["key_columns"]
        children = [str(c).upper() for c in r.get("children", [])]
        for n in [master, *children]:
            if n not in names:
                fail(f"referential_integrity references unknown object {n}")
        ri = {
            "master": master,
            "children": children,
            "key_list": ", ".join(keys),
            "key_select": ", ".join(q(k) for k in keys),
            "join_on": " AND ".join(f"m.{q(k)} = c.{q(k)}" for k in keys),
            "first_key": q(keys[0]),
        }

    target = cfg["target_database"]
    gold = cfg.get("gold") or {}
    return {
        "config_name": config_path.name,
        "T": quote_db(target),
        "T_plain": target.strip('"'),
        "WH": cfg["warehouse"],
        "src": src,
        "objects": objects,
        "ri": ri,
        "gold_dts": gold.get("dynamic_tables", []),
        "gold_template": gold.get("template"),
        "schedule": {
            "capture": (cfg.get("schedule") or {}).get("capture", "15 MINUTE"),
            "assurance": (cfg.get("schedule") or {}).get("assurance", "USING CRON 0 6 * * * UTC"),
        },
    }


GOLD_STUB = """/* ============================================================================
   40  Gold — domain marts (write by hand; see references/gold-patterns.md)
   ----------------------------------------------------------------------------
   No gold.template was set in {config_name}. Gold is where SAP field names
   become business names, and it is domain specific, so it is not generated.

   Rules that still apply here:
     - REFRESH_MODE = INCREMENTAL, stated. No SYSDATE()/CURRENT_* in a DT.
     - Pre-aggregate child objects in CTEs, THEN join to the master.
     - Keep many-to-many relationships as a separate bridge object.
     - Time-relative logic (ages, "days since") goes in a plain VIEW.
   List every Gold dynamic table under gold.dynamic_tables in the config so
   SP_CAPTURE_CYCLE refreshes them when Silver moves.
   ========================================================================== */
USE DATABASE {T};
"""


def render(cfg_path: Path, out: Path) -> list[Path]:
    cfg = yaml.safe_load(cfg_path.read_text())
    ctx = build_context(cfg, cfg_path)
    env = Environment(loader=FileSystemLoader([str(TEMPLATES), str(cfg_path.parent)]),
                      undefined=StrictUndefined, keep_trailing_newline=True,
                      trim_blocks=False, lstrip_blocks=False)
    env.filters["sqlstr"] = sqlstr
    out.mkdir(parents=True, exist_ok=True)
    written = []
    for name in ("10_control", "20_bronze", "30_silver", "50_ops"):
        text = env.get_template(f"{name}.sql.j2").render(**ctx)
        path = out / f"{name}.sql"
        path.write_text(text.rstrip() + "\n")
        written.append(path)
    gold = out / "40_gold.sql"
    if ctx["gold_template"]:
        gold.write_text(env.get_template(ctx["gold_template"]).render(**ctx).rstrip() + "\n")
    else:
        gold.write_text(GOLD_STUB.format(config_name=ctx["config_name"], T=ctx["T"]))
    written.insert(3, gold)
    return written


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("config")
    ap.add_argument("--out", help="output directory (default: <config dir>/generated)")
    a = ap.parse_args()
    cfg = Path(a.config).resolve()
    out = Path(a.out).resolve() if a.out else cfg.parent / "generated"
    for p in render(cfg, out):
        print(f"wrote {p}")


if __name__ == "__main__":
    main()
