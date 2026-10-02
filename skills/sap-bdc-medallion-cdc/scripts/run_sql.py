#!/usr/bin/env python3
"""Run generated SQL files against Snowflake, in order, statement by statement.

    python3 run_sql.py --connection <name> generated/10_control.sql [more.sql ...]
    python3 run_sql.py --connection <name> --dir generated            # all *.sql, sorted

Uses the Python connector's script splitter, which understands $$-quoted
procedure bodies (naive semicolon splitting cuts them in half). Stops at the
first failing statement and prints it, so a partial deploy is obvious. Every
generated file is idempotent, so fix and re-run.
"""
from __future__ import annotations

import argparse
import sys
from io import StringIO
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "readiness"))
from conn import _connect  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("files", nargs="*")
    ap.add_argument("--dir")
    ap.add_argument("--connection")
    ap.add_argument("--warehouse", help="USE WAREHOUSE before running")
    ap.add_argument("--role", help="USE ROLE before running (must see unmasked keys; see references/masking.md)")
    a = ap.parse_args()
    files = [Path(f) for f in a.files]
    if a.dir:
        files += sorted(Path(a.dir).glob("*.sql"))
    if not files:
        ap.error("no SQL files given")

    conn = _connect(a.connection)
    if a.role:
        conn.cursor().execute(f"USE ROLE {a.role}")
    if a.warehouse:
        conn.cursor().execute(f"USE WAREHOUSE {a.warehouse}")
    for f in files:
        n = 0
        try:
            for cur in conn.execute_stream(StringIO(f.read_text()), remove_comments=True):
                n += 1
                cur.close()
        except Exception as e:  # report the failing file and stop
            print(f"FAILED {f.name} after {n} statement(s): {' '.join(str(e).split())}")
            sys.exit(1)
        print(f"ok     {f.name}: {n} statement(s)")


if __name__ == "__main__":
    main()
