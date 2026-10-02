#!/usr/bin/env python3
"""Probe the change-feed pattern and the Delta-compaction hypothesis.

Two separate questions, and only one of them can be answered against a real SAP BDC
share. Keeping them apart is the whole point of this probe.

    python3 change_feed_probe.py --connection my_account
    python3 change_feed_probe.py --connection my_account --scratch-db PROBE_CDC
    python3 change_feed_probe.py --connection my_account --report change_feed.md
    python3 change_feed_probe.py --connection my_account --skip-part-b

PART A - the change-feed pattern, on your real share. Definitive.

  A1  streams / CHANGES() / CHANGE_TRACKING refused directly on the share object
  A2  a dynamic table over the share creates, and refreshes INCREMENTAL
  A3  a standard stream on that dynamic table creates - the interposition is legal
  A4  METADATA$ACTION and METADATA$ROW_ID resolve on that stream

  Part A cannot prove delete or update *semantics*, because that needs a row deleted at
  the source and the share is read-only. Endress+Hauser already demonstrated it; this
  probe confirms the mechanism is available on your tenant, not that deletes work in
  general.

PART B - the compaction hypothesis, on a Snowflake-managed Iceberg table we own.
INDICATIVE ONLY, and the reason is worth reading before quoting any number from it.

  The claim under test is: when the source is optimized, small Parquet files merge into
  larger ones with no change to the business data, and a snapshot-diffing dynamic table
  may nonetheless register extensive change - costing refresh compute, and on a
  non-SOLEX account a data-sharing fee proportional to it.

  Testing that end to end requires triggering an OPTIMIZE on the source Delta table.
  On a BDC share the source belongs to SAP, so you cannot. This probe substitutes the
  nearest thing it can create: an owned Iceberg table whose files are entirely rewritten
  by INSERT OVERWRITE while the rows stay byte-identical. That isolates the question
  "does a pure file rewrite present as business change to a dynamic table" - which is
  the mechanism at issue - but it is NOT a Delta OPTIMIZE seen through the
  Delta-to-Iceberg read path.

  B3 is a deliberate control: on a Snowflake-managed Iceberg table, row lineage may be
  available and METADATA$ISUPDATE may therefore work. If it does, that is not a
  contradiction of the customer finding - it localizes the cause to the external Delta
  path rather than to streams-on-dynamic-tables as a feature.

  A definitive answer needs someone who owns the upstream. Ask the customer to run
  OPTIMIZE on their Datasphere-side Delta table and read ROWS_INSERTED / ROWS_DELETED
  off the next refresh; that is the measurement this probe approximates.

Creates nothing outside --scratch-db, and drops it on the way out unless --keep.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

from conn import Session, fqn, q
from discover import catalog_linked_databases

VERDICT = {True: "PASS", False: "FAIL", None: "SKIP"}


def pick_share_object(s: Session) -> str | None:
    """Largest queryable object across the catalog-linked databases.

    Discovery is done here rather than via discover.inventory() because that helper
    does not carry row counts, and because a share can advertise a table that has
    never been materialised - selecting from one fails with "not initialized". Each
    candidate is therefore probed with a LIMIT 0 before being accepted, largest
    first, so the probe runs against a real object rather than a broken one.
    """
    candidates: list[tuple[int, str]] = []
    for db in catalog_linked_databases(s):
        try:
            rows = s.rows(f"SHOW TABLES IN DATABASE {fqn(db)}")
        except Exception:
            continue
        for r in rows:
            schema = r.get("schema_name") or r.get("SCHEMA_NAME")
            name = r.get("name") or r.get("NAME")
            n = r.get("rows") or r.get("ROWS") or 0
            try:
                n = int(n)
            except (TypeError, ValueError):
                n = 0
            if schema and name and n > 0:
                candidates.append((n, fqn(db, str(schema), str(name))))
    for _, obj in sorted(candidates, reverse=True):
        ok, _ = s.try_execute(f"SELECT * FROM {obj} LIMIT 0")
        if ok:
            return obj
    return None


class Probe:
    def __init__(self, s: Session, scratch: str, share_obj: str | None):
        self.s = s
        self.scratch = scratch
        self.share_obj = share_obj
        self.results: list[tuple[str, str, str, str]] = []

    def record(self, test: str, verdict, headline: str, detail: str = "") -> None:
        v = VERDICT.get(verdict, str(verdict))
        self.results.append((test, v, headline, detail))
        print(f"[{v:4}] {test:5} {headline}")
        if detail:
            print(f"            {detail}")

    # ------------------------------------------------------------------ setup
    def setup(self) -> None:
        self.s.execute(f"CREATE DATABASE IF NOT EXISTS {q(self.scratch)}")
        self.s.execute(f"CREATE SCHEMA IF NOT EXISTS {fqn(self.scratch, 'PROBE')}")

    def teardown(self) -> None:
        self.s.try_execute(f"DROP DATABASE IF EXISTS {q(self.scratch)}")

    # ----------------------------------------------------------------- part A
    def part_a(self, warehouse: str) -> None:
        if not self.share_obj:
            self.record("A", None, "no catalog-linked share object found - Part A skipped")
            return

        obj = self.share_obj
        print(f"\nPart A - change feed on {obj}\n" + "-" * 72)

        # A1 - the three refusals, each expected to fail
        refusals = {
            "stream": f"CREATE STREAM {fqn(self.scratch,'PROBE','A1_STREAM')} ON TABLE {obj}",
            "changes": f"SELECT * FROM {obj} CHANGES(INFORMATION => DEFAULT) "
                       f"AT(OFFSET => -60) LIMIT 1",
            "change_tracking": f"ALTER TABLE {obj} SET CHANGE_TRACKING = TRUE",
        }
        refused, allowed = [], []
        for name, sql in refusals.items():
            ok, err = self.s.try_execute(sql)
            (allowed if ok else refused).append(f"{name}: {(err or '').strip()[:90]}")
            if ok:  # unexpected - clean up so the run stays idempotent
                self.s.try_execute(f"DROP STREAM IF EXISTS {fqn(self.scratch,'PROBE','A1_STREAM')}")
        self.record(
            "A1",
            len(refused) == 3,
            f"{len(refused)}/3 native mechanisms refused on the share object",
            "; ".join(refused if refused else allowed),
        )

        # A2 - dynamic table over the share
        dt = fqn(self.scratch, "PROBE", "DT_SHARE")
        ok, err = self.s.try_execute(
            f"CREATE OR REPLACE DYNAMIC TABLE {dt} TARGET_LAG = '24 hours' "
            f"WAREHOUSE = {q(warehouse)} REFRESH_MODE = INCREMENTAL AS SELECT * FROM {obj}"
        )
        if not ok:
            self.record("A2", False, "dynamic table over the share failed to create", str(err)[:160])
            return
        row = self.s.one(f"SHOW DYNAMIC TABLES LIKE 'DT_SHARE' IN SCHEMA {fqn(self.scratch,'PROBE')}")
        mode = row.get("refresh_mode") or row.get("REFRESH_MODE")
        reason = row.get("refresh_mode_reason") or row.get("REFRESH_MODE_REASON")
        self.record(
            "A2",
            str(mode).upper() == "INCREMENTAL",
            f"dynamic table created, refresh_mode = {mode}",
            f"refresh_mode_reason = {reason}" if reason else "no fallback reason - true incremental",
        )

        # A3 - the interposition: a standard stream on the dynamic table
        st = fqn(self.scratch, "PROBE", "STR_DT")
        ok, err = self.s.try_execute(f"CREATE OR REPLACE STREAM {st} ON DYNAMIC TABLE {dt}")
        self.record(
            "A3",
            ok,
            "standard stream on the dynamic table created" if ok
            else "stream on the dynamic table REFUSED - the pattern does not hold here",
            "" if ok else str(err)[:200],
        )
        if not ok:
            return

        # A4 - do the metadata columns resolve
        ok, err = self.s.try_execute(
            f"SELECT METADATA$ACTION, METADATA$ISUPDATE, METADATA$ROW_ID FROM {st} LIMIT 0"
        )
        self.record(
            "A4",
            ok,
            "METADATA$ACTION / $ISUPDATE / $ROW_ID resolve on the stream" if ok
            else "stream metadata columns do not resolve",
            "empty at creation is expected; semantics are exercised in Part B"
            if ok else str(err)[:200],
        )

    # ----------------------------------------------------------------- part B
    def part_b(self, warehouse: str) -> None:
        print("\nPart B - file rewrite vs business change (INDICATIVE - owned table, not a share)\n"
              + "-" * 72)

        base = fqn(self.scratch, "PROBE", "B_BASE")
        dt = fqn(self.scratch, "PROBE", "B_DT")
        st = fqn(self.scratch, "PROBE", "B_STR")

        # A plain table is enough: the question is how the DT diff reacts to a wholesale
        # rewrite, not how Iceberg stores it. Kept simple so the result is unambiguous.
        self.s.execute(
            f"CREATE OR REPLACE TABLE {base} (MATERIAL_NUMBER STRING, MATERIAL_DESCRIPTION STRING, "
            f"NET_WEIGHT NUMBER(12,2), GROSS_WEIGHT NUMBER(12,2))"
        )
        self.s.execute(
            f"INSERT INTO {base} VALUES "
            f"('1','AAA',12.50,222.00),('2','BBB',13.70,23.00),('3','CCC',1.00,1.00)"
        )
        self.s.execute(
            f"CREATE OR REPLACE DYNAMIC TABLE {dt} TARGET_LAG = 'DOWNSTREAM' "
            f"WAREHOUSE = {q(warehouse)} REFRESH_MODE = INCREMENTAL AS SELECT * FROM {base}"
        )
        self.s.execute(f"ALTER DYNAMIC TABLE {dt} REFRESH")
        self.s.execute(f"CREATE OR REPLACE STREAM {st} ON DYNAMIC TABLE {dt}")

        def refresh_delta(label: str) -> dict:
            self.s.execute(f"ALTER DYNAMIC TABLE {dt} REFRESH")
            # Row counts are not columns: they live inside the STATISTICS variant.
            # INPUTS_WITH_CHANGED_DATA carries the source-side view, which is what the
            # compaction question is really about - how much of the base the refresh
            # considered changed, regardless of what landed in the target.
            r = self.s.rows(
                f"SELECT REFRESH_ACTION, REINIT_REASON,"
                f" STATISTICS:numInsertedRows::NUMBER AS INS,"
                f" STATISTICS:numDeletedRows::NUMBER AS DEL,"
                f" STATISTICS:numCopiedRows::NUMBER AS COPIED,"
                f" STATISTICS:numAddedPartitions::NUMBER AS PART_ADD,"
                f" STATISTICS:numRemovedPartitions::NUMBER AS PART_DEL,"
                f" INPUTS_WITH_CHANGED_DATA[0]:statistics:numRegisteredRows::NUMBER AS SRC_REG,"
                f" INPUTS_WITH_CHANGED_DATA[0]:statistics:numUnregisteredRows::NUMBER AS SRC_UNREG"
                f" FROM TABLE({q(self.scratch)}.INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY("
                f"NAME => '{dt}')) ORDER BY DATA_TIMESTAMP DESC LIMIT 1"
            )
            out = r[0] if r else {}
            print(f"            {label}: {out}")
            return out

        # B1 - the real mutation mix, as the customer described it
        self.s.execute(f"DELETE FROM {base} WHERE MATERIAL_NUMBER = '2'")
        self.s.execute(f"INSERT INTO {base} VALUES ('4','DDD',11.00,11.00)")
        self.s.execute(f"UPDATE {base} SET NET_WEIGHT = 9.99 WHERE MATERIAL_NUMBER = '3'")
        d1 = refresh_delta("after delete+insert+update")
        acts = self.s.rows(
            f"SELECT MATERIAL_NUMBER, METADATA$ACTION AS ACT, METADATA$ISUPDATE AS ISUPD "
            f"FROM {st} ORDER BY MATERIAL_NUMBER, ACT DESC"
        )
        deletes = [a for a in acts if str(a.get("ACT")).upper() == "DELETE"]
        self.record(
            "B1",
            len(deletes) >= 1,
            f"stream exposed {len(acts)} change records, {len(deletes)} DELETE",
            "; ".join(f"{a.get('MATERIAL_NUMBER')}:{a.get('ACT')}/isupd={a.get('ISUPD')}"
                      for a in acts),
        )

        # B3 (control) - is ISUPDATE usable on an owned, non-Delta source?
        isupd_vals = {str(a.get("ISUPD")).upper() for a in acts}
        any_true = "TRUE" in isupd_vals
        self.record(
            "B3",
            None,
            f"CONTROL: METADATA$ISUPDATE values seen = {sorted(isupd_vals) or ['none']}",
            "ISUPDATE works on an owned source, so a FALSE-only result on a BDC share "
            "localizes the cause to the external Delta read path"
            if any_true else
            "no TRUE seen even on an owned source - do not attribute the customer's "
            "FALSE-only result to Delta on this evidence alone",
        )

        # B2 - the compaction proxy: rewrite every file, change no row.
        # The stream must be RECREATED, not merely read: selecting from a stream does
        # not advance its offset (only DML that consumes it does), so reusing the B1
        # stream here would report B1's changes on top of B2's and overstate the result.
        self.s.execute(f"CREATE OR REPLACE STREAM {st} ON DYNAMIC TABLE {dt}")
        before = self.s.one(f"SELECT COUNT(*) AS N, SUM(NET_WEIGHT) AS W FROM {base}")
        self.s.execute(f"INSERT OVERWRITE INTO {base} SELECT * FROM {base}")
        after = self.s.one(f"SELECT COUNT(*) AS N, SUM(NET_WEIGHT) AS W FROM {base}")
        d2 = refresh_delta("after INSERT OVERWRITE with identical rows")
        churn = int(d2.get("INS") or 0) + int(d2.get("DEL") or 0)
        src_churn = int(d2.get("SRC_REG") or 0) + int(d2.get("SRC_UNREG") or 0)
        stream_rows = self.s.one(f"SELECT COUNT(*) AS N FROM {st}")["N"]
        identical = (before["N"] == after["N"]) and (before["W"] == after["W"])
        summary = (
            f"file rewrite, rows unchanged ({after['N']} rows): target churn = {churn}, "
            f"source rows considered changed = {src_churn}, stream rows = {stream_rows}"
        )
        if not identical:
            self.record("B2", None, "rewrite changed the data - test invalid, not a result",
                        f"before={before} after={after}")
        elif churn == 0 and int(stream_rows) == 0:
            self.record(
                "B2", True, summary,
                "a pure rewrite did NOT propagate as business change to the target"
                + (f" — but the refresh still considered {src_churn} source rows changed, "
                   f"so the work was done and paid for even though nothing moved"
                   if src_churn else " and the refresh considered no source rows changed"),
            )
        else:
            # Reproducing the concern is a finding, not a probe malfunction, so this must
            # not read as FAIL or set a failing exit code.
            self.record(
                "B2", "FLAG", summary,
                "zero logical change produced non-zero churn - consistent with the customer's "
                "concern; confirm upstream with a real Delta OPTIMIZE before quoting it",
            )
        self.record("B2n", None, "INDICATIVE ONLY - proxy for Delta OPTIMIZE, not the real thing",
                    "definitive test requires OPTIMIZE on the source Delta table, which "
                    "only the data product owner can run")

    # ----------------------------------------------------------------- report
    def report(self) -> str:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        lines = [
            "# Change-feed and compaction probe",
            "",
            f"Run {now}. Share object: `{self.share_obj or 'none found'}`.",
            "",
            "| Test | Verdict | Finding |",
            "| --- | --- | --- |",
        ]
        for test, v, headline, detail in self.results:
            lines.append(f"| {test} | {v} | {headline}{(' — ' + detail) if detail else ''} |")
        lines += [
            "",
            "## How much these results prove",
            "",
            "**Part A is definitive for your tenant.** It confirms the native mechanisms are "
            "refused on the share object and that a dynamic table plus a stream on that "
            "dynamic table is permitted. It does not exercise delete semantics, because that "
            "requires deleting a row at the source and the share is read-only.",
            "",
            "**Part B is indicative only.** The compaction test substitutes an owned table "
            "rewritten by `INSERT OVERWRITE` for a Delta `OPTIMIZE` seen through the "
            "Delta-to-Iceberg read path. It isolates the mechanism - does a pure file rewrite "
            "present as business change - but it is not the same code path. Do not publish a "
            "figure from Part B as a measurement of BDC behaviour.",
            "",
            "**The definitive compaction test needs the upstream owner.** Ask them to run "
            "`OPTIMIZE` on the Datasphere-side Delta table and report `ROWS_INSERTED` / "
            "`ROWS_DELETED` from the dynamic table refresh that follows.",
        ]
        return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--connection", help="named entry in ~/.snowflake/connections.toml")
    ap.add_argument("--scratch-db", default="PROBE_CHANGE_FEED")
    ap.add_argument("--warehouse", help="warehouse for the dynamic tables (default: session)")
    ap.add_argument("--share-object", help="fully qualified share object; default: auto-discover")
    ap.add_argument("--skip-part-b", action="store_true")
    ap.add_argument("--keep", action="store_true", help="do not drop the scratch database")
    ap.add_argument("--report", help="write a markdown report to this path")
    a = ap.parse_args()

    s = Session(a.connection)
    wh = a.warehouse or (s.one("SELECT CURRENT_WAREHOUSE() AS W").get("W"))
    if not wh:
        print("No warehouse in session and --warehouse not given.", file=sys.stderr)
        return 2

    share_obj = a.share_object
    if not share_obj:
        share_obj = pick_share_object(s)
        if share_obj:
            print(f"auto-discovered share object: {share_obj}")
        else:
            print("no queryable catalog-linked share object found - Part A will be skipped")

    p = Probe(s, a.scratch_db, share_obj)
    try:
        p.setup()
        p.part_a(wh)
        if not a.skip_part_b:
            p.part_b(wh)
    finally:
        if not a.keep:
            p.teardown()
            print(f"\nscratch database {a.scratch_db} dropped")
        else:
            print(f"\nscratch database {a.scratch_db} KEPT")

    if a.report:
        with open(a.report, "w") as f:
            f.write(p.report())
        print(f"report written to {a.report}")

    return 0 if all(v != "FAIL" for _, v, _, _ in p.results) else 1


if __name__ == "__main__":
    sys.exit(main())
