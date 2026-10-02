#!/usr/bin/env python3
"""CDC readiness assessment for an SAP BDC catalog-linked share.

Point this at any Snowflake account that has SAP BDC data products linked into it and
it answers one question: can change data capture be built on this share, and by which
mechanism.

    python3 cdc_readiness.py --connection my_account
    python3 cdc_readiness.py --connection my_account --with-scratch-tests
    python3 cdc_readiness.py --connection my_account --report readiness.md

Read-only by default. `--with-scratch-tests` additionally creates a stream and a
dynamic table in a scratch database to prove the mechanisms work end to end, and drops
them; use it when you want proof rather than inference.

Two kinds of data product carry two different change signals, and one account can hold
both, so every object is classified individually:

  standard  SAP generates __OPERATION_TYPE, __TIMESTAMP and load_type / run_id. The
            operation column means deletes can be represented, so this track is
            preferred wherever it exists.
  custom    the pipeline owner exposes a source-system change timestamp such as the SLT
            extension field ZZLASTCHGDATE. There is no operation column, so the source
            signal cannot represent a delete and row counts must be reconciled
            separately. Note this constrains the *signal*, not your options: a dynamic
            table over the share plus a stream on that dynamic table detects deletes on
            either track. See docs/how-it-works.md.

The questions it answers, in the order they matter:

  1  Is there a change signal at all?          and which track does each object use
  2  Can the standard metadata be trusted?     measured, because copies corrupt it
  3  Does the connector merge, or reload?      if it reloads, no watermark can work
  4  Is polling the watermark free?            it is, but only if you do not parse it
  5  Does a change predicate prune?            if not, an incremental cycle costs a full scan
  6  Which native mechanisms are permitted?    stream, CHANGES, change tracking, time travel
  7  Can a dynamic table refresh incrementally? the recommended engine
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone

from conn import Session, fqn, q
from discover import (Obj, StandardSignal, catalog_linked_databases, find_standard_signal,
                      find_watermark, inventory, track_of)

SCRATCH = "CDC_READINESS_SCRATCH"


class Assessment:
    def __init__(self, s: Session, verbose: bool = True):
        self.s = s
        self.v = verbose
        self.findings: list[dict] = []
        self.started = datetime.now(timezone.utc).isoformat(timespec="seconds")

    # ------------------------------------------------------------------ plumbing
    def log(self, msg: str = "") -> None:
        if self.v:
            print(msg, flush=True)

    def record(self, key: str, status: str, detail: str, evidence=None) -> dict:
        f = {"check": key, "status": status, "detail": detail, "evidence": evidence}
        self.findings.append(f)
        mark = {"PASS": "  ok  ", "FAIL": " FAIL ", "WARN": " warn ", "INFO": "  ..  "}[status]
        self.log(f"[{mark}] {key}: {detail}")
        return f

    def status_of(self, key: str) -> str | None:
        return next((f["status"] for f in self.findings if f["check"] == key), None)

    # ---------------------------------------------------------------- 1 discovery
    def discover(self) -> list[Obj]:
        dbs = catalog_linked_databases(self.s)
        if not dbs:
            self.record("share.present", "FAIL",
                        "No catalog-linked databases found. Either BDC Connect has not "
                        "linked anything into this account, or the current role cannot "
                        "see it.")
            return []
        self.record("share.present", "PASS",
                    f"{len(dbs)} catalog-linked database(s): {', '.join(dbs)}",
                    evidence={"databases": dbs})
        objs = inventory(self.s)
        cols = sum(len(o.columns) for o in objs)
        broken = [o for o in objs if o.error]
        self.record("share.inventory", "INFO",
                    f"{len(objs)} objects, {cols} columns",
                    evidence={"objects": [f"{o.database}.{o.schema}.{o.name}" for o in objs]})
        if broken:
            self.record("share.uninitialized", "WARN",
                        f"{len(broken)} of {len(objs)} objects could not be described and are "
                        "excluded from every check below. A share can advertise a table that "
                        "has never been materialised; ask the pipeline owner whether these are "
                        "expected: "
                        + ", ".join(o.name for o in broken[:4])
                        + ("" if len(broken) <= 4 else f", and {len(broken) - 4} more"),
                        evidence={"objects": [{"object": f"{o.database}.{o.schema}.{o.name}",
                                               "error": o.error} for o in broken]})
            objs = [o for o in objs if not o.error]
        return objs

    # -------------------------------------------------------- 2 the change signal
    def change_signal(self, objs: list[Obj],
                      sigs: dict[str, StandardSignal] | None = None) -> dict[str, str]:
        """The custom-track signal: a source-system change timestamp.

        Objects already covered by the standard track are held apart rather than counted
        as missing. On a pure standard-product account there is no extension field to
        find, and reporting that as a failure would be wrong - the account has a better
        signal, not a worse one.
        """
        sigs = sigs or {}
        found: dict[str, str] = {}
        missing: list[str] = []
        covered: list[str] = []
        for o in objs:
            key = f"{o.database}.{o.schema}.{o.name}"
            wm = find_watermark(o)
            if wm:
                found[key] = wm.name
            elif key in sigs:
                covered.append(o.name)
            else:
                missing.append(o.name)
        ev = {"found": found, "objects_without": missing,
              "covered_by_standard_track": covered}
        if not found and covered:
            self.record("signal.watermark", "INFO",
                        f"No custom extension timestamp, and none is needed: all "
                        f"{len(covered)} object(s) carry the standard connector metadata "
                        "instead.", evidence=ev)
        elif not found:
            self.record("signal.watermark", "FAIL",
                        "No replication timestamp found on any object. Without one, a "
                        "watermark engine is impossible and only dynamic tables remain "
                        "viable. Ask the BDC pipeline owner to add a load timestamp.",
                        evidence=ev)
        elif missing:
            self.record("signal.watermark", "WARN",
                        f"Custom extension timestamp on {len(found)} object(s); "
                        f"{len(covered)} covered by the standard track; {len(missing)} with "
                        "no signal of either kind, which must rely on dynamic tables.",
                        evidence=ev)
        else:
            names = sorted(set(found.values()))
            self.record("signal.watermark", "PASS",
                        f"Replication timestamp present on all {len(found)} objects "
                        f"(column: {', '.join(names)})", evidence=ev)
        return found

    # ----------------------------------------------------- 2b the standard-product signal
    def standard_signal(self, objs: list[Obj]) -> dict[str, StandardSignal]:
        """Classify every object by track and report the split.

        An account can hold both kinds of product at once, so this is per object and
        not per account. Where both signals exist the standard one wins: it carries an
        operation column, so it can represent deletes, which the custom track cannot.
        """
        found: dict[str, StandardSignal] = {}
        tracks: dict[str, list[str]] = {"STANDARD": [], "CUSTOM": [], "NONE": []}
        for o in objs:
            key = f"{o.database}.{o.schema}.{o.name}"
            tracks[track_of(o)].append(key)
            sig = find_standard_signal(o)
            if sig:
                found[key] = sig
        ev = {k: v for k, v in tracks.items()}
        n_std, n_cus, n_non = (len(tracks[k]) for k in ("STANDARD", "CUSTOM", "NONE"))
        if not found:
            self.record("signal.standard", "INFO",
                        f"No standard data products found. {n_cus} object(s) carry a "
                        "custom extension timestamp only.", evidence=ev)
            return found
        hashed = sum(1 for s in found.values() if s.hash_suffixed)
        detail = (f"{n_std} object(s) carry connector metadata columns "
                  f"({hashed} with a flow hash), {n_cus} custom-track, {n_non} with no signal.")
        self.record("signal.standard", "PASS" if hashed == n_std else "WARN",
                    detail if hashed == n_std else
                    detail + " Objects without the hash suffix are usually local copies "
                             "rather than live shares; treat their metadata as suspect.",
                    evidence=ev)
        return found

    def standard_semantics(self, objs: list[Obj], sigs: dict[str, StandardSignal]) -> None:
        """Discover what the operation and load-type columns actually contain.

        Deliberately a discovery and not a validation. The authoritative value list for
        __OPERATION_TYPE could not be confirmed from SAP documentation, so asserting an
        enumeration here would be inventing a contract. What the probe can do honestly
        is report the distribution it finds and let the reader compare it against their
        own expectations.

        Every standard object is surveyed and the results unioned, rather than sampling
        the largest one. On the reference tenant the largest object carried only the
        initial snapshot while a smaller one held the deltas, so sampling by size alone
        reported 'no deltas seen' on a share that plainly had them.

        On that reference share only 'L' (initial load) and 'A' (delta, after-image)
        ever appeared. No delete code was observed anywhere, which is why the caveat
        below is stated as unverified rather than as absent.
        """
        targets = self._standard_objects(objs, sigs)
        if not targets:
            self.record("semantics.operation", "WARN", "Skipped: no standard object with rows.")
            return
        ops: dict[str, int] = {}
        loads: dict[str, int] = {}
        per_object = {}
        for o, sig in targets:
            rows = self.s.rows(f"""
SELECT {q(sig.operation.name)} AS OP, COUNT(*) AS N
FROM {o.fqn} GROUP BY 1 ORDER BY N DESC LIMIT 25""")
            d = {("NULL" if r["OP"] is None else str(r["OP"])): int(r["N"]) for r in rows}
            per_object[o.name] = d
            for k, v in d.items():
                ops[k] = ops.get(k, 0) + v
            if sig.load_type:
                for r in self.s.rows(f"""
SELECT {q(sig.load_type.name)} AS LT, COUNT(*) AS N
FROM {o.fqn} GROUP BY 1 ORDER BY N DESC LIMIT 10"""):
                    k = "NULL" if r["LT"] is None else str(r["LT"])
                    loads[k] = loads.get(k, 0) + int(r["N"])

        ranked = dict(sorted(ops.items(), key=lambda kv: -kv[1]))
        codes = [k for k in ranked if k != "NULL"]
        has_delete = any(c.upper().startswith("D") for c in codes)
        self.record("semantics.operation", "INFO",
                    f"Across {len(targets)} object(s): "
                    + ", ".join(f"{k}={v:,}" for k, v in list(ranked.items())[:8])
                    + (". A delete code is present, so deletes can be applied."
                       if has_delete else
                       ". No delete code observed. Either none has occurred yet or this "
                       "product never emits one; until one is seen, treat hard deletes as "
                       "unrepresented and reconcile row counts on a schedule."),
                    evidence={"union": ranked, "per_object": per_object,
                              "delete_code_seen": has_delete})

        if loads:
            ranked_lt = dict(sorted(loads.items(), key=lambda kv: -kv[1]))
            keys = {k.lower() for k in ranked_lt}
            only_initial = keys <= {"initial", "null"}
            self.record("semantics.load_type", "WARN" if only_initial else "PASS",
                        f"Across {len(targets)} object(s): "
                        + ", ".join(f"{k}={v:,}" for k, v in ranked_lt.items())
                        + (". Only an initial load has landed anywhere, so the "
                           "merge-versus-reload question is still open; re-run after the "
                           "next replication cycle." if only_initial else
                           ". Both a snapshot and deltas are present, so the connector "
                           "appends changes rather than reloading."),
                        evidence={"union": ranked_lt})

    def standard_sanity(self, objs: list[Obj], sigs: dict[str, StandardSignal]) -> None:
        """Whether the metadata columns can be trusted at all.

        This check exists because of a measured failure, not a hypothetical one. On the
        reference tenant every object inside the live catalog-linked database was clean,
        and every object that had been copied into an ordinary database had corrupted
        metadata: on one product __OPERATION_TYPE held product titles, on another it
        held customer identifiers, and on a third __OPERATION_TYPE, __TIMESTAMP and
        load_type all held the same value with run_id null.

        Any of those would silently produce a wrong incremental result, so the columns
        are validated before anything is built on them. The thresholds come from the
        contrast between the clean share (2 distinct codes, 1 character) and the corrupt
        copies (17 distinct at 33 characters, 620 distinct at 10).
        """
        checked, bad = [], []
        for o in objs:
            sig = sigs.get(f"{o.database}.{o.schema}.{o.name}")
            if not sig or not o.rows:
                continue
            op, ts = q(sig.operation.name), q(sig.timestamp.name)
            r = self.s.one(f"""
SELECT COUNT(*) AS N,
       COUNT(DISTINCT {op}) AS OP_CARD,
       MAX(LENGTH({op})) AS OP_LEN,
       COUNT({_parse_ts(sig.timestamp.name)}) AS TS_OK,
       SUM(IFF({op} = {ts}, 1, 0)) AS DEGENERATE
FROM {o.fqn}""")
            n = int(r["N"] or 0)
            faults = []
            if int(r["OP_CARD"] or 0) > 12 or int(r["OP_LEN"] or 0) > 2:
                faults.append(f"operation column holds {int(r['OP_CARD'])} distinct values "
                              f"up to {int(r['OP_LEN'])} characters, so it is not a change code")
            if n and int(r["TS_OK"] or 0) == 0:
                faults.append("no value in the timestamp column parses as a timestamp")
            if int(r["DEGENERATE"] or 0) > 0:
                faults.append(f"operation and timestamp hold the same value on "
                              f"{int(r['DEGENERATE']):,} of {n:,} rows")
            if not sig.hash_suffixed:
                faults.append("load_type carries no flow hash")
            rec = {"object": f"{o.database}.{o.schema}.{o.name}", "rows": n,
                   "operation_cardinality": int(r["OP_CARD"] or 0),
                   "operation_max_length": int(r["OP_LEN"] or 0),
                   "timestamps_parsed": int(r["TS_OK"] or 0),
                   "degenerate_rows": int(r["DEGENERATE"] or 0),
                   "hash_suffixed": sig.hash_suffixed, "faults": faults}
            checked.append(rec)
            if faults:
                bad.append(rec)
        if not checked:
            self.record("semantics.trustworthy", "WARN", "Skipped: no standard object with rows.")
            return
        if not bad:
            self.record("semantics.trustworthy", "PASS",
                        f"All {len(checked)} standard object(s) passed: the operation column "
                        "holds short codes, timestamps parse, and no column is duplicated.",
                        evidence={"checked": checked})
        else:
            self.record("semantics.trustworthy", "FAIL",
                        f"{len(bad)} of {len(checked)} standard object(s) have unusable "
                        "metadata. Do not build capture on these until the pipeline owner "
                        "explains them: " + "; ".join(
                            f"{b['object'].split('.')[-1]} ({b['faults'][0]})" for b in bad[:3]),
                        evidence={"checked": checked, "unusable": bad})

    def standard_poll_cost(self, objs: list[Obj], sigs: dict[str, StandardSignal]) -> None:
        """Whether the watermark can be read without paying for a scan.

        __TIMESTAMP arrives as text in ISO-8601 with a comma decimal separator
        (2026-06-15T09:41:59,6426580Z). That format is fixed-width and zero-padded, so
        lexicographic order equals chronological order and MAX() on the raw text is
        answerable from Iceberg manifest statistics alone.

        Parsing it first destroys that property, and the difference is not marginal:
        measured on the reference share, MAX on the raw text scanned 0.000 MB in 2 ms
        while MAX over a parsed expression scanned 0.310 MB in 45 ms. On a large object
        that gap is the entire cost of polling. Hence the rule the SQL follows: compare
        as text, and parse only for display.
        """
        target = self._largest_standard(objs, sigs)
        if not target:
            self.record("cost.poll", "WARN", "Skipped: no standard object with rows.")
            return
        o, sig = target
        col = q(sig.timestamp.name)
        self.s.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE")
        self.s.execute("USE DATABASE SNOWFLAKE")
        try:
            agree = self.s.one(f"""
SELECT {_parse_ts_of(f'MAX({col})')} AS RAW_MAX_PARSED,
       MAX({_parse_ts(sig.timestamp.name)}) AS PARSED_MAX FROM {o.fqn}""")
            self.s.rows(f"SELECT MAX({col}) AS M FROM {o.fqn}")
            raw = self.s.last_query_bytes()
            self.s.rows(f"SELECT MAX({_parse_ts(sig.timestamp.name)}) AS M FROM {o.fqn}")
            parsed = self.s.last_query_bytes()
        finally:
            self.s.execute("ALTER SESSION UNSET USE_CACHED_RESULT")
        rb = float(raw.get("BYTES_SCANNED") or 0)
        pb = float(parsed.get("BYTES_SCANNED") or 0)
        same = str(agree.get("RAW_MAX_PARSED")) == str(agree.get("PARSED_MAX"))
        ev = {"object": o.name, "column": sig.timestamp.name,
              "raw_max_parsed": str(agree.get("RAW_MAX_PARSED")),
              "parsed_max": str(agree.get("PARSED_MAX")),
              "orders_agree": same,
              "raw_scan_mb": round(rb / 1048576, 3),
              "parsed_scan_mb": round(pb / 1048576, 3)}
        if not same:
            self.record("cost.poll", "FAIL",
                        "Lexicographic and chronological order disagree on this column, so "
                        "the raw text cannot be compared directly. Parse it in the poll and "
                        "accept the scan cost.", evidence=ev)
        elif rb == 0 and pb > 0:
            self.record("cost.poll", "PASS",
                        f"MAX on the raw text scanned nothing against {pb/1048576:.3f} MB when "
                        "parsed, and both give the same instant. Poll the raw text.",
                        evidence=ev)
        elif rb == 0:
            self.record("cost.poll", "PASS",
                        "MAX on the raw text is answered from manifest statistics without "
                        "reading data.", evidence=ev)
        else:
            self.record("cost.poll", "WARN",
                        f"MAX on the raw text still scanned {rb/1048576:.3f} MB, so polling is "
                        "not free on this object.", evidence=ev)

    def _baseline(self, o: Obj, exclude: set[str]) -> tuple[str | None, dict, list]:
        """A full-scan byte count to compare a predicate against.

        SUM(LENGTH(col)) rather than COUNT(*) or COUNT(DISTINCT col): both of those can
        be answered from Iceberg manifest statistics without reading data. Even with a
        computed expression a single-valued column can still scan zero bytes - which
        looks like a broken measurement but is correct - so several columns are tried and
        the first that forces a genuine read is kept. Without one there is no baseline
        and the comparison would be meaningless rather than merely imprecise.
        """
        cands = [c.name for c in o.columns
                 if c.is_varchar and c.name.lower() not in exclude][:6]
        probe, full, tried = None, {}, []
        for cand in cands:
            self.s.rows(f"SELECT SUM(LENGTH({q(cand)})) AS N FROM {o.fqn}")
            st = self.s.last_query_bytes()
            tried.append({"column": cand,
                          "mb": round(float(st.get("BYTES_SCANNED") or 0) / 1048576, 2)})
            if float(st.get("BYTES_SCANNED") or 0) > 0:
                probe, full = cand, st
                break
        return probe, full, tried

    def standard_pruning(self, objs: list[Obj], sigs: dict[str, StandardSignal]) -> None:
        """Whether extracting the delta reads only the delta.

        Polling the watermark is free, but that only tells you when to run - not what a
        run costs. This measures the extraction: filtering on __TIMESTAMP as text against
        reading the object whole.

        The bound is taken from the data rather than the wall clock. A fixed lookback
        would select nothing on a share whose last delta is older than the window, and
        an empty result scans nothing, which would be reported as perfect pruning on an
        object that has never been tested. Using the object's own maximum timestamp
        selects the most recent write batch, which is exactly what an incremental cycle
        fetches, and is non-empty by construction.
        """
        target = self._largest_standard(objs, sigs)
        if not target:
            self.record("cost.extract", "WARN", "Skipped: no standard object with rows.")
            return
        o, sig = target
        ts = q(sig.timestamp.name)
        self.s.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE")
        self.s.execute("USE DATABASE SNOWFLAKE")
        try:
            bound = self.s.one(f"SELECT MAX({ts}) AS M FROM {o.fqn}").get("M")
            if not bound:
                self.record("cost.extract", "WARN",
                            f"{o.name}: watermark column is empty on every row.")
                return
            probe, full, tried = self._baseline(
                o, {sig.timestamp.name.lower(), sig.operation.name.lower()})
            if probe is None:
                self.record("cost.extract", "WARN",
                            "No probe column produced a measurable full scan - every candidate "
                            "was answerable from Iceberg statistics alone. Extraction cost "
                            "cannot be compared. This is not a fault in the share.",
                            evidence={"object": o.name, "columns_tried": tried})
                return
            lit = str(bound).replace("'", "''")
            self.s.rows(f"""SELECT SUM(LENGTH({q(probe)})) AS N FROM {o.fqn}
WHERE {ts} >= '{lit}'""")
            delta = self.s.last_query_bytes()
        finally:
            self.s.execute("ALTER SESSION UNSET USE_CACHED_RESULT")

        fb = float(full.get("BYTES_SCANNED") or 0)
        db_ = float(delta.get("BYTES_SCANNED") or 0)
        ratio = (db_ / fb) if fb else None
        ev = {"object": o.name, "watermark": sig.timestamp.name, "bound": str(bound),
              "projected_column": probe, "columns_tried": tried,
              "full_scan_mb": round(fb / 1048576, 3),
              "delta_scan_mb": round(db_ / 1048576, 3),
              "ratio": round(ratio, 6) if ratio is not None else None}
        if ratio is not None and ratio < 0.35:
            self.record("cost.extract", "PASS",
                        f"Extracting the most recent batch scanned {db_/1048576:.3f} MB against "
                        f"{fb/1048576:.3f} MB for a full read - {ratio:.1%}. The text watermark "
                        "prunes.", evidence=ev)
        elif ratio is not None:
            self.record("cost.extract", "WARN",
                        f"Extracting the most recent batch scanned {ratio:.0%} of a full read, so "
                        "an incremental cycle costs about as much as a rebuild. Prefer dynamic "
                        "tables, whose refresh does not depend on this predicate pruning.",
                        evidence=ev)
        else:
            self.record("cost.extract", "WARN",
                        "Could not measure a full scan; bytes scanned reported zero.",
                        evidence=ev)

    # ------------------------------------------- 3 does the connector merge or reload
    def merge_or_reload(self, objs: list[Obj], wms: dict[str, str]) -> None:
        """The decisive test.

        A connector that reloads stamps every row with the latest write time, so a
        watermark cannot separate changed from unchanged. A connector that merges leaves
        historical rows with their original stamp. The signature is the number of
        distinct write days: one means a single load so far or a reload; several with an
        uneven row distribution means a merge.
        """
        target = self._largest_with_watermark(objs, wms)
        if not target:
            self.record("signal.merge_or_reload", "WARN",
                        "Skipped: no object with both a watermark and rows.")
            return
        o, wm = target
        rows = self.s.rows(f"""
SELECT TO_CHAR({q(wm)}, 'YYYY-MM-DD') AS WRITE_DAY, COUNT(*) AS ROWS_WRITTEN
FROM {o.fqn} GROUP BY 1 ORDER BY 1""")
        rows = [r for r in rows if r.get("WRITE_DAY")]
        if not rows:
            self.record("signal.merge_or_reload", "WARN",
                        f"{o.name}: watermark column is empty on every row.")
            return
        total = sum(r["ROWS_WRITTEN"] for r in rows)
        biggest = max(r["ROWS_WRITTEN"] for r in rows)
        concentration = biggest / total if total else 1.0
        ev = {"object": o.name, "write_days": rows, "days": len(rows),
              "largest_day_share": round(concentration, 6)}
        if len(rows) == 1:
            self.record("signal.merge_or_reload", "WARN",
                        f"{o.name}: all {total:,} rows carry a single write day. Either the "
                        "share was loaded once and has not changed, or the connector "
                        "reloads. Re-run after the next refresh to tell these apart.",
                        evidence=ev)
        elif concentration > 0.98:
            self.record("signal.merge_or_reload", "PASS",
                        f"{o.name}: {len(rows)} write days, {concentration:.4%} of rows on "
                        "the initial load and small deltas since. The connector merges, so "
                        "a watermark is viable.", evidence=ev)
        else:
            self.record("signal.merge_or_reload", "WARN",
                        f"{o.name}: {len(rows)} write days but rows are spread evenly "
                        f"(largest day {concentration:.1%}). Confirm this is genuine change "
                        "volume and not a periodic reload.", evidence=ev)

    # ------------------------------------------------------------------ 4 pruning
    def pruning(self, objs: list[Obj], wms: dict[str, str]) -> None:
        """Whether a watermark predicate reads only the changed data.

        Both queries project a data column deliberately. COUNT(*) alone can be answered
        from Iceberg manifest statistics without reading data, which flatters the result
        and hides the real extraction cost.
        """
        target = self._largest_with_watermark(objs, wms)
        if not target:
            self.record("cost.pruning", "WARN", "Skipped: no suitable object.")
            return
        o, wm = target
        candidates = [c.name for c in o.columns
                      if c.is_varchar and c.name.lower() != wm.lower()][:6]
        if not candidates:
            self.record("cost.pruning", "WARN", f"{o.name}: no varchar column to project.")
            return

        # SUM(LENGTH(col)) rather than COUNT(*) or COUNT(DISTINCT col): both of those can
        # be answered from Iceberg manifest statistics without reading data. Even with a
        # computed expression, a single-valued column still scans zero bytes - which looks
        # like a broken measurement but is correct - so try several columns and keep the
        # first that forces a genuine read. Otherwise the comparison has no baseline.
        self.s.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE")
        # Query history needs a real database as context: a catalog-linked database has no
        # usable INFORMATION_SCHEMA, so reading history from inside one returns nothing.
        self.s.execute("USE DATABASE SNOWFLAKE")
        probe_col, full, tried = None, {}, []
        try:
            for cand in candidates:
                self.s.rows(f"SELECT SUM(LENGTH({q(cand)})) AS N FROM {o.fqn}")
                st = self.s.last_query_bytes()
                tried.append({"column": cand,
                              "mb": round(float(st.get("BYTES_SCANNED") or 0) / 1048576, 2)})
                if float(st.get("BYTES_SCANNED") or 0) > 0:
                    probe_col, full = cand, st
                    break
            if probe_col is None:
                self.record("cost.pruning", "WARN",
                            "No probe column produced a measurable full scan - every candidate "
                            "was answerable from Iceberg statistics alone. Pruning cannot be "
                            "compared. This is not a fault in the share.",
                            evidence={"object": o.name, "columns_tried": tried})
                return
            self.s.rows(f"""SELECT SUM(LENGTH({q(probe_col)})) AS N FROM {o.fqn}
WHERE {q(wm)} > DATEADD('hour', -36, CURRENT_TIMESTAMP())""")
            delta = self.s.last_query_bytes()
        finally:
            self.s.execute("ALTER SESSION UNSET USE_CACHED_RESULT")

        fb = float(full.get("BYTES_SCANNED") or 0)
        db_ = float(delta.get("BYTES_SCANNED") or 0)
        ratio = (db_ / fb) if fb else None
        ev = {"object": o.name, "watermark": wm, "projected_column": probe_col,
              "columns_tried": tried,
              "full_scan_mb": round(fb / 1048576, 2),
              "watermark_scan_mb": round(db_ / 1048576, 2),
              "ratio": round(ratio, 6) if ratio is not None else None}
        if fb == 0:
            self.record("cost.pruning", "WARN",
                        "Could not measure a full scan; bytes scanned reported zero.",
                        evidence=ev)
        elif ratio is not None and ratio < 0.02:
            self.record("cost.pruning", "PASS",
                        f"Watermark predicate scans {db_/1048576:,.2f} MB against "
                        f"{fb/1048576:,.2f} MB for a full read - {ratio:.4%}. Near-complete "
                        "file elimination.", evidence=ev)
        elif ratio is not None and ratio < 0.35:
            self.record("cost.pruning", "WARN",
                        f"Watermark predicate scans {ratio:.1%} of a full read. Pruning "
                        "works but is partial; incremental cycles will not be free.",
                        evidence=ev)
        else:
            self.record("cost.pruning", "FAIL",
                        f"Watermark predicate scans {ratio:.1%} of a full read. The column "
                        "is not correlated with file layout, so it cannot prune. Prefer "
                        "dynamic tables, whose refresh does not depend on this.",
                        evidence=ev)

    # -------------------------------------------------------------- 5 capability
    def capability(self, objs: list[Obj]) -> None:
        o = self._largest(objs)
        if not o:
            return
        # Streams. A refusal naming INSERT_ONLY is the expected and useful outcome.
        ok, err = self.s.try_execute(
            f"CREATE OR REPLACE STREAM {SCRATCH}.PROBE.S_CAP ON TABLE {o.fqn}")
        if ok:
            self.record("capability.stream_standard", "PASS",
                        "A standard stream was accepted, which implies this is not an "
                        "externally catalogued Iceberg table. Unusual for BDC; verify.")
        elif err and "INSERT_ONLY" in err.upper():
            self.record("capability.stream_standard", "INFO",
                        "Standard stream refused; INSERT_ONLY required. Expected for an "
                        "Iceberg table on an external catalog.", evidence={"error": err})
        else:
            self.record("capability.stream_standard", "INFO",
                        "Standard stream refused.", evidence={"error": err})

        ok, err = self.s.try_execute(f"""SELECT COUNT(*) AS N FROM {o.fqn}
CHANGES(INFORMATION => DEFAULT) AT(TIMESTAMP => DATEADD('hour', -1, CURRENT_TIMESTAMP()))""")
        self.record("capability.changes_clause", "PASS" if ok else "INFO",
                    "CHANGES clause available." if ok else
                    "CHANGES clause unsupported, as expected on an external catalog.",
                    evidence={"error": err})

        ok, err = self.s.try_execute(f"SELECT METADATA$ACTION FROM {o.fqn} LIMIT 1")
        self.record("capability.stream_metadata", "PASS" if ok else "INFO",
                    "Stream metadata columns resolve." if ok else
                    "Stream metadata columns do not resolve; change tracking is not active.",
                    evidence={"error": err})

        ok, err = self.s.try_execute(f"SELECT METADATA$FILENAME AS F FROM {o.fqn} LIMIT 1")
        self.record("capability.file_metadata", "PASS" if ok else "WARN",
                    "METADATA$FILENAME available, so file-level change detection is possible."
                    if ok else "METADATA$FILENAME unavailable.", evidence={"error": err})

        # Time travel reach, bisected coarsely.
        reach = None
        for hours in (1, 6, 24, 48, 168):
            ok, _ = self.s.try_execute(
                f"SELECT COUNT(*) AS N FROM {o.fqn} AT(OFFSET => -{hours * 3600})")
            if ok:
                reach = hours
            else:
                break
        self.record("capability.time_travel", "INFO",
                    f"Time travel reaches about {reach} hour(s)." if reach else
                    "Time travel unavailable.",
                    evidence={"reach_hours": reach})

    # ---------------------------------------------- 6 dynamic table incrementality
    def dynamic_table(self, objs: list[Obj], wms: dict[str, str]) -> None:
        """The recommended engine. Requested explicitly as INCREMENTAL so Snowflake
        must honour it or refuse - AUTO would silently downgrade to FULL and hide the
        answer."""
        target = self._largest_with_watermark(objs, wms)
        o, wm = target if target else (self._largest(objs), None)
        if not o:
            return
        cols = [c.name for c in o.columns][:6]
        proj = ", ".join(f"{q(c)} AS {_safe(c)}" for c in cols)
        wh = self.s.one("SELECT CURRENT_WAREHOUSE() AS W").get("W")
        if not wh:
            self.record("capability.dynamic_table", "WARN",
                        "No current warehouse, so a dynamic table cannot be tested.")
            return
        dt = f"{SCRATCH}.PROBE.DT_CAP"
        ok, err = self.s.try_execute(f"""CREATE OR REPLACE DYNAMIC TABLE {dt}
  TARGET_LAG = '1 hour' WAREHOUSE = {wh} REFRESH_MODE = INCREMENTAL
AS SELECT {proj} FROM {o.fqn}""")
        if not ok:
            self.record("capability.dynamic_table", "FAIL",
                        "A dynamic table over the share was refused with REFRESH_MODE = "
                        "INCREMENTAL. The watermark path becomes the only option.",
                        evidence={"error": err})
            return
        import time
        time.sleep(3)
        self.s.try_execute(f"ALTER DYNAMIC TABLE {dt} REFRESH")
        time.sleep(5)
        hist = self.s.rows(f"""
SELECT STATE, REFRESH_ACTION, REFRESH_TRIGGER,
       DATEDIFF('millisecond', REFRESH_START_TIME, REFRESH_END_TIME) AS DURATION_MS
FROM TABLE({fqn(SCRATCH)}.INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
       NAME => '{dt}')) ORDER BY REFRESH_START_TIME""")
        actions = [h.get("REFRESH_ACTION") for h in hist]
        no_data = [h for h in hist if h.get("REFRESH_ACTION") == "NO_DATA"]
        ev = {"object": o.name, "refresh_history": hist}
        if no_data:
            ms = no_data[0].get("DURATION_MS")
            self.record("capability.dynamic_table", "PASS",
                        f"Incremental refresh accepted, and a no-change refresh returned "
                        f"NO_DATA in {ms:.0f} ms - Snowflake established that nothing "
                        "changed without recomputing the source.", evidence=ev)
        elif "INCREMENTAL" in actions:
            self.record("capability.dynamic_table", "WARN",
                        "Incremental refresh accepted, but a second refresh did not report "
                        "NO_DATA. Re-check after a source change.", evidence=ev)
        else:
            self.record("capability.dynamic_table", "WARN",
                        f"Refresh actions seen: {actions}.", evidence=ev)

    # ------------------------------------------------------------------- verdict
    def verdict(self) -> dict:
        wm = self.status_of("signal.watermark")
        merge = self.status_of("signal.merge_or_reload")
        prune = self.status_of("cost.pruning")
        dt = self.status_of("capability.dynamic_table")
        present = self.status_of("share.present")
        std = self.status_of("signal.standard")
        trust = self.status_of("semantics.trustworthy")
        poll = self.status_of("cost.poll")
        extract = self.status_of("cost.extract")
        load = self.status_of("semantics.load_type")

        # A usable signal can come from either track. Corrupt standard metadata does not
        # count as one, however plausible the column names look.
        signal_ok = wm == "PASS" or trust == "PASS"

        if present != "PASS":
            v, why = "NOT ASSESSABLE", ["No catalog-linked share is visible to this role."]
        elif trust == "FAIL" and wm != "PASS":
            v = "NOT READY"
            why = ["The standard metadata columns are present but unusable, and there is no "
                   "custom extension timestamp to fall back on. Building capture on these "
                   "columns would produce a silently wrong result.",
                   "Take the failing objects to the BDC pipeline owner before going further."]
        elif dt == "PASS":
            v = "READY"
            why = ["Dynamic tables refresh incrementally over the share and detect "
                   "no-change cheaply. This is the recommended engine and needs no code."]
            if signal_ok and prune == "PASS":
                why.append("A pruning watermark is also available for the exception path - "
                           "non-deterministic transformations and external consumers.")
            elif not signal_ok:
                why.append("No usable watermark, so the exception path is unavailable. "
                           "Everything must be expressible as a dynamic table.")
        elif dt == "FAIL" and signal_ok and prune in ("PASS", "WARN"):
            v = "READY WITH CAVEATS"
            why = ["Dynamic tables were refused, so capture must be built on the "
                   "watermark with a MERGE per object."]
            if trust == "PASS":
                why.append("The standard track carries an operation column, so deletes can "
                           "be applied if the product emits them - check the "
                           "semantics.operation finding, which reports what was actually "
                           "observed rather than what is documented.")
            else:
                why.append("That path cannot detect physical deletes, because the share carries "
                           "no change-mode column. Reconcile row counts on a schedule.")
        elif not signal_ok and dt != "PASS":
            v = "NOT READY"
            why = ["No usable change signal and no incremental dynamic table. There is "
                   "nothing to build on.",
                   "Ask the BDC pipeline owner either to publish these objects as standard "
                   "data products, which carry the connector metadata columns, or to add a "
                   "load timestamp to each object."]
        elif dt == "INFO":
            v = "READY WITH CAVEATS"
            why = ["The change signal is present and usable, but the recommended mechanism was "
                   "not proven: re-run with --with-scratch-tests to confirm that a dynamic "
                   "table refreshes incrementally over this share rather than inferring it."]
            if prune == "PASS":
                why.append("The watermark prunes, so the exception path is available regardless.")
            elif prune == "WARN":
                why.append("Pruning could not be measured cleanly; see the finding below.")
        else:
            v = "READY WITH CAVEATS"
            why = ["Some checks were inconclusive. Read the findings below and re-run "
                   "after the next connector refresh."]

        if std == "PASS" and poll == "PASS":
            why.append("Standard-product objects can be polled for free: MAX() on the raw "
                       "__TIMESTAMP text is answered from Iceberg statistics. Compare it as "
                       "text and parse only for display, or the poll starts scanning.")
        elif std == "WARN":
            why.append("Some objects carry the standard metadata column names without the "
                       "flow hash, which usually means they are local copies rather than live "
                       "shares. Point capture at the catalog-linked database itself.")
        if extract == "WARN":
            why.append("Detecting a change is free but extracting one is not: a watermark "
                       "predicate on the standard track read essentially the whole object. "
                       "Drive refreshes from the poll, but let dynamic tables do the "
                       "recomputation rather than hand-written incremental MERGEs.")
        if trust == "FAIL" and wm == "PASS":
            why.append("Some standard objects have unusable metadata, so route those through "
                       "the custom watermark track instead and treat the operation column as "
                       "absent.")
        if load == "PASS":
            why.append("A snapshot and at least one delta have both landed, so the connector "
                       "appends changes rather than reloading - the precondition for any "
                       "watermark to mean anything.")
        elif merge == "WARN" and load != "PASS":
            why.append("The merge-versus-reload question is unresolved: see the "
                       "signal.merge_or_reload finding. If the connector reloads, a watermark "
                       "cannot separate changed rows from unchanged ones and the incremental "
                       "path costs the same as a full rebuild.")
        return {"verdict": v, "reasons": why}

    # ------------------------------------------------------------------- helpers
    def _largest(self, objs: list[Obj]) -> Obj | None:
        with_rows = [o for o in objs if o.rows]
        if with_rows:
            return max(with_rows, key=lambda o: o.rows or 0)
        return max(objs, key=lambda o: len(o.columns)) if objs else None

    def _largest_with_watermark(self, objs, wms) -> tuple[Obj, str] | None:
        cands = [(o, wms.get(f"{o.database}.{o.schema}.{o.name}")) for o in objs]
        cands = [(o, w) for o, w in cands if w]
        if not cands:
            return None
        return max(cands, key=lambda t: (t[0].rows or 0, len(t[0].columns)))

    def _standard_objects(self, objs, sigs) -> list[tuple[Obj, StandardSignal]]:
        """Every standard-track object that has rows, largest first."""
        out = [(o, sigs.get(f"{o.database}.{o.schema}.{o.name}")) for o in objs]
        out = [(o, s) for o, s in out if s and o.rows]
        return sorted(out, key=lambda t: -(t[0].rows or 0))

    def _largest_standard(self, objs, sigs) -> tuple[Obj, StandardSignal] | None:
        got = self._standard_objects(objs, sigs)
        return got[0] if got else None

    def count_rows(self, objs: list[Obj]) -> None:
        for o in objs:
            try:
                o.rows = int(self.s.one(f"SELECT COUNT(*) AS N FROM {o.fqn}")["N"])
            except Exception:
                o.rows = None


def _safe(name: str) -> str:
    out = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in name).upper()
    return out if out and not out[0].isdigit() else f"C_{out}"


def _parse_ts_of(expr: str) -> str:
    """Parse a BDC __TIMESTAMP expression into a real timestamp.

    The connector writes ISO-8601 with a comma as the decimal separator
    (2026-06-15T09:41:59,6426580Z), which TRY_TO_TIMESTAMP_NTZ rejects outright: it
    returns NULL for every row rather than erroring, so an unguarded cast looks like an
    empty column rather than a format mismatch.
    """
    return f"TRY_TO_TIMESTAMP_NTZ(REPLACE({expr}, ',', '.'))"


def _parse_ts(column: str) -> str:
    return _parse_ts_of(q(column))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--connection", help="named entry in ~/.snowflake/connections.toml")
    ap.add_argument("--with-scratch-tests", action="store_true",
                    help="also create a stream and a dynamic table in a scratch database "
                         "to prove the mechanisms, then drop them")
    ap.add_argument("--report", help="write a markdown report to this path")
    ap.add_argument("--json", help="write the raw findings to this path")
    ap.add_argument("--quiet", action="store_true")
    a = ap.parse_args()

    s = Session(a.connection)
    asm = Assessment(s, verbose=not a.quiet)

    asm.log("SAP BDC change data capture readiness")
    asm.log("=" * 72)
    objs = asm.discover()
    if not objs:
        _emit(asm, a)
        return 2

    asm.count_rows(objs)
    sigs = asm.standard_signal(objs)
    wms = asm.change_signal(objs, sigs)
    if sigs:
        asm.standard_semantics(objs, sigs)
        asm.standard_sanity(objs, sigs)
        asm.standard_poll_cost(objs, sigs)
        asm.standard_pruning(objs, sigs)
    asm.merge_or_reload(objs, wms)
    asm.pruning(objs, wms)

    if a.with_scratch_tests:
        s.execute(f"CREATE DATABASE IF NOT EXISTS {SCRATCH}")
        s.execute(f"CREATE SCHEMA IF NOT EXISTS {SCRATCH}.PROBE")
        try:
            asm.capability(objs)
            asm.dynamic_table(objs, wms)
        finally:
            for stmt in (f"DROP DYNAMIC TABLE IF EXISTS {SCRATCH}.PROBE.DT_CAP",
                         f"DROP STREAM IF EXISTS {SCRATCH}.PROBE.S_CAP",
                         "USE DATABASE SNOWFLAKE",
                         f"DROP DATABASE IF EXISTS {SCRATCH}"):
                s.try_execute(stmt)
            asm.log("\nscratch objects dropped")
    else:
        asm.record("capability.dynamic_table", "INFO",
                   "Not tested. Re-run with --with-scratch-tests to prove the recommended "
                   "mechanism rather than infer it.")

    _emit(asm, a)
    return 0 if asm.verdict()["verdict"].startswith("READY") else 1


def _emit(asm: Assessment, a) -> None:
    v = asm.verdict()
    asm.log("\n" + "=" * 72)
    asm.log(f"VERDICT: {v['verdict']}")
    for r in v["reasons"]:
        asm.log(f"  - {r}")
    payload = {"generated_at": asm.started, "verdict": v, "findings": asm.findings}
    if a.json:
        with open(a.json, "w") as fh:
            json.dump(payload, fh, indent=1, default=str)
        asm.log(f"\nfindings -> {a.json}")
    if a.report:
        with open(a.report, "w") as fh:
            fh.write(_markdown(payload))
        asm.log(f"report   -> {a.report}")


def _markdown(p: dict) -> str:
    v = p["verdict"]
    out = ["# SAP BDC change data capture readiness", "",
           f"**Verdict: {v['verdict']}**", ""]
    out += [f"- {r}" for r in v["reasons"]]
    out += ["", f"Assessed {p['generated_at']}.", "", "## Findings", "",
            "| Check | Status | Detail |", "| --- | --- | --- |"]
    for f in p["findings"]:
        out.append(f"| `{f['check']}` | {f['status']} | {f['detail']} |")

    tracks = next((f.get("evidence") or {} for f in p["findings"]
                   if f["check"] == "signal.standard"), {})
    n_std = len(tracks.get("STANDARD") or [])
    n_cus = len(tracks.get("CUSTOM") or [])

    out += ["", "## Which track applies", "",
            f"- **Standard data products: {n_std} object(s).** SAP generates "
            "`__OPERATION_TYPE`, `__TIMESTAMP` and `load_type` / `run_id`. Poll "
            "`MAX(\"__TIMESTAMP\")` on the raw text - it is answered from Iceberg "
            "manifest statistics, and parsing it before comparing turns a free poll into "
            "a full scan. The format is fixed-width ISO-8601, so text order is "
            "chronological order.",
            f"- **Custom data products: {n_cus} object(s).** Poll the extension "
            "timestamp, typically `ZZLASTCHGDATE`. There is no operation column, so "
            "deletes are invisible on this track and row counts have to be reconciled "
            "separately.", "",
            "Where an object has both, prefer the standard track: only it can represent "
            "a delete.", "",
            "## What to do next", "",
            "If the verdict is READY, configure your Gold layer dynamic tables with",
            "`REFRESH_MODE = INCREMENTAL` stated explicitly and set `TARGET_LAG` per object",
            "class. See `docs/how-it-works.md` and `sql/`.", "",
            "Before building anything on the standard metadata columns, read the",
            "`semantics.trustworthy` finding. Those columns are reliable on a live",
            "catalog-linked share and were found corrupted on every local copy of one on",
            "the reference tenant - holding business data, or duplicating each other. A",
            "corrupt operation column does not fail loudly; it produces a wrong",
            "incremental result quietly.", "",
            "Whatever the verdict, deploy `sql/03_monitoring.sql`. It detects the single",
            "event that invalidates the whole pattern: the connector switching from",
            "incremental merge to full reload.", ""]
    return "\n".join(out)


if __name__ == "__main__":
    sys.exit(main())
