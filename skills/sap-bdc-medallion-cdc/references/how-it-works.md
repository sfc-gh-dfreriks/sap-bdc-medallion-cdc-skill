> Background reference, from [sap-bdc-cdc](https://github.com/sfc-gh-dfreriks/sap-bdc-cdc). File names such as `sql/05_...` and `probes/` refer to that repository; in this skill the equivalents are `templates/` and `scripts/`.

# How it works

This document covers what is common to both CDC tracks: why the platform refuses standard CDC mechanisms, why dynamic tables with `REFRESH_MODE = INCREMENTAL` are the engine on both tracks, and the connector merge precondition that makes incremental refresh viable. For deployment steps, see [deployment.md](deployment.md).

**Two tracks, two documents.** An SAP BDC catalog-linked share can carry standard data products, custom data products, or both at once. The tracks are not interchangeable - the signals differ, the validation steps differ, and the action required to close the delete gap differs. Classify each object individually before deploying capture.

![Two tracks side by side](https://raw.githubusercontent.com/sfc-gh-dfreriks/sap-bdc-cdc/main/assets/diagrams/D10_delineation.png)

| Track | What SAP provides | Starting document |
| --- | --- | --- |
| Standard | Four connector-generated columns (`__OPERATION_TYPE`, `__TIMESTAMP`, `load_type_<hash>`, `run_id_<hash>`) | [standard-products-cdc.md](standard-products-cdc.md) - a procedure you follow step by step |
| Custom | Only what the pipeline owner chose to include - typically one replication timestamp field | [custom-products-cdc.md](custom-products-cdc.md) - a specification of what to ask for and why |

On both tracks, the engine of choice is dynamic tables with `REFRESH_MODE` pinned to `INCREMENTAL`. The watermark procedure (`sql/04` for the custom track, `sql/05` for the standard track) is the trigger and the exception path; it is not the engine.

---

## The share and its apparent nature

A catalog-linked database created by SAP BDC Connect reports `kind = TABLE` and `is_external = N` on every object inside it. `SHOW TABLES` and `INFORMATION_SCHEMA` agree. Both are incomplete descriptions.

The objects are Iceberg tables whose catalog is managed by SAP, not by Snowflake. Snowflake can read them but does not own their metadata lifecycle. The distinction becomes visible only when you try to attach a change mechanism.

Three behaviours are worth knowing before you start:

- `INFORMATION_SCHEMA.COLUMNS` returns no rows for a catalog-linked database. `DESCRIBE TABLE` is the only route to a column list, one statement per object. There is no bulk alternative.
- Schema names contain a colon (for example `"dp_finance_v2_srv:v1"`) and object names are mixed case. Every reference in SQL must be double-quoted.
- A share can advertise a table that has never been materialised. `DESCRIBE TABLE` on such an object fails with `table 'refresh_table' is not initialized`. That is a property of the share, not a fault in the caller. Tooling must tolerate the per-object failure and continue. The probe handles this already.

---

## What the platform refuses

The three standard CDC mechanisms Snowflake offers are all unavailable **on the share objects themselves**, for one shared reason - the catalog is external:

| You attempt | Error |
| --- | --- |
| `CREATE STREAM ... ON TABLE <share_object>` | *Streams on External Tables or Iceberg tables with an external catalog must have INSERT_ONLY set to true.* |
| `SELECT ... CHANGES(INFORMATION => DEFAULT)` | *CHANGES clause is not supported on external tables or Iceberg tables with an external catalog.* |
| `ALTER TABLE ... SET CHANGE_TRACKING = TRUE` | *change_tracking cannot be set for Iceberg tables unless they use Snowflake catalog.* |

**`SHOW TABLES` reports `change_tracking = ON` on standard data product objects.** This is misleading. Despite that value, both streams (other than `INSERT_ONLY`) and the `CHANGES()` clause are still refused on the reference account with the same errors as on custom-track objects. `change_tracking = ON` in `SHOW TABLES` output does not indicate that Snowflake CDC mechanisms will work on these objects. Do not use it as the basis for a deployment decision.

**INSERT_ONLY stream.** A stream with `INSERT_ONLY = TRUE` is permitted. It creates successfully and positions at the current state, returning zero rows at creation. It sees inserts and updates but cannot detect physical deletes - that requires comparing two source snapshots, which INSERT_ONLY mode does not do. The 14-day staleness window applies: an unread stream expires after that period. The same procedural code required here is also required for the watermark path, but with less capability and an additional staleness risk. There is no situation in which the INSERT_ONLY stream path is preferable to the watermark path.

**Metadata columns.** `METADATA$ACTION`, `METADATA$ISUPDATE`, and `METADATA$ROW_ID` do not resolve **when queried against a catalog-linked object directly**. `METADATA$FILENAME` works and exposes the underlying Parquet file paths. This is a property of the share object, not a dead end - see the interposition below, where `METADATA$ACTION` and `METADATA$ROW_ID` do resolve.

### The interposition that does work: a stream on a dynamic table

Every refusal above is scoped to the share object. A dynamic table built over the share is an ordinary Snowflake object that **you** own, so a standard stream on *it* is legal - and that stream exposes the change feed the share will not.

```sql
-- DT over the share does the snapshot diff
CREATE DYNAMIC TABLE DT_MATERIAL
  TARGET_LAG = '15 minutes' WAREHOUSE = LOAD_WH REFRESH_MODE = INCREMENTAL
AS SELECT * FROM <catalog_linked_db>."<schema>"."material";

-- standard stream on the DT: full metadata, deletes included
CREATE STREAM STR_MATERIAL ON DYNAMIC TABLE DT_MATERIAL;
```

**This closes the delete gap on both tracks, including the custom track.** The dynamic table detects the delete by diffing snapshots; the stream is what lets a consumer *read* it as a change record rather than inferring it from a row-count reconciliation. No operation column is required, so it does not depend on `__OPERATION_TYPE` or on the pipeline owner exposing a change mode.

Field-validated by a customer (Endress+Hauser, SAP Datasphere source) on an SAP data product: with material 2 deleted, 4 inserted and 3 updated, the stream returned the corresponding `INSERT` and `DELETE` records with `METADATA$ROW_ID` populated.

**Independently confirmed on a second tenant** against `COREWORKFORCEDATA_V1."coreworkforcedata"."coreworkforce_standardfields"`, a SuccessFactors data product on a catalog-linked share. All three native mechanisms were refused on the share object with the errors in the table above (`091104`, `091947`, `093654`); a dynamic table over it created with `REFRESH_MODE = INCREMENTAL` and no fallback reason; a standard stream on that dynamic table created; and `METADATA$ACTION`, `METADATA$ISUPDATE` and `METADATA$ROW_ID` all resolved. Reproduce with `probes/change_feed_probe.py`.

**One real limitation: `METADATA$ISUPDATE` is always `FALSE` here.** An update surfaces as a `DELETE` + `INSERT` pair for the same key rather than as a flagged update. The cause is that Snowflake's Delta-to-Iceberg read path does not consume Delta **Row Tracking** (see [Format constraints](#format-constraints-that-shape-all-of-this)), so no row-level lineage is available to mark the pair as an update.

A control measurement supports that attribution rather than assuming it. On a *Snowflake-owned* source table, the same DT-plus-stream construction did flag the update correctly: deleting one row, inserting another and updating a third produced `3:INSERT/isupd=True` and `3:DELETE/isupd=True` alongside `isupd=False` on the genuine insert and delete. So `METADATA$ISUPDATE` is not broken for streams on dynamic tables as a feature - the signal is lost specifically on the external Delta path, which is where the customer observed it.

Do not treat this as an Iceberg-specific defect to be worked around. Snowflake's own guidance on streams on dynamic tables warns that after a dynamic table is reinitialized, previously-updated rows appear as `DELETE` + `INSERT` pairs with `METADATA$ISUPDATE = FALSE` **even in fully native cases**, and that logic depending on that column "can lose or duplicate data without an error." The correct pattern is the same one you would write for a native DT:

```sql
MERGE INTO target t
USING (SELECT *, METADATA$ACTION, METADATA$ISUPDATE FROM STR_MATERIAL) s
  ON t.MaterialNumber = s.MaterialNumber
WHEN MATCHED AND s.METADATA$ACTION = 'DELETE' AND NOT s.METADATA$ISUPDATE THEN DELETE
WHEN MATCHED AND s.METADATA$ACTION = 'INSERT' THEN UPDATE SET ...
WHEN NOT MATCHED AND s.METADATA$ACTION = 'INSERT' THEN INSERT ...;
```

The `DELETE` arm removes the old image and the `INSERT` arm re-adds the new one, so a `DELETE` + `INSERT` pair converges on the correct row whether or not it was flagged as an update. Never make correctness contingent on `METADATA$ISUPDATE`.

`APPEND_ONLY` is not supported for streams on dynamic tables. For insert-only semantics, consume the standard stream and filter on `METADATA$ACTION = 'INSERT'`.

### Format constraints that shape all of this

BDC surfaces Delta-format tables through an Iceberg read path, and that conversion decides what change metadata can exist at all.

| Capability | Status | Consequence here |
| --- | --- | --- |
| Delta **Row Tracking** | not supported on the Delta-to-Iceberg read path | why `METADATA$ISUPDATE` is always `FALSE` |
| Delta **Change Data Feed** (CDF) | not supported (`change data files`, `change metadata`, `DataChange`, `CDC` all unsupported) | no native change feed to read; the DT diff is the substitute |
| Iceberg **row lineage** | requires Iceberg **v3** | unavailable on a Delta-sourced share, so no update flagging |
| Delta **deletion vectors** | readable since the 2026-03-02 release (`minReaderVersion 3`, Delta 4.0.0) | deletes read correctly; **not** exposed as a diff accelerator, so no latency gain |

Two cautions on that last row. Legacy *external tables* over Delta still do not support deletion vectors - only the Iceberg/Delta Direct read path does. And reading deletion vectors is not the same as using them to shortcut the snapshot comparison: a merge-on-read delete is resolved correctly, but the dynamic table still performs its own diff. Whether a given tenant benefits is unverified - measure it rather than assuming.

**Time travel.** `DATA_RETENTION_TIME_IN_DAYS` is fixed at 1 on the catalog-linked objects and cannot be altered. Time travel is unavailable at 36 hours. `AT(OFFSET => ...)` for historical comparisons on source objects beyond 24 hours is not available.

---

## The connector merge precondition

The incremental pattern on both tracks rests on one observable property of the BDC connector: it merges rather than reloads. Historical rows keep their original replication timestamp while new data arrives in new Parquet files. Because the connector writes the replication timestamp in monotonic batches, per-file Iceberg statistics eliminate every file that cannot contain a match when a watermark predicate is applied.

If the connector reloaded on every cycle - writing every row with a fresh timestamp each time - the watermark window would cover the entire object on every run, and incremental refresh would be a full scan by another name.

**This is a measured property, not a documented guarantee.** Verify it on your own tenant before deploying. The query in `sql/00_discover.sql` (step 6) groups rows by write day. The shape to look for is several distinct write days with the bulk of rows on the initial load and small incremental additions since. A single write day covering all rows means the connector either loaded once or reloads fully. Neither supports an incremental watermark.

The connector merge behaviour was confirmed on both reference tenants:

- On the custom-track tenant, `ZZLASTCHGDATE` grouped into 5 distinct write days with 100.0000% of rows on the initial load and small deltas since. Day-2 and day-3 deltas included retroactive updates to rows originally created in prior months, confirming that the connector does not deliver append-only data. A watermark MERGE must handle retroactive updates by matching on the business key, not by appending.
- On the standard-track tenant, `load_type_<hash>` carried `initial` and `delta` values side by side, which is the same conclusion by a more direct route.

A `NOT READY` result on `signal.merge_or_reload` from the readiness probe means the incremental watermark path is not viable on that object. Dynamic tables produce a correct result regardless - they diff consecutive snapshots without depending on the watermark predicate.

---

## REFRESH_MODE = INCREMENTAL: the engine on both tracks

Dynamic tables with `REFRESH_MODE = INCREMENTAL` were verified to accept incremental refresh over catalog-linked shares. A no-change cycle returns `REFRESH_ACTION = NO_DATA` in a few hundred milliseconds without reading any source data.

**State `REFRESH_MODE = INCREMENTAL` explicitly.** Never leave it as `AUTO`. `AUTO` allows Snowflake to silently fall back to `FULL` when it cannot support incremental for a given query. A full refresh produces no error, no warning, and is indistinguishable from an incremental one in the object's apparent behaviour - only the cost differs. Stating `INCREMENTAL` converts a silent cost regression into a deployment-time error.

**The cascade.** A dynamic table in `FULL` mode disables change tracking on itself, which forces every downstream dynamic table that reads it to `FULL` as well. On the custom-track reference tenant, before `REFRESH_MODE` was pinned explicitly, Snowflake chose `FULL` on several tables and cascaded it. After pinning, 10 of 10 Gold dynamic tables ran `INCREMENTAL`.

Confirm the setting used after each table is created:

```sql
SHOW DYNAMIC TABLES IN SCHEMA <L2_DB>.<SCHEMA>;
-- Expected: refresh_mode = INCREMENTAL, refresh_mode_reason IS NULL
```

A non-null `refresh_mode_reason` means Snowflake fell back to `FULL` despite the instruction. Investigate before proceeding.

---

## The four verified query shapes

Dynamic tables with `REFRESH_MODE = INCREMENTAL` were tested over a catalog-linked share across four query shapes (custom track, reference tenant). All four were accepted as incremental, with `refresh_mode_reason = null`. No-change refreshes returned `REFRESH_ACTION = NO_DATA` in the durations shown:

| Shape | Description | NO_DATA duration |
| --- | --- | --- |
| A | Flat projection over a view that casts dates | 348 ms |
| B | Join of two views | 486 ms |
| C | GROUP BY aggregate | 450 ms |
| D | Left join to a pre-aggregated child CTE | 570 ms |

The SQL for each shape is in `sql/02_dynamic_table_engine.sql`. These shapes cover the query patterns that appear most often in a SAP BDC Gold layer and can be used as templates.

---

## What blocks incremental refresh

Any non-deterministic expression inside a dynamic table's defining query forces a full refresh on every cycle, regardless of whether the source has changed. `CURRENT_DATE()`, `CURRENT_TIMESTAMP()`, and `RANDOM()` are the common cases. Month-to-date logic is the most frequent source of this problem in practice.

The correct pattern is to put time-relative logic in a view over the dynamic table rather than inside it:

```sql
-- Do NOT put this inside the dynamic table:
--   WHERE SALESDOCUMENTDATE >= DATE_TRUNC('month', CURRENT_DATE())

-- Define a view over the dynamic table instead:
CREATE OR REPLACE VIEW <L2_DB>.<SCHEMA>.VW_MONTH_TO_DATE AS
SELECT *
FROM   <L2_DB>.<SCHEMA>.DT_SO_LINE
WHERE  SALESDOCUMENTDATE >= DATE_TRUNC('month', CURRENT_DATE());
```

The view is always current, adds no refresh cost, and leaves the dynamic table incrementally refreshable. This pattern is included in `sql/02_dynamic_table_engine.sql`.

Symptom of the problem: `REFRESH_ACTION = FULL` appearing in `VW_REFRESH_AUDIT` in steady state (not just on the first cycle, which is always `INCREMENTAL`). Confirm with `refresh_mode_reason` from `SHOW DYNAMIC TABLES`.

---

## Choosing a mechanism

| | Dynamic table | DT + stream on the DT | Watermark + MERGE | INSERT_ONLY stream |
| --- | --- | --- | --- | --- |
| Inserts | yes | yes | yes | yes |
| Updates | yes | yes, but as a `DELETE`+`INSERT` pair (`ISUPDATE` always `FALSE`) | yes | yes |
| Physical deletes | yes - diffs snapshots | **yes, readable as change records** | custom track: no (no operation column). standard track: structurally possible, unverified - see [standard-products-cdc.md](standard-products-cdc.md) | no - by definition |
| Standard metadata columns | n/a | `METADATA$ACTION`, `METADATA$ROW_ID` resolve | no | no |
| Cheap no-change detection | yes (`NO_DATA`) | yes (inherits the DT) | yes (0 MB from manifest statistics on both tracks) | yes |
| Code required | none | none for capture; a MERGE to apply | task + control table + MERGE | task + MERGE |
| Can feed targets outside Snowflake | no | yes - consume the stream | yes | yes |
| Goes stale if unread | no | yes, 14 days | no | yes, 14 days |
| Blocked by non-deterministic SQL | yes | yes (inherits the DT) | no | no |

**Use dynamic tables on both tracks.** They are the mechanism that reliably detects physical deletes, and they require no code. Add a **stream on the dynamic table** when a consumer needs to *read* those changes as records - which is what makes deletes actionable on the custom track, and what lets you feed a target outside Snowflake without the watermark path. Reserve the watermark path for the two cases dynamic tables cannot serve: a transformation containing a non-deterministic expression, and a target outside Snowflake where you do not want a stream to expire.

Note the one cost of adding the stream: like any stream, it goes stale after 14 days unread, whereas a bare dynamic table does not. If nothing consumes the change feed on a schedule, do not create it.

The watermark's more useful role is as a **trigger** rather than an engine. Polling it is free on both tracks - 0.00 MB on the custom track, 0.000 MB on the standard track - so a task can check whether the source moved and refresh a dynamic table only when it did, instead of refreshing on a fixed schedule and discovering nothing changed.

Regardless of mechanism, schedule row-count reconciliation between source and target for every monitored object on both tracks. Physical deletes are invisible on the custom-track watermark path - if that is the gap you are trying to close, prefer a stream on the dynamic table over reconciliation, which tells you a divergence exists but not which rows caused it. On the standard track, no delete code was observed on the reference share; their presence on any specific tenant and product is unverified.

**Measured risk: a file rewrite costs a full reload.** When a source table is optimized, small files are merged into larger ones with no change to the business data. A snapshot-diffing dynamic table does not recognise that as a no-op - it registers every row as deleted and reinserted. Measured on this account:

| Event on a 5,000-row source | Target churn | Rows copied | Source rows considered changed |
| --- | --- | --- | --- |
| One genuine single-row update | 2 (1 insert, 1 delete) | 4,999 | 10,000 |
| Zero logical change, all files rewritten | **10,000** (5,000 insert, 5,000 delete) | 0 | 10,000 |

The rewrite cost is `2 x row count` - every row deleted and reinserted - and it scales with the table, not with the change: a 3-row table produced churn 6, a 5,000-row table produced churn 10,000. Against the single real update on the same table that is a factor of 5,000. A stream on the dynamic table emits the whole table as change records, so any downstream MERGE re-applies every row.

Two honest caveats on those figures. They were produced with `INSERT OVERWRITE` on a Snowflake table, which is a faithful proxy for the *mechanism* - the refresh reported partitions removed and re-added, so it is genuine compaction - but it is not a Delta `OPTIMIZE` seen through the Delta-to-Iceberg read path. And `INSERT OVERWRITE` rewrites every file, whereas a real optimize may touch only a subset, so treat `2 x rows` as the upper bound for a full rewrite rather than the expected cost of every optimize.

The consequence is the same either way: on a non-SOLEX account the data-sharing fee is metered on the compute this consumes, so an upstream optimize you do not control can bill like a full reload while changing nothing. Before committing to a tight `TARGET_LAG` on a table that is regularly optimized, ask the data product owner how often it happens. `probes/change_feed_probe.py` reproduces the measurement.

---

## Freshness measurement

Use `SYSDATE()` for the current-time reference in any freshness or lag calculation. `CURRENT_TIMESTAMP()` is session-timezone-aware; on a UTC-7 session it reported a real 109-minute lag as -311 minutes when mixed with a UTC watermark. `SYSDATE()` returns UTC as `TIMESTAMP_NTZ` regardless of session timezone. This applies to both tracks.

### Arrival latency is not a constant, and it is not all yours

A change written in SAP Datasphere does not become visible in Snowflake on a predictable interval. One customer measured the same write appearing almost immediately in some cases and after roughly **20 minutes** in others.

That range is not explained by the documented polling defaults. A catalog-linked database polls the remote catalog on `SYNC_INTERVAL_SECONDS` (default 30 seconds), and Delta-based automated refresh polls storage on `REFRESH_INTERVAL_SECONDS` (also 30 seconds by default). A 20-minute delay therefore points somewhere other than steady-state Snowflake polling - most likely the SAP-side Delta-to-Iceberg metadata conversion, or a refresh interval configured well above the default.

**Attribute the delay before escalating it**, using `SYSTEM$AUTO_REFRESH_STATUS`. It returns `lastSnapshotTime` - when the snapshot was created in the external catalog - alongside the time Snowflake successfully processed it. The gap between those two values is Snowflake-side; anything before `lastSnapshotTime` is upstream of Snowflake and belongs in a conversation with SAP. `SHOW ICEBERG TABLES` exposes the same status per object in its `auto_refresh_status` column.

This matters directly for `TARGET_LAG`. A dynamic table cannot be fresher than its source, so setting lag below the *arrival* latency buys nothing and pays for refresh cycles that find no changes. Measure arrival latency first, and treat its upper bound - not its median - as the floor for `TARGET_LAG`.

---

## Open questions

These items were not resolved in the reference measurement windows. They are stated here as open, not settled.

**1. The authoritative `__OPERATION_TYPE` value enumeration (standard track).**

Only `L` (initial load) and `A` (delta after-image) were observed on the reference standard-track share across all non-empty objects. Whether a standard data product ever emits a delete code is unknown. The SAP Help documentation for this column is JavaScript-rendered and could not be retrieved; a web search surfaced only the SAP Datasphere BigQuery adapter documentation, which covers a different target system and must not be cited as authority for this. The probe discovers and reports the actual distribution per tenant rather than assuming an enumeration. See [gaps-and-asks.md](gaps-and-asks.md) for the ask to SAP.

**2. What a changed-source dynamic table refresh produces on a production-scale custom-track object.**

On the standard track, `REFRESH_ACTION = INCREMENTAL SUCCEEDED` was observed after a watermark advance on a 3,355-row table. The equivalent on a multi-million-row custom-track object - `REFRESH_ACTION = INCREMENTAL` with `ROWS_INSERTED` matching the connector delta count - was not observed in the available window. After your next connector write on the custom track, query `VW_REFRESH_AUDIT` and record the result. If `REFRESH_ACTION = FULL` appears, the measured fallback is the watermark path in `sql/04_watermark_exception_path.sql`.

Neither of these gaps changes the recommendation. The design is not contingent on either being settled in a particular way.
