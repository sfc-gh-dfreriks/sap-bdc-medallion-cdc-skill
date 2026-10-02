> Background reference, from [sap-bdc-cdc](https://github.com/sfc-gh-dfreriks/sap-bdc-cdc). File names such as `sql/05_...` and `probes/` refer to that repository; in this skill the equivalents are `templates/` and `scripts/`.

# Gaps and asks

What incremental capture from an SAP BDC catalog-linked share solves, what it does not solve, known limitations, and the changes to the platform and data products that would improve the pattern.

---

## What capture solves and what it does not

These two complaints arrive together. They require different conversations with different owners.

| Complaint | Category | Solved here? |
| --- | --- | --- |
| "The data is hours or a day behind" | Freshness - the consuming layer re-reads the entire share on a schedule because nothing tells it what changed | Yes. Dynamic tables with incremental refresh detect no-change in a few hundred milliseconds and read only changed rows otherwise. |
| "The ledger has no amounts" | Completeness - a field or object was never included in the SAP data product | No. No refresh frequency fixes a column that was never shared. Delivering fifteen-minute freshness on a ledger with no amount columns answers the wrong question convincingly. |
| "There is no customer master" | Completeness - an entire object is absent from the share | No. The data product must be extended on the SAP side. |

Capture is a platform mechanism layered over the share. The content of the share is a data product decision made on the SAP side. Before investing in deployment, confirm that the objects and fields the consuming team actually needs are present in the share.

---

## Limitations

### Standard track: operation column value enumeration not confirmed

**Severity: high.**

`__OPERATION_TYPE` exists on standard data product objects and its presence is the primary reason to prefer the standard track over the custom track. However, the authoritative value enumeration could not be confirmed. On the reference share (CUSTOMER_V1, 67,480 rows across 5 non-empty objects), only two values were observed: `L` (initial load) and `A` (delta after-image). No delete code appeared anywhere.

Whether this product ever emits a delete code is unverified. The MERGE exception path in `sql/05_standard_product_track.sql` includes a commented DELETE branch that activates if a delete code is observed, and requires the count reconciliation query regardless. Do not assume the absence of observed delete codes means deletes will never arrive; schedule reconciliation as if they might.

### Standard track: metadata corruption on local copies

**Severity: high.**

The connector metadata columns (`__OPERATION_TYPE`, `__TIMESTAMP`, `load_type`, `run_id`) were clean inside every live catalog-linked database examined. Every local copy of a standard product on the same account carried corrupted metadata: operation columns holding media titles or customer identifiers, all three metadata columns holding the same value on every row, `RUN_ID` null throughout. The corruption propagates into downstream medallion layers. None of it fails loudly; it produces wrong incremental results without raising any error.

The practical rule is to point capture at the catalog-linked database itself, never at a copy of it. Run `SP_VALIDATE_STANDARD` before scheduling anything. The `load_type_<hash>` column name carries the 32-character hex flow hash on a genuine live share; plain `LOAD_TYPE` without the suffix is a reliable marker that you are reading a copy.

### Standard track: `__TIMESTAMP` arrives as TEXT with a comma decimal separator

**Severity: medium.**

The format is `2026-06-15T09:41:59,6426580Z` - ISO-8601 with a comma in place of the fractional seconds decimal point. `TRY_TO_TIMESTAMP_NTZ` does not error on this format; it returns NULL for every row. An unguarded cast looks like an empty column rather than a format mismatch. The correct parse requires `REPLACE(col, ',', '.')` first. For comparison and polling, no parse is needed: the format is fixed-width and zero-padded, so lexicographic order is chronological order, and `MAX()` over the raw text is answered from Iceberg manifest statistics at zero scan cost.

### Physical delete detection on the custom-track watermark path

**Severity: high.**

The custom-track share carries no change-mode column. There is no `ODQ_CHANGEMODE`, `RECORDMODE`, or `UPDKZ`. A row that disappears from the source leaves no trace in the data. The watermark window will never encounter it.

On the watermark path, the only available detection is a count comparison between source and target (`VW_DELETE_RECONCILIATION` in `sql/04_watermark_exception_path.sql`). A persistent positive divergence means rows were deleted upstream. Identifying which rows were deleted is impractical; rebuilding the target is the recommended response.

Dynamic tables handle physical deletes correctly by diffing consecutive source snapshots. This is the primary reason dynamic tables are the recommended path.

### `change_tracking = ON` is misleading on standard data product objects

**Severity: low - operationally confusing.**

`SHOW TABLES` reports `change_tracking = ON` on standard data product objects. Despite this, both streams (other than `INSERT_ONLY`) and the `CHANGES()` clause are still refused on the reference account with the same errors as on custom-track objects. The `ON` value does not confirm that Snowflake CDC mechanisms will work; it should not be used as the basis for a deployment decision.

### Watermark column name varies by pipeline implementation (custom track)

**Severity: medium.**

The replication timestamp on the custom track is a customer extension field added to the SAP source during SLT pipeline configuration. Its name - `ZZLASTCHGDATE` on the reference tenant - is set per BDC pipeline implementation. It may differ between data products on the same tenant, or be absent on a BDC deployment that was not configured to include it. The readiness assessment discovers the column name; do not assume a name.

### Grain not declared on catalog-linked tables

**Severity: medium.**

Catalog-linked tables carry no primary key constraint. The correct merge key for some SAP objects is not obvious from the column names. Purchase order history (EKBE) requires a seven-part key including `AccountAssignmentNumber` and `PurchasingHistoryCategory`. On a five-part key the table appears to hold duplicates, and de-duplicating on that key silently discards real goods movements without any error. Confirm the grain before writing any MERGE statement. This applies to both tracks.

### Uninitialised objects cannot be described

**Severity: low - operationally inconvenient.**

A share can advertise a table that was never materialised. `DESCRIBE TABLE` on such an object fails with `table 'refresh_table' is not initialized`. This is a property of the share rather than a fault in the caller. Tooling must tolerate the per-object failure and continue; the readiness probe (`cdc_readiness.py`) does this already. The object is recorded in the survey with its error, and the rest of the account-wide assessment continues.

### `INFORMATION_SCHEMA.COLUMNS` returns no rows

**Severity: low - operationally inconvenient.**

Column discovery requires `DESCRIBE TABLE`, one statement per object. There is no bulk alternative. On a share with many objects, this is a scripting problem rather than a blocker, but it prevents using standard information-schema tooling for documentation or impact analysis.

### Schema and object names require quoting

**Severity: low.**

Schema names contain a colon (for example `"dp_finance_v2_srv:v1"`). Object names are mixed case. Every reference in SQL must be double-quoted. This is a consistent rule once known, but it causes confusing errors on first encounter and must be applied in every tool that generates SQL against the share.

### Non-deterministic expressions block incremental refresh

**Severity: medium - design constraint.**

`CURRENT_DATE()`, `CURRENT_TIMESTAMP()`, and `RANDOM()` inside a dynamic table's defining query force a full refresh on every cycle. Month-to-date and rolling-window calculations are the common cases. The pattern is to put time-relative logic in a view over the dynamic table. This is a design constraint, not an error, but it requires knowing the rule before writing the query.

### Parquet compaction degrades watermark pruning

**Severity: low - latent.**

The watermark's near-zero scan cost on the custom track depends on per-file Iceberg statistics spanning a narrow range. If SAP compacts the underlying Parquet files - merging many small files into fewer large ones - the per-file ranges widen and file elimination degrades toward a full scan. The file count monitor in `sql/03_monitoring.sql` detects this. If it fires, re-run the pruning probe in `sql/00_discover.sql` to measure the new scan ratio. Dynamic table refreshes are unaffected because they do not depend on the watermark predicate. Predicate pruning on the standard track was not demonstrated on the reference objects (single micropartition), so compaction risk there is not assessed.

### Time travel limited to 24 hours

**Severity: low.**

`DATA_RETENTION_TIME_IN_DAYS` is fixed at 1 on the catalog-linked share and cannot be altered. Time travel is unavailable at 36 hours. This rules out `AT(OFFSET => ...)` for historical comparisons on the source objects beyond 24 hours.

### INSERT_ONLY stream expires if unread

**Severity: low (relevant only if adopting the stream path).**

An INSERT_ONLY stream expires after 14 days if not consumed. A dynamic table does not have a staleness window. This is one of several reasons the stream path is not recommended.

---

## Risks

| Risk | How it would present | Detection |
| --- | --- | --- |
| BDC connector switches from merge to reload | Dynamic table refreshes become as expensive as a full rebuild every cycle; no error is raised. Watermark procedure processes every row every cycle. Neither path produces incorrect results; cost and lag increase. | `VW_RELOAD_DETECTION.CAPTURE_MODE = 'FULL RELOAD DETECTED'`; hourly alert in `AL_CAPTURE_HEALTH`. |
| Non-deterministic expression enters a dynamic table query | `REFRESH_ACTION` becomes `FULL` on every cycle; cost rises silently. No error is raised. | `VW_REFRESH_AUDIT.UNEXPECTED_FULL_REFRESH = TRUE`; `SHOW DYNAMIC TABLES` exposes `refresh_mode_reason`. |
| Connector stops writing | Dynamic table continues to report `NO_DATA` and appears healthy. Downstream consumers receive stale data with no signal. | `VW_RELOAD_DETECTION.CAPTURE_MODE = 'CONNECTOR STALLED'` when `LAST_WRITE` exceeds the staleness threshold. |
| Parquet compaction widens file ranges | Watermark predicate scan cost rises toward a full scan (custom track); no error is raised. Dynamic tables are unaffected. | `VW_CAPTURE_EXCEPTIONS` `FILE_COUNT_DROP` row; file count drops below 70% of the baseline. |
| Wrong merge key chosen (either track watermark path) | Duplicate rows in the target, or real records silently discarded. No constraint violation is raised. | Row count comparison between source and target; business-level data quality audits. |
| Standard track metadata corruption enters the pipeline | Incremental merge produces wrong results silently; `__OPERATION_TYPE` does not contain change codes. | `SP_VALIDATE_STANDARD` / `VW_STANDARD_OPERATION_MIX`; re-run validation whenever the share is re-linked or a product is re-published. |

---

## Asks for SAP

These changes to BDC data products would make the pattern more reliable, close the delete gap, and reduce the risk of deployment errors.

### 1. Authoritative enumeration and contract for `__OPERATION_TYPE`

The value enumeration for `__OPERATION_TYPE` could not be confirmed from available documentation. On the reference share, only `L` (initial load) and `A` (delta after-image) were observed. The specific open questions are:

- What is the full set of valid values, and what does each mean?
- Is a delete code ever emitted, and if so, what is it?
- Is the set stable across data product versions and connector releases?

Without an authoritative answer, the MERGE exception path cannot safely omit the DELETE branch, and reconciliation is required on every schedule cycle rather than as a precaution. A machine-readable contract - or at minimum a versioned reference document per data product type - would close this gap.

### 2. `__TIMESTAMP` as a real timestamp, not TEXT

`__TIMESTAMP` arrives as `TEXT` with a comma decimal separator (`2026-06-15T09:41:59,6426580Z`). `TRY_TO_TIMESTAMP_NTZ` silently returns NULL on this format without the `REPLACE` workaround. Delivering it as `TIMESTAMP_NTZ` or as ISO-8601 with a standard period separator would eliminate a silent data-loss risk and allow the column to be used directly in typed comparisons.

### 3. Clarification on `change_tracking = ON`

`SHOW TABLES` reports `change_tracking = ON` on standard data product objects, but streams (other than `INSERT_ONLY`) and the `CHANGES()` clause are still refused with standard external-catalog errors. A clear statement of what `change_tracking = ON` means on these objects - and whether it represents a future capability rather than a current one - would prevent misinterpretation during deployment.

### 4. A guaranteed load timestamp on every object (custom track)

The replication timestamp on the custom track is a customer extension field. It is absent on some deployments and may vary in name between data products on the same tenant. A first-class `_BDC_LOAD_TS` column with a guaranteed name on every shared object would remove the discovery step, make the readiness assessment unnecessary, and give all consumers a consistent watermark predicate.

### 5. A change-mode column on every custom-track object

Without a column that distinguishes insert, update, and delete at row level, physical deletes are undetectable on the custom-track watermark path. An `_BDC_CHANGE_MODE` column - equivalent to `RECORDMODE` in classic SAP delta queues - would close this gap for the watermark path without requiring dynamic table snapshot diffing.

### 6. Documented grain and primary key per object

Catalog-linked tables carry no primary key constraint. For most objects, the business key is published in SAP documentation. For several it is not obvious. A machine-readable key specification in the data product metadata, or a supplementary reference document covering every object in each data product, would prevent silent merge key errors. This applies to both tracks.

### 7. Guidance on uninitialised objects

A share can advertise a table (`refresh_table` was observed) that has never been materialised and cannot be described. The current behaviour - `DESCRIBE TABLE` fails with `table 'X' is not initialized` - is recoverable if the caller handles it, but it is not documented. Guidance on what `refresh_table` represents, whether it is expected to initialise, and how to detect the condition programmatically would help consumers build robust account-wide surveys.

---

## Asks for Snowflake product management

These asks are derived from the platform refusals documented in [how-it-works.md](how-it-works.md#what-the-platform-refuses). Each is a direct consequence of a specific error message.

### 1. `CHANGES()` clause on externally catalogued Iceberg tables

Currently refused with: *CHANGES clause is not supported on external tables or Iceberg tables with an external catalog.*

A read-only `CHANGES()` implementation that reads Iceberg snapshot metadata - without requiring Snowflake to own or write to the catalog - would give consumers a standard CDC interface. The Iceberg format already stores snapshot history; the information required to compute a change set is present in the catalog.

### 2. Change tracking on externally catalogued Iceberg tables

Currently refused with: *change_tracking cannot be set for Iceberg tables unless they use Snowflake catalog.*

Even a read-only implementation that consumes existing Iceberg snapshot history, rather than writing CDC metadata, would allow streams to expose `METADATA$ACTION` and `METADATA$ISUPDATE` to consumers **directly on the share**, removing the need to materialize a dynamic table first.

**Partly superseded — reduced in priority.** The delete-detection half of this ask is already achievable today: a dynamic table over the share plus a standard stream on that dynamic table yields working `METADATA$ACTION` and `METADATA$ROW_ID`, including deletes, on both tracks. Field-validated with a customer (Endress+Hauser). See [how-it-works.md](how-it-works.md#the-interposition-that-does-work-a-stream-on-a-dynamic-table).

What remains genuinely unmet is narrower, and worth stating precisely:

- **Update identification.** `METADATA$ISUPDATE` is always `FALSE` on this path, so an update is indistinguishable from a delete-plus-insert of the same key. The cause is that the Delta-to-Iceberg read path does not consume Delta **Row Tracking**. Consuming it, or Iceberg v3 row lineage where the source provides it, would close this.
- **Avoiding the materialization.** The interposition requires a dynamic table, which means storage and refresh compute the consumer would not otherwise pay for, plus a 14-day stream staleness window. Direct change tracking would remove both.

### 2a. Delta Change Data Feed support

The customer ask, stated in their words: support the Delta Lake **Change Data Feed (CDF)** API. CDF writes changes to separate files and identifies updates unambiguously, rather than deriving them from file-version comparison.

The current Delta-to-Iceberg read path explicitly does not support `Row Tracking`, `change data files`, `change metadata`, `DataChange`, `CDC`, or protocol evolution. Two consequences follow that a snapshot-diff approach cannot address:

1. **Updates cannot be identified** — the `METADATA$ISUPDATE` gap above is a direct symptom.
2. **Optimize/compaction bills like a full reload.** When a Delta table is optimized, small Parquet files merge into larger ones with no business-data change. A snapshot comparison does not recognise that as a no-op. **Measured:** on a 5,000-row source, a rewrite with zero logical change produced 10,000 rows of target churn (5,000 inserted, 5,000 deleted, none copied) against 2 rows of churn for a genuine single-row update on the same table - a factor of 5,000. The cost is `2 x row count` and scales with the table rather than the change. On a non-SOLEX account the data-sharing fee is metered on the compute that consumes, so an upstream optimize the consumer does not control can bill like a full reload while changing nothing. A native change feed captures actual row changes and is largely immune. *Measured with `INSERT OVERWRITE` on a Snowflake table as a proxy for the mechanism - genuine partition consolidation, but not a Delta `OPTIMIZE` through the Delta-to-Iceberg read path; see how-it-works.md for the caveats.*

Deletion vectors are worth separating from this ask, because they are already supported for reads: as of the 2026-03-02 release Snowflake reads Delta-based Iceberg tables containing deletion vectors (`minReaderVersion 3`, Delta 4.0.0). What is *not* available is any way to exploit them as a diff accelerator — a merge-on-read delete is resolved correctly, but the dynamic table still performs a full comparison, so the latency benefit the format offers is not passed through.

### 3. Longer time travel on catalog-linked shares

`DATA_RETENTION_TIME_IN_DAYS` is fixed at 1 on the linked objects and cannot be altered. Seven-day time travel would support historical queries, some recovery scenarios, and point-in-time snapshot comparisons that are currently impossible without the consumer copying the data.

### 4. A first-class change feed for catalog-linked databases

The current options are INSERT_ONLY streams (no deletes, 14-day expiry, no standard metadata columns), dynamic tables (full snapshot diff, no direct stream interface), and a dynamic table with a stream on it (working deletes and metadata, at the cost of materializing the data and with no update flag). A purpose-built change feed that reads Iceberg snapshot diffs and exposes them as a standard Snowflake stream - without requiring the consumer to own the catalog or to materialize a copy - would be the complete long-term answer. The items above are partial mitigations; this is what they collectively point toward.

Two additions to the requirement, both from customer feedback:

- It must **identify updates**, not just emit delete/insert pairs. This is the capability gap that the DT-plus-stream interposition cannot close at all.
- It should be **insensitive to physical file reorganization**, so that Delta optimize/compaction does not present as business change.

### 5. Predictable arrival latency, and a way to attribute it

A change written in SAP Datasphere becomes visible in Snowflake anywhere between near-immediately and roughly 20 minutes later, per customer measurement. Documented polling defaults do not explain that spread: catalog-linked databases sync on `SYNC_INTERVAL_SECONDS` (default 30s) and Delta-based auto-refresh polls on `REFRESH_INTERVAL_SECONDS` (default 30s).

`SYSTEM$AUTO_REFRESH_STATUS` already lets a consumer separate the Snowflake-side component (the gap between `lastSnapshotTime` and when Snowflake processed it) from whatever happens upstream. What is missing is documentation of the end-to-end path - specifically whether the SAP-side Delta-to-Iceberg metadata conversion is event-driven or scheduled - so that a customer can set `TARGET_LAG` against a known bound rather than an observed worst case. This is partly a question for SAP rather than Snowflake, and should be put to both.

---

## Open questions

Two things were not observable in the reference measurement windows. They are stated here as open, not settled. Both appear in [how-it-works.md](how-it-works.md#open-questions); they are repeated here because their resolution affects the confidence level of the pattern.

**1. The authoritative `__OPERATION_TYPE` value enumeration (standard track).**

Only `L` and `A` were observed. Whether the product ever emits a delete code is unknown. The probe discovers the actual distribution per tenant rather than assuming it. Resolution requires confirmation from SAP per data product type.

**2. What a changed-source dynamic table refresh produces on a production-scale custom-track object.**

On the standard track, `INCREMENTAL SUCCEEDED` was observed after a watermark advance (3,355-row table, 5,241 ms). The equivalent on a multi-million-row custom-track object was not observed in the available window. To confirm: after your next connector write on the custom track, query `VW_REFRESH_AUDIT` and record the result.
