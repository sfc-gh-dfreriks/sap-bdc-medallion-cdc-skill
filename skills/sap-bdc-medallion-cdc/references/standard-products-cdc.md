> Background reference, from [sap-bdc-cdc](https://github.com/sfc-gh-dfreriks/sap-bdc-cdc). File names such as `sql/05_...` and `probes/` refer to that repository; in this skill the equivalents are `templates/` and `scripts/`.

# Standard data products: CDC procedure

This document is a step-by-step procedure for deploying incremental capture from a standard SAP BDC data product. SAP generates change metadata automatically on these products; the procedure validates it, registers it, and keeps a Gold dynamic table current against it.

For the mechanism shared with the custom track, see [how-it-works.md](how-it-works.md). If your share carries custom data products instead of or in addition to standard ones, see [custom-products-cdc.md](custom-products-cdc.md).

![Standard track end to end](https://raw.githubusercontent.com/sfc-gh-dfreriks/sap-bdc-cdc/main/assets/diagrams/D8_standard_track.png)

---

## What a standard data product carries

SAP generates four columns on every object in a standard data product. Their names follow a fixed pattern:

| Column | Name pattern | Type | Notes |
| --- | --- | --- | --- |
| Operation type | `__OPERATION_TYPE` | TEXT | Fixed name, two leading underscores, uppercase |
| Connector timestamp | `__TIMESTAMP` | TEXT | Fixed name, two leading underscores. Format: ISO-8601 with comma decimal separator |
| Load phase | `load_type_<32-hex-hash>` | TEXT | Lowercase prefix; value is `initial` or `delta` |
| Flow ID | `run_id_<32-hex-hash>` | TEXT | Lowercase prefix; identifies the replication flow |

The 32-character hex hash appended to `load_type` and `run_id` was identical across all 7 objects of the reference product. It identifies the replication flow, not the individual object. Its presence is a reliable marker that you are reading a live catalog-linked database - copies lose the suffix and expose plain `LOAD_TYPE` and `RUN_ID` without it.

**`__TIMESTAMP` text format.** The format observed is `2026-06-15T09:41:59,6426580Z` - ISO-8601 with a comma where the fractional-seconds decimal point should be. `TRY_TO_TIMESTAMP_NTZ` does not error on this format; it returns NULL for every row. An unguarded cast therefore looks like an empty column rather than a format error. Correct parse requires `REPLACE(col, ',', '.')` first.

**The comparison rule: compare as text, parse only to display.** The format is fixed-width and zero-padded, so lexicographic order is chronological order. This was verified directly by confirming that `parse(MAX(raw)) == MAX(parse(raw))`. The poll cost difference is significant:

| Operation | Data scanned | Duration (39,462-row reference table) |
| --- | --- | --- |
| `MAX("__TIMESTAMP")` over raw TEXT | 0.000 MB | 2 ms |
| `MAX(TRY_TO_TIMESTAMP_NTZ(...))` over parsed expression | 0.310 MB | 45 ms |

The raw MAX is answered from Iceberg manifest statistics without touching the data. The parsed MAX reads the object. Every comparison in `sql/05_standard_product_track.sql` stays in text; parsing appears only in display views.

**`run_id` is not a batch discriminator.** On the reference share it held the same value on both the initial snapshot (67,087 rows) and all subsequent delta rows. It cannot be used as a watermark.

**Operations observed.** Across 5 non-empty objects on the reference share: `L` (initial load, 67,087 rows) and `A` (delta after-image, 393 rows). `load_type` carried `initial` = 67,087 and `delta` = 393. No delete code appeared anywhere. The authoritative value enumeration could not be confirmed from SAP documentation - the probe discovers the actual distribution per tenant rather than assuming it. See [gaps-and-asks.md](gaps-and-asks.md) for the open ask to SAP.

---

## The trust gate

This finding is the reason validation is Step 2 rather than optional. On the reference account the four connector columns were clean inside every live catalog-linked database. Every local copy of a standard product on the same account had corrupted metadata:

- **One copy:** `__OPERATION_TYPE` held media titles such as "Mission Possible 1" and "About a Girl - Series" plus 16,317 nulls - 17 distinct values, the longest 33 characters.
- **A second copy:** `__OPERATION_TYPE`, `__TIMESTAMP`, and `LOAD_TYPE` all held the same value - customer identifiers, 620 distinct values, 10 characters - on 735,305 of 735,305 rows. `RUN_ID` was null throughout. Confirmed with a positional `SELECT *`, ruling out a column-resolution artefact.
- **A third copy:** the same degeneracy on 557,693 of 558,011 rows.

The defect then propagated downstream into medallion layers built on those copies. None of it raised an error. A corrupt operation column produces wrong incremental results silently.

**Two rules follow directly.** First, point capture at the catalog-linked database itself, never at a copy. Second, validate before you build. `SP_VALIDATE_STANDARD` checks the specific thresholds derived from this contrast. Clean objects had 2 distinct operation values of length 1; corrupted copies had 17 distinct values at length 33, or 620 distinct at length 10.

---

## Step 1 - Identify standard objects

Run the readiness probe. It classifies each object by track, validates standard metadata where present, and reports the exact column names found.

```bash
cd probes
pip install -r requirements.txt
python3 cdc_readiness.py --connection <your_connection> --report readiness.md
```

A standard-track result contains:

```
[  ok  ] signal.standard: 7 object(s) carry connector metadata columns (7 with a flow hash)
[  ..  ] semantics.operation: Across 5 object(s): L=67,087, A=393. No delete code observed.
[  ok  ] semantics.trustworthy: All 5 standard object(s) passed: the operation column holds
         short codes, timestamps parse, and no column is duplicated.
```

Objects the probe cannot describe (`share.uninitialized`) are excluded until SAP initialises them.

**Success criterion:** at least one object classified as standard, with a hash-suffixed `load_type_<hash>` column name reported.

---

## Step 2 - Validate the metadata (gate)

This step is not optional. Do not create dynamic tables or schedule tasks for any object until it passes.

Run `sql/05_standard_product_track.sql` to create the validation procedure. It extends the control schema from `sql/01_capture_control.sql` - deploy that file first if you have not already. Then call the procedure:

```sql
CALL <L2_DB>.CAPTURE_CONTROL.SP_VALIDATE_STANDARD();
```

The procedure checks each registered STANDARD object for:

- **Operation cardinality.** More than 12 distinct values means the column is not a change code.
- **Max operation length.** More than 2 characters means the column is not a change code.
- **Timestamp parse rate.** How many rows yield a non-NULL result from `TRY_TO_TIMESTAMP_NTZ(REPLACE(__TIMESTAMP, ',', '.'))`. Zero parseable timestamps means the column does not hold timestamps.
- **Degeneracy.** Whether `__OPERATION_TYPE`, `__TIMESTAMP`, and `LOAD_TYPE_<hash>` all hold the same value on any row.
- **Hash suffix.** Whether the column name is `load_type_<32-hex-hash>` or the plain `LOAD_TYPE`. An absent hash suffix means the source is a copy, not a live share.

Read the validation results:

```sql
SELECT OBJECT_NAME, VERDICT, FAULTS, OPERATION_CARD, OPERATION_MAX_LEN,
       TIMESTAMPS_PARSED, DEGENERATE_ROWS
FROM   <L2_DB>.CAPTURE_CONTROL.VW_STANDARD_OPERATION_MIX
ORDER  BY VERDICT DESC, OBJECT_NAME;
```

**Do not proceed with any UNUSABLE object.** An UNUSABLE result most commonly means `SOURCE_FQN` points at a local copy rather than the live catalog-linked database. Verify that `SOURCE_FQN` resolves to a database with `kind = 'CATALOG-LINKED DATABASE'` in `SHOW DATABASES`.

**Success criterion:** all registered objects return `USABLE`.

---

## Step 3 - Register in WATERMARK

After validation passes, register each standard-track object. The `WATERMARK` table is created by `sql/01_capture_control.sql` and carries a `TRACK` column with a CHECK constraint - any insert must specify `'STANDARD'` or `'CUSTOM'`; other values are rejected with error 23514.

```sql
INSERT INTO <L2_DB>.CAPTURE_CONTROL.WATERMARK
  (OBJECT_NAME, SOURCE_FQN, TRACK, WATERMARK_COLUMN, OPERATION_COLUMN,
   MERGE_KEY_COLUMNS, LAST_WATERMARK_TEXT)
SELECT '<NAME>',
       '"<LINKED_DB>"."<LINKED_SCHEMA>"."<OBJECT_NAME>"',
       'STANDARD',
       '__TIMESTAMP',
       '__OPERATION_TYPE',
       '<KEY_COLUMN_1>,<KEY_COLUMN_2>',
       NULL
WHERE  NOT EXISTS (SELECT 1 FROM <L2_DB>.CAPTURE_CONTROL.WATERMARK
                    WHERE OBJECT_NAME = '<NAME>');
```

`WATERMARK_COLUMN` must be `'__TIMESTAMP'` - the fixed-name column, two leading underscores. `LAST_WATERMARK_TEXT` is the comparison column for this track; it stores the raw text value and is never parsed during comparison. `OPERATION_COLUMN` must be `'__OPERATION_TYPE'`.

`MERGE_KEY_COLUMNS` is required and has no default. A wrong grain silently produces duplicate rows or discards real records without any constraint violation. Confirm the business key against SAP data product documentation before inserting.

**Success criterion:** `SELECT * FROM <L2_DB>.CAPTURE_CONTROL.WATERMARK WHERE TRACK = 'STANDARD'` returns one row per registered object.

---

## Step 4 - Create dynamic tables

Create one dynamic table per standard-track object, reading from the live catalog-linked database:

```sql
CREATE OR REPLACE DYNAMIC TABLE <L2_DB>.<SCHEMA>.DT_<NAME>
  TARGET_LAG    = '15 minutes'
  WAREHOUSE     = <WH>
  REFRESH_MODE  = INCREMENTAL
AS
SELECT *
FROM   "<LINKED_DB>"."<LINKED_SCHEMA>"."<OBJECT_NAME>";
```

State `REFRESH_MODE = INCREMENTAL` explicitly. `AUTO` allows Snowflake to silently fall back to `FULL` with no error when it cannot support incremental for a given query. Stating `INCREMENTAL` converts that silent cost regression into a deployment-time error.

Do not put non-deterministic expressions (`CURRENT_DATE()`, `CURRENT_TIMESTAMP()`, `RANDOM()`) inside the table definition. They force a full refresh every cycle. Put time-relative logic in a view over the dynamic table instead.

Confirm incremental acceptance after creation:

```sql
SHOW DYNAMIC TABLES IN SCHEMA <L2_DB>.<SCHEMA>;
-- Expected: refresh_mode = INCREMENTAL, refresh_mode_reason IS NULL
```

A non-null `refresh_mode_reason` means Snowflake fell back to `FULL`. Investigate before continuing.

On the reference share, a 3,355-row standard-track dynamic table refreshed at `REFRESH_ACTION = INCREMENTAL SUCCEEDED` in 5,241 ms, then at `REFRESH_ACTION = NO_DATA SUCCEEDED` in 610 ms on a subsequent poll that found no watermark advance.

**Note on predicate pruning.** On the reference standard-track objects (39,462 rows, single micropartition, 320 KB), a watermark predicate scanned approximately 100% of a full read. Predicate pruning at production scale was not demonstrated on this track. Drive refreshes from the free poll in Step 5 and let dynamic tables handle the recomputation; do not plan around file elimination on this track.

**Success criterion:** `refresh_mode = INCREMENTAL` and `refresh_mode_reason IS NULL` for each created table.

---

## Step 5 - Schedule the poll

`SP_POLL_STANDARD` checks whether the source watermark (`__TIMESTAMP` as text) advanced since the last poll. If it did, the procedure refreshes the named dynamic table and updates `WATERMARK`. If it did not, it returns `NO CHANGE` at a cost of 0.000 MB in 2 ms - answered from Iceberg manifest statistics without touching the data.

Create one task per registered object. Leave it suspended until Step 2 validation is confirmed clean for that object:

```sql
CREATE OR REPLACE TASK <L2_DB>.CAPTURE_CONTROL.TSK_POLL_<NAME>
  WAREHOUSE = <WH>
  SCHEDULE  = '15 minutes'
AS
CALL <L2_DB>.CAPTURE_CONTROL.SP_POLL_STANDARD(
       '<NAME>',
       '<L2_DB>.<SCHEMA>.DT_<NAME>');

-- Resume only after Step 2 returns USABLE for this object:
-- ALTER TASK <L2_DB>.CAPTURE_CONTROL.TSK_POLL_<NAME> RESUME;
```

On the reference share, the first poll returned `REFRESHED` and advanced the watermark to `2026-08-19T04:32:03,9099180Z`. The second poll, finding no advance, returned `NO CHANGE`.

**Success criterion:** first task execution returns `REFRESHED` or `NO CHANGE` (not `NOT REGISTERED` or an error). Query `VW_REFRESH_AUDIT` to confirm the dynamic table's refresh action after the first successful poll.

```sql
SELECT NAME, REFRESH_ACTION, DURATION_MS, ROWS_INSERTED, ROWS_DELETED
FROM   <L2_DB>.CAPTURE_CONTROL.VW_REFRESH_AUDIT
ORDER  BY REFRESH_START_TIME DESC
LIMIT  10;
```

---

## Step 6 - Monitor

Two views provide ongoing observability for standard-track objects.

**`VW_STANDARD_FRESHNESS` - lag per registered object:**

```sql
SELECT OBJECT_NAME, LAST_WATERMARK_TEXT, MINUTES_BEHIND, AS_OF
FROM   <L2_DB>.CAPTURE_CONTROL.VW_STANDARD_FRESHNESS;
```

The view uses `SYSDATE()` for the current-time reference, not `CURRENT_TIMESTAMP()`. `CURRENT_TIMESTAMP()` is session-timezone-aware; on a UTC-7 session it reported a real 109-minute lag as -311 minutes. Any custom lag query must use `SYSDATE()` for the same reason.

To parse the text watermark for display purposes only:

```sql
TRY_TO_TIMESTAMP_NTZ(REPLACE(LAST_WATERMARK_TEXT, ',', '.'))
```

Do not use this parsed value as a comparison operand. Comparisons must stay in text to preserve the zero-scan poll cost.

**`VW_STANDARD_OPERATION_MIX` - latest validation result per object:**

```sql
SELECT OBJECT_NAME, VERDICT, FAULTS, OPERATION_CARD, OPERATION_MAX_LEN
FROM   <L2_DB>.CAPTURE_CONTROL.VW_STANDARD_OPERATION_MIX;
```

Re-run `SP_VALIDATE_STANDARD` whenever the share is re-linked or a data product is re-published. Metadata corruption enters silently when a local copy replaces a live share as the source.

---

## Exception path: watermark MERGE

Use the MERGE template in `sql/05_standard_product_track.sql` only where a dynamic table cannot serve:

- The transformation contains a non-deterministic expression, forcing a full refresh every cycle.
- The target is outside Snowflake.

The MERGE template includes a commented DELETE branch. That branch requires knowing the delete code value for `__OPERATION_TYPE`. No delete code was observed on the reference share (67,480 rows across 5 objects), and the authoritative enumeration could not be confirmed. **Leave the DELETE branch commented until you observe a delete code on your specific tenant and confirm its value.**

Regardless, schedule the row-count reconciliation query from `sql/05_standard_product_track.sql` on a regular cadence. A persistent positive divergence (target row count exceeds source row count) indicates physical deletes that the watermark path did not capture.

---

## Delete status

`__OPERATION_TYPE` exists on every standard-track object, and delete detection is structurally possible if the column ever carries a delete code. Only `L` (initial load) and `A` (delta after-image) were observed on the reference share. No delete code appeared. Whether this data product ever emits one is an open question; see [gaps-and-asks.md](gaps-and-asks.md).

On the dynamic table path, no operation column is required. Dynamic tables detect deletes by diffing consecutive source snapshots - a row present in snapshot N and absent in N+1 is removed from the target. This is the recommended path for that reason.
