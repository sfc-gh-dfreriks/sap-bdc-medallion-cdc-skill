# Layer rules — Bronze, Silver, Gold

## Bronze: views, never tables

- One `BRONZE.V_<OBJECT>` view per shared object, `SELECT *` on the live
  catalog-linked database. No copy: SAP already landed the data as Iceberg, and
  connector metadata on local **copies** was found corrupted (operation codes
  holding media titles; operation, timestamp and load_type holding the same
  customer ID on every row).
- Standard track: rename `load_type_<hash>` / `run_id_<hash>` to `LOAD_TYPE` /
  `RUN_ID` here, so a re-publish (new hash) is one re-render.
- Keep `"__TIMESTAMP"` raw TEXT. It has a **comma** decimal separator
  (`2026-06-15T09:41:59,6426580Z`); `TRY_TO_TIMESTAMP_NTZ` returns NULL on it
  silently, so parse only as `TRY_TO_TIMESTAMP_NTZ(REPLACE(col, ',', '.'))` and
  only for display (`CDC_TIMESTAMP_UTC`).
- `SELECT *` fixes the column list at creation. Re-render and re-run
  `20_bronze.sql` after SAP adds columns to a product.

## Silver: four rules that fail silently

1. **`REFRESH_MODE = INCREMENTAL`, stated.** `AUTO` falls back to `FULL`
   without error, and a FULL table forces everything downstream to FULL.
2. **No `SYSDATE()`, `CURRENT_TIMESTAMP()`, `CURRENT_DATE()`** in any dynamic
   table. One blocks incremental refresh. Time-relative logic goes in a plain view.
3. **De-duplicate on the verified grain.** Use the primary key SAP declares
   on the catalog-linked table (`SHOW PRIMARY KEYS`; `profile_share.py` copies
   it into the config with `key_source: declared`). It is `RELY` — asserted by
   SAP, not enforced by Snowflake — so `verify_grain.py` must still PASS for the
   role that will own Silver (see masking-and-privileges.md); a config key that
   differs from the declaration is `DRIFT`, a declaration that is not unique is
   `VIOLATED`. Infer a key (`key_source: inferred`) only where none is declared,
   which today means custom data products. Each Silver table's COMMENT states
   the key source. List key parts
   that can be NULL under `nullable_keys` so they are COALESCEd to `'~'`.
4. **ORDER BY the raw watermark**, not a parsed expression.

Flags (`flags:` in the config) are deterministic expressions added in Silver.
**Check the column type first** — `profile_share.py` prints it. SAP indicator
fields are TEXT (`'X'`/`''`) on some objects and BOOLEAN on others, even within
one table. `COALESCE(<boolean>, '')` fails at the **first refresh**, not at
CREATE, leaving a broken object behind.

| column type | flag expression |
| --- | --- |
| BOOLEAN | `COALESCE("DeletionIndicator", FALSE)` |
| VARCHAR indicator | `IFF(COALESCE("DeliveryIsBlocked", '') <> '', TRUE, FALSE)` |
| numeric in text | `TRY_TO_NUMBER("DunningLevel")` |

Object classes:

| class | Silver | polled | default `TARGET_LAG` |
| --- | --- | --- | --- |
| `MASTER` | dynamic table | yes | 24 hours (backstop; the poll drives refresh) |
| `TRANSACTION` | dynamic table | yes | 1 hour |
| `EMPTY` | none — Bronze view only | no | — |

`verify_grain.py` reports `POPULATED` when an EMPTY object starts carrying rows.

## Changes: an I/U/D log on Silver (optional)

`cdc_log: true` (globally or per object) adds `60_changes.sql`: a standard
stream on each Silver dynamic table, `CHANGES.CHANGE_LOG`, and
`CHANGES.SP_DRAIN_CHANGES()`. An incremental refresh writes a changed row as a
DELETE plus an INSERT and `METADATA$ISUPDATE` is always FALSE on this path, so
the drain pairs the two on the key and compares a hash of the business columns
(lineage `CDC_*` columns excluded): different hash = `U`, INSERT only = `I`,
DELETE only = `D`, same hash = re-delivery, dropped. This is only correct on the
true grain — another reason to use the declared key. The stream read and the
log write share one committed transaction. No task is created: schedule the
drain yourself (create any task SUSPENDED), often enough that the stream does
not go stale, and re-run `60_changes.sql` after re-running `30_silver.sql`
(replacing a dynamic table invalidates its stream).

## Gold: hand-written, domain specific

Gold is where SAP field names become `UPPER_SNAKE_CASE` business names; it is
the last layer that quotes identifiers. It is not generated — write it as a
Jinja template (`gold.template` in the config) so it shares `{{ T }}` and
`{{ WH }}` with the generated files. `examples/customer-master/gold.sql.j2` is a
complete worked example.

- **Pre-aggregate children, then join.** Child objects are larger than the
  master (10,594 company-code rows and 42,960 tax rows against 3,427 customers on
  the test share). Joining at row level then aggregating fans the master out and
  every count is wrong while still looking plausible.

  ```sql
  WITH cc AS (SELECT "Customer" AS CUSTOMER_ID, COUNT(*) AS COMPANY_CODE_COUNT
              FROM SILVER.DT_CUSTOMERCOMPANYCODE GROUP BY 1)
  SELECT c."Customer" AS CUSTOMER_ID, COALESCE(cc.COMPANY_CODE_COUNT, 0) AS COMPANY_CODE_COUNT
  FROM SILVER.DT_CUSTOMER c LEFT JOIN cc ON cc.CUSTOMER_ID = c."Customer";
  ```
- **Bridges are their own objects.** When consumers need to filter by a
  child's attributes (sales org, company code), publish the child grain as a
  `BRG_*` dynamic table so the fan-out is explicit.
- **Same two Silver rules apply**: `REFRESH_MODE = INCREMENTAL`, nothing
  non-deterministic. Ages and "days since" go in a `VW_*` view over the DT.
- **Explicit `TARGET_LAG`, not `DOWNSTREAM`** — nothing downstream of Gold is a
  dynamic table, so DOWNSTREAM would never refresh on its own.
- List every Gold dynamic table under `gold.dynamic_tables` so
  `SP_CAPTURE_CYCLE` refreshes it, in order, when Silver moved.
- Anchor dimensions on the master. Children whose key is missing from the master
  are excluded; configure `referential_integrity` so
  `VW_REFERENTIAL_INTEGRITY` reports that number.
