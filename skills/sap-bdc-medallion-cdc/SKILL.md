---
name: sap-bdc-medallion-cdc
description: "Build change data capture and a Bronze/Silver/Gold medallion over SAP Business Data Cloud (BDC) catalog-linked (zero-copy) shares in Snowflake: readiness assessment, standard vs custom CDC track, declared primary keys and grain verification, masking/privilege gate, generated SQL for control plane, Bronze views, incremental Silver dynamic tables, Gold marts, polling, reload detection, key drift, reconciliation and an optional I/U/D change log. Use when: CDC on SAP BDC data products, incremental capture from a catalog-linked database, medallion over a BDC share, Silver/Gold dynamic tables on SAP data, data is a day behind, streams or CHANGES refused on BDC tables, primary key or merge key of a BDC object, insert/update/delete change log from SAP data. Triggers: SAP BDC CDC, BDC medallion, catalog-linked database CDC, zero-copy CDC, __OPERATION_TYPE, __TIMESTAMP, ZZLASTCHGDATE, load_type, BDC Connect dynamic tables, incremental refresh SAP share, bronze silver gold SAP BDC, SHOW PRIMARY KEYS, declared key, primary key, RELY constraint, key drift, cdc_log, change log."
---

# SAP BDC Medallion + CDC

Builds incremental capture and a medallion over one SAP BDC catalog-linked
database, from assessment to a running, self-checking pipeline. Everything is
generated from a reviewed config, so the SQL is deterministic and diffable.

**Why it is not obvious.** Objects in a catalog-linked database are Iceberg
tables on SAP's external catalog: `CREATE STREAM`, `CHANGES()` and
`SET CHANGE_TRACKING` are all refused. What works is dynamic tables with
`REFRESH_MODE = INCREMENTAL`, driven by a free poll of the replication
watermark SAP already writes. Background: `references/how-it-works.md`.

## Prerequisites

- A Snowflake connection (`~/.snowflake/connections.toml`) and a warehouse.
- At least one catalog-linked database created by BDC Connect.
- Python 3.10+ with `pip install -r <SKILL_DIR>/requirements.txt` — check with
  `python3 -c "import snowflake.connector, jinja2, yaml"` and install if it fails.
- A **capture role** that sees unmasked key columns and holds Iceberg-table and
  connector grants — see `references/masking-and-privileges.md`. This is the
  most common reason a build looks healthy and is wrong.

`<SKILL_DIR>` is this skill's directory. All scripts are read-only except
`run_sql.py`, which only runs files you have reviewed.

## Workflow

### Step 1: Discover the share

```bash
python3 <SKILL_DIR>/scripts/profile_share.py --connection <conn>
```

Lists catalog-linked databases (found by `kind`, not by name). Ask which one to
build on. **Never** use a local copy of a share: connector metadata on copies
was found corrupted, silently.

### Step 2: Readiness assessment

```bash
cd <SKILL_DIR>/scripts/readiness && \
python3 cdc_readiness.py --connection <conn> --report readiness.md
```

Classifies every object's track and validates the metadata; verdict is READY,
READY WITH CAVEATS or NOT READY. Summarise the verdict and caveats.

| | STANDARD track | CUSTOM track |
| --- | --- | --- |
| signal | `__OPERATION_TYPE`, `__TIMESTAMP` (TEXT), `load_type_<hash>`, `run_id_<hash>` | a replication timestamp such as `ZZLASTCHGDATE` |
| provided by | SAP, automatically | the pipeline owner, by choice |
| details | `references/standard-products-cdc.md` | `references/custom-products-cdc.md` |

If an object has no signal (`NONE`), it cannot be polled; say so.

**STOP**: Confirm the target database, the track, and which objects to include.

### Step 3: Profile and draft the config

```bash
python3 <SKILL_DIR>/scripts/profile_share.py --connection <conn> --role <capture_role> \
  --database <CLD> --target <TARGET_DB> --warehouse <WH> --out <work>/config.yaml --guess-keys
```

Prints per object: rows, track, flow hash, the **declared primary key**,
**masked columns**, and indicator columns with their **types**. Writes a draft
`config.yaml` (schema: `<SKILL_DIR>/config/example.yaml`). Then fill in:

- `keys` — the full grain. Standard data products declare a primary key on
  every object (`SHOW PRIMARY KEYS`); the draft copies it, in `key_sequence`
  order, with `key_source: declared` and the constraint name in a comment. Keep
  it. Only objects with **no** declared key (custom data products) need a key
  from you: use SAP knowledge of the CDS entity (CSN key fields); `--guess-keys`
  proposes the shortest unique leading prefix for those objects only
  (`key_source: inferred`). To deliberately differ from a declared key, set
  `key_override: true` and `key_override_reason`.
- `class` — `MASTER`, `TRANSACTION` or `EMPTY` (Bronze view only).
- `nullable_keys`, `flags` (type-checked), optional `referential_integrity`.
- `cdc_log` — optional, global or per object: also generate the I/U/D change
  log (`60_changes.sql`, see Step 5).

One config per schema. Multiple flow hashes mean multiple products.

### Step 4: Verify the grain AS the capture role

```bash
python3 <SKILL_DIR>/scripts/verify_grain.py <work>/config.yaml --connection <conn> --role <capture_role>
```

Every populated object must report `PASS`; the note says whether the key is
the declared one or inferred. Declared keys are `RELY` — asserted by SAP, not
enforced by Snowflake — which is why they are counted here too.

- `DRIFT` — config keys differ from the declared primary key. Use the declared
  key; only keep yours with `key_override: true` and a reason.
- `VIOLATED` — the declared key itself is not unique (counts reported). Do not
  build on it; raise it with SAP.
- `FAIL` — key too short; it would silently discard rows. Add key parts.
- `BLOCKED` — a key column is **masked** for this role; Silver would collapse
  to one row per masked value. Follow `references/masking-and-privileges.md`.
  Do not work around it by dropping the key.
- `POPULATED` — an `EMPTY` object now has rows; change its class.

**STOP**: Show the verdict table. Do not continue until all PASS (the script
exits non-zero on DRIFT, VIOLATED, FAIL or BLOCKED).

### Step 5: Render and review the SQL

```bash
python3 <SKILL_DIR>/scripts/render_sql.py <work>/config.yaml --out <work>/generated
```

Writes `10_control`, `20_bronze`, `30_silver`, `40_gold`, `50_ops`, and
`60_changes` when any object has `cdc_log: true`.

`60_changes` creates a `CHANGES` schema with a standard stream on each chosen
Silver dynamic table, `CHANGES.CHANGE_LOG`, and `CHANGES.SP_DRAIN_CHANGES()`.
The drain copies the streams into a temp table and writes the log in one
committed transaction, pairing DELETE and INSERT rows on the key: different
business-column hash = `U`, INSERT only = `I`, DELETE only = `D`, same hash =
re-delivery (dropped). It never reads `METADATA$ISUPDATE` (always FALSE on this
path). No task is created — scheduling the drain is the user's decision.
Re-run `60_changes` after re-running `30_silver`.

Gold is domain specific and is not generated. Either write a Jinja template
(`gold.template`) or leave the stub. Follow `references/layer-rules.md`:
pre-aggregate children before joining, bridges as separate objects,
`REFRESH_MODE = INCREMENTAL`, no `SYSDATE()`/`CURRENT_*` in a dynamic table.
List Gold dynamic tables under `gold.dynamic_tables`.
`examples/customer-master/gold.sql.j2` is a complete example.

**STOP**: Present the files and ask for approval to deploy. They create a
database, schemas, views, dynamic tables, procedures and two **suspended** tasks
(plus streams, the change log and the drain procedure if `cdc_log` is set).

### Step 6: Deploy as the capture role

```bash
python3 <SKILL_DIR>/scripts/run_sql.py --connection <conn> --role <capture_role> \
  --warehouse <WH> --dir <work>/generated
```

Idempotent; stops at the first failing statement. A refresh error saying the
object "does not exist or not authorized" while the role can query it means the
Iceberg-table or connector grant is missing (`references/masking-and-privileges.md`).

### Step 7: Prove it

Run as the capture role, in `<TARGET_DB>`:

```sql
CALL CAPTURE_CONTROL.SP_VALIDATE_METADATA();   -- expect: All N object(s) validated; records declared keys
SELECT * FROM CAPTURE_CONTROL.VW_KEY_DRIFT;    -- expect: MATCH (NO_DECLARED_KEY on custom products)
CALL CAPTURE_CONTROL.SP_CAPTURE_CYCLE();       -- first: refreshed N, Gold rebuilt
CALL CAPTURE_CONTROL.SP_CAPTURE_CYCLE();       -- second: refreshed 0 (nothing moved)
CALL CAPTURE_CONTROL.SP_RECONCILE();           -- expect: source and Silver agree
SELECT * FROM CAPTURE_CONTROL.VW_CAPTURE_HEALTH;
SHOW DYNAMIC TABLES IN DATABASE <TARGET_DB>;   -- every refresh_mode = INCREMENTAL
```

Report each result. Investigate any `UNUSABLE`, `DIFFERENCE`, `RELOADED`,
`CONFIG_DRIFT`, `DECLARED_KEY_CHANGED` or a
`FULL` refresh mode before handing over. `SNAPSHOT_ONLY` means the object holds
only initial-load rows (often a re-publish); note it.

### Step 8: Operate

**STOP**: Ask before resuming tasks — they start consuming credits. The same
applies to any task the user wants for `CHANGES.SP_DRAIN_CHANGES()`: create it
SUSPENDED and resume only on approval.

```sql
ALTER TASK <TARGET_DB>.CAPTURE_CONTROL.TSK_CAPTURE_CYCLE RESUME;    -- poll, default 15 min
ALTER TASK <TARGET_DB>.CAPTURE_CONTROL.TSK_DAILY_ASSURANCE RESUME;  -- validate + reconcile
```

Hand over `VW_CAPTURE_HEALTH` as the daily check, and these follow-ups:

- After SAP adds columns or re-publishes a product (new flow hash):
  re-profile, re-render, re-run `20_bronze` onward.
- New objects need the Iceberg-table grant again.
- Masking can be added later: re-run `verify_grain.py --role <capture_role>`
  periodically.
- `VW_KEY_DRIFT` showing `DECLARED_KEY_CHANGED` means SAP changed an object's
  primary key: re-profile, re-verify, re-render.

## Rules that must not be relaxed

- Point capture at the live catalog-linked database, never a copy.
- Compare the watermark as TEXT; parse only to display. `__TIMESTAMP` uses a
  comma decimal separator and an unguarded cast returns NULL silently.
- `run_id` is a flow identifier, not a watermark.
- State `REFRESH_MODE = INCREMENTAL`; no non-deterministic SQL in a dynamic table.
- Use the declared primary key, verify it, infer only where none is declared.
  `RELY` is not enforcement. Quote key columns: they are case-sensitive camel
  case. Check indicator types before `COALESCE`.
- Pair change-log events on the key and a business-column hash; never trust
  `METADATA$ISUPDATE` on a Silver stream.
- Deletes are unverified on both tracks: keep daily reconciliation on.
- Capture fixes freshness, not missing fields: `references/gaps-and-asks.md`.

## Stopping points

- Step 2: target, track and object scope confirmed
- Step 3: keys taken from the declared primary keys; any inferred or overridden key confirmed with the user
- Step 4: all objects PASS grain verification as the capture role (no DRIFT / VIOLATED)
- Step 5: generated SQL approved before any DDL runs
- Step 8: explicit approval before resuming tasks, including any drain task for the change log

## Output

A target database with `CAPTURE_CONTROL` (registry, validation, audit,
reconciliation, declared-key baseline and drift view, procedures, views,
suspended tasks), `BRONZE` zero-copy views, `SILVER` incremental dynamic tables,
`GOLD` marts and, with `cdc_log`, `CHANGES` (streams, change log, drain). Also the reviewed
`config.yaml`, the generated SQL, a readiness report, and a verification summary.

## References

- `references/masking-and-privileges.md` — masked keys, Iceberg grants, connector USAGE
- `references/layer-rules.md` — Bronze, Silver and Gold rules, flags, Gold patterns
- `references/how-it-works.md` — why streams are refused and dynamic tables work
- `references/standard-products-cdc.md`, `references/custom-products-cdc.md` — per-track detail
- `references/gaps-and-asks.md` — limits and open questions
- `config/example.yaml` — annotated config schema
