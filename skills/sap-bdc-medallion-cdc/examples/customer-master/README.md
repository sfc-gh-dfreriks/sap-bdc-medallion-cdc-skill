# CUSTOMER_V1 worked example

SAP BDC standard data product **CUSTOMER_V1** (customer master), standard
track. Seven objects in schema `customer`.

| file | what it is |
| --- | --- |
| `config.yaml` | the reviewed share config: declared (and verified) keys, classes, type-checked flags, `cdc_log` on two objects |
| `gold.sql.j2` | hand-written Gold: `DIM_CUSTOMER_360`, `BRG_CUSTOMER_SALES_AREA`, `VW_CUSTOMER_RISK` |
| `generated/` | what `render_sql.py config.yaml` produces — committed so you can read it without running anything |
| `readiness_report.md` | sample `cdc_readiness.py` output from a standard-track reference tenant |

Regenerate:

```bash
python3 ../../scripts/render_sql.py config.yaml
```

## Declared keys (2026-11-02)

`CUSTOMER_V1` now declares a primary key on all 11 objects (`SHOW PRIMARY KEYS
IN DATABASE CUSTOMER_V1`, `rely = true`). `profile_share.py` drafted every key
in this config from those declarations (`key_source: declared`), and
`verify_grain.py` returned PASS on all of them: `customer` 3,450 / 3,450,
`customersalesareatax` 43,085 / 43,085 on its six-part key. A copy of the config
with `customersalesareatax` cut to the four-part sales-area key returned
`DRIFT` (it would discard 28,997 rows) and exit code 1. This config covers 7 of
the 11 objects; the other four (`customerbusinesspartner`,
`customercontactbusinesspartner`, `customercontactperson`,
`customerpartnerfunction`) can be added from a fresh `profile_share.py` draft.

`CUSTOMER` and `CUSTOMERSALESAREATAX` set `cdc_log: true`, so `generated/`
includes `60_changes.sql`. Its statements compile; it has not been deployed.

## Test results (2026-10-02)

Deployed end-to-end from this config into a scratch database on a live SAP BDC
share, as a dedicated capture role, then dropped.

| check | result |
| --- | --- |
| `verify_grain.py` as ACCOUNTADMIN | **BLOCKED** on all 7 objects — `Customer` masked by `PII_MASK_STRING` (1 distinct value in 3,427 rows) |
| `verify_grain.py` as capture role tagged `UNMASK_PII` | PASS on all 7 |
| deploy (`10`–`50`) | 45 statements, no errors (after Iceberg-table and connector grants) |
| `SP_VALIDATE_METADATA` | all 7 objects `USABLE` |
| `SP_CAPTURE_CYCLE` #1 / #2 | refreshed 7, Gold rebuilt / refreshed 0, every refresh `NO_DATA` |
| `SP_RECONCILE` | source = Silver on all 7 (3,427 · 10,594 · 14,159 · 42,960 · 25 · 7 · 14) |
| dynamic tables | 9 of 9 `INCREMENTAL` |
| `DIM_CUSTOMER_360` | 3,427 rows, 3,427 distinct customers |
| `VW_REFERENTIAL_INTEGRITY` | 11 child rows reference a customer absent from the master |
| `VW_RELOAD_DETECTION` | 2 objects `SNAPSHOT_ONLY` (initial-load rows only — a re-publish) |

The two objects that were empty at first build (`customerunloadingpoint`,
`customerwithholdingtax`) had since been populated by SAP; `verify_grain.py`
flags that as `POPULATED` and they are now `class: MASTER`.

A custom-track config was tested the same way against a stand-in source with a
`ZZLASTCHGDATE` column (native tables, not a live share): after an insert, an
update and a hard delete, the next cycle refreshed only the changed object,
`INCREMENTAL`, and all three changes were correct in Silver with reconciliation
matching.
