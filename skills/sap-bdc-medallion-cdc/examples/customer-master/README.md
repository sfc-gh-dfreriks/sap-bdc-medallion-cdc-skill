# CUSTOMER_V1 worked example

SAP BDC standard data product **CUSTOMER_V1** (customer master), standard
track. Seven objects in schema `customer`.

| file | what it is |
| --- | --- |
| `config.yaml` | the reviewed share config: verified keys, classes, type-checked flags |
| `gold.sql.j2` | hand-written Gold: `DIM_CUSTOMER_360`, `BRG_CUSTOMER_SALES_AREA`, `VW_CUSTOMER_RISK` |
| `generated/` | what `render_sql.py config.yaml` produces — committed so you can read it without running anything |
| `readiness_report.md` | sample `cdc_readiness.py` output from a standard-track reference tenant |

Regenerate:

```bash
python3 ../../scripts/render_sql.py config.yaml
```

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
