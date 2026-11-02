# Changelog

## 1.1.0 — 2026-11-02

- Keys come from SAP's declared primary keys. Catalog-linked databases now answer `SHOW PRIMARY KEYS` (first seen 2026-10-30; 54 of 55 objects across 8 of 9 shares on the reference account; the custom product `SAP_REFRESHDB_SHARE_V1` has none). `profile_share.py` writes them as `keys` with `key_source: declared`; `--guess-keys` now only fills objects with no declared key (`key_source: inferred`).
- `verify_grain.py` compares config keys with the declared key: new verdicts `DRIFT` (config differs; allowed only with `key_override: true` + `key_override_reason`) and `VIOLATED` (declared key not unique — `RELY` is not enforced). `PASS` says whether the key is declared or inferred. Non-zero exit on any failure.
- Control plane: `KEY_SOURCE` / `KEY_OVERRIDE` in the registry, `DECLARED_KEY_SNAPSHOT`, declared key recorded in `OBJECT_BASELINE` by `SP_RECORD_DECLARED_KEYS` (called from `SP_VALIDATE_METADATA`), `VW_KEY_DRIFT`, and `KEY_VERDICT` in `VW_CAPTURE_HEALTH`. Existing registries are upgraded with `ADD COLUMN IF NOT EXISTS`.
- Silver table comments state the key source.
- New optional `60_changes.sql` (`cdc_log: true`, global or per object): streams on Silver, `CHANGES.CHANGE_LOG`, and `SP_DRAIN_CHANGES`, which pairs DELETE/INSERT stream rows on the key and a business-column hash into I/U/D events in one committed transaction. No task is created.
- Worked example re-verified against the live `CUSTOMER_V1` share and regenerated.


## 1.0.0 — 2026-10-02

- First release: discover, readiness, profile, grain verification (with masking gate), config-driven SQL rendering for control plane, Bronze, Silver, Gold and operations, deploy and verification workflow.
- Standard track (`__OPERATION_TYPE` / `__TIMESTAMP`) tested end-to-end on a live SAP BDC share; custom track (`ZZLASTCHGDATE`) tested on a stand-in source including insert, update and hard delete.
- Documents two silent blockers found during testing: tag-based PII masking on key columns, and Iceberg-table / zero-copy connector grants needed for dynamic-table refresh.
