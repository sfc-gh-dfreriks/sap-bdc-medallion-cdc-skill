# Changelog

## 1.0.0 — 2026-10-02

- First release: discover, readiness, profile, grain verification (with masking gate), config-driven SQL rendering for control plane, Bronze, Silver, Gold and operations, deploy and verification workflow.
- Standard track (`__OPERATION_TYPE` / `__TIMESTAMP`) tested end-to-end on a live SAP BDC share; custom track (`ZZLASTCHGDATE`) tested on a stand-in source including insert, update and hard delete.
- Documents two silent blockers found during testing: tag-based PII masking on key columns, and Iceberg-table / zero-copy connector grants needed for dynamic-table refresh.
