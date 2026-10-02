# SAP BDC change data capture readiness

**Verdict: READY WITH CAVEATS**

- The change signal is present and usable, but the recommended mechanism was not proven: re-run with --with-scratch-tests to confirm that a dynamic table refreshes incrementally over this share rather than inferring it.
- Standard-product objects can be polled for free: MAX() on the raw __TIMESTAMP text is answered from Iceberg statistics. Compare it as text and parse only for display, or the poll starts scanning.
- The merge-versus-reload test was inconclusive on a single write day. Re-run after the next refresh; if the connector reloads, the watermark path becomes as expensive as a full rebuild.

Assessed 2026-08-21T16:56:46+00:00.

## Findings

| Check | Status | Detail |
| --- | --- | --- |
| `share.present` | PASS | 8 catalog-linked database(s): CLASS_V2, COMMONCONFIGURATIONDATA_V1, COMPENSATIONSTRUCTURE_V1, COMPENSATION_V1, COREWORKFORCEDATA_V1, CUSTOMER_V1, SAP_REFRESHDB_SHARE_V1, WORKFORCEPERSONPROFILE_V1 |
| `share.inventory` | INFO | 47 objects, 853 columns |
| `share.uninitialized` | WARN | 1 of 47 objects could not be described and are excluded from every check below. A share can advertise a table that has never been materialised; ask the pipeline owner whether these are expected: refresh_table |
| `signal.standard` | PASS | 7 object(s) carry connector metadata columns (7 with a flow hash), 1 custom-track, 38 with no signal. |
| `signal.watermark` | WARN | Custom extension timestamp on 1 object(s); 7 covered by the standard track; 38 with no signal of either kind, which must rely on dynamic tables. |
| `semantics.operation` | INFO | Across 5 object(s): L=67,087, A=393. No delete code observed. Either none has occurred yet or this product never emits one; until one is seen, treat hard deletes as unrepresented and reconcile row counts on a schedule. |
| `semantics.load_type` | PASS | Across 5 object(s): initial=67,087, delta=393. Both a snapshot and deltas are present, so the connector appends changes rather than reloading. |
| `semantics.trustworthy` | PASS | All 5 standard object(s) passed: the operation column holds short codes, timestamps parse, and no column is duplicated. |
| `cost.poll` | PASS | MAX on the raw text is answered from manifest statistics without reading data. |
| `cost.extract` | WARN | Extracting the most recent batch scanned 100% of a full read, so an incremental cycle costs about as much as a rebuild. Prefer dynamic tables, whose refresh does not depend on this predicate pruning. |
| `signal.merge_or_reload` | WARN | class: 27 write days but rows are spread evenly (largest day 33.0%). Confirm this is genuine change volume and not a periodic reload. |
| `cost.pruning` | FAIL | Watermark predicate scans 100.0% of a full read. The column is not correlated with file layout, so it cannot prune. Prefer dynamic tables, whose refresh does not depend on this. |
| `capability.dynamic_table` | INFO | Not tested. Re-run with --with-scratch-tests to prove the recommended mechanism rather than infer it. |

## What to do next

If the verdict is READY, configure your Gold layer dynamic tables with
`REFRESH_MODE = INCREMENTAL` stated explicitly and set `TARGET_LAG` per object
class. See `docs/04-operating-it.md` and `sql/`.

Whatever the verdict, deploy `sql/04_monitoring.sql`. It detects the single
event that invalidates the whole pattern: the connector switching from
incremental merge to full reload.
