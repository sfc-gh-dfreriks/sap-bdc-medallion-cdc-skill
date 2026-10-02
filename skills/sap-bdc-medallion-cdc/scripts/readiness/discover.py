"""Discovery.

Finds the SAP BDC catalog-linked databases in whatever account it is pointed at,
without any hardcoded names. The marker is the database kind that BDC Connect sets,
which is stable across tenants and naming conventions:

    SHOW DATABASES  ->  kind = 'CATALOG-LINKED DATABASE'

Two behaviours of these objects shape everything downstream and are worth knowing
before reading the code:

  * INFORMATION_SCHEMA.COLUMNS returns no rows for a catalog-linked database, so the
    column list can only be obtained with DESCRIBE TABLE, one statement per object.
  * Schema names contain a colon and object names are mixed case, so every reference
    must be quoted.

Two kinds of data product carry two different change signals, and an account can hold
both at once, so every object is classified individually rather than per account:

  standard  SAP generates the metadata columns __OPERATION_TYPE, __TIMESTAMP and
            load_type / run_id. Nothing has to be configured in the source system.
  custom    the pipeline owner exposes a source-system change timestamp, commonly an
            SLT extension field named ZZLASTCHGDATE. No operation column exists, so
            deletes are invisible on this track.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

from conn import Session, fqn


@dataclass
class Column:
    name: str
    type: str

    @property
    def is_varchar(self) -> bool:
        return self.type.upper().startswith(("VARCHAR", "STRING", "TEXT"))

    @property
    def is_numeric(self) -> bool:
        return self.type.upper().startswith(("NUMBER", "FLOAT", "DOUBLE", "DECIMAL", "INT"))

    @property
    def is_temporal(self) -> bool:
        return self.type.upper().startswith(("DATE", "TIMESTAMP", "TIME"))


@dataclass
class Obj:
    database: str
    schema: str
    name: str
    columns: list[Column] = field(default_factory=list)
    rows: int | None = None
    error: str | None = None

    @property
    def fqn(self) -> str:
        return fqn(self.database, self.schema, self.name)

    def column(self, name: str) -> Column | None:
        low = name.lower()
        return next((c for c in self.columns if c.name.lower() == low), None)

    def find(self, *candidates: str) -> Column | None:
        """First column matching any candidate name, case-insensitively."""
        for cand in candidates:
            c = self.column(cand)
            if c:
                return c
        return None


# Candidate names for the replication write timestamp. SAP BDC pipelines built with
# SLT commonly add a customer extension field; the name varies by implementation, so
# several spellings are tried before falling back to shape-based detection.
WATERMARK_CANDIDATES = (
    "ZZLASTCHGDATE", "ZZ_LAST_CHG_DATE", "ZZLASTCHANGEDATE", "ZZUPDATED_AT",
    "ZZLOADTS", "ZZ_LOAD_TS", "_LOAD_TS", "LAST_REPLICATED_AT", "REPLICATION_TS",
)


# ---------------------------------------------------------------- standard products
#
# Standard data products carry connector-generated metadata columns instead of a
# customer extension field. Two of the four names are fixed; the other two are
# suffixed with a 32-hex flow hash on a genuine catalog-linked share:
#
#     __OPERATION_TYPE                            the change mode for the row
#     __TIMESTAMP                                 when the connector wrote it
#     load_type_8995a2862a8343bd8390aaa82c46e881  'initial' or 'delta'
#     run_id_8995a2862a8343bd8390aaa82c46e881     identifies the replication flow
#
# The hash is identical across every object of one product, so it names the flow
# rather than the object. Measured on a reference share: run_id held the same value
# on both the initial snapshot and the deltas that followed, so it is NOT a batch
# discriminator and cannot be used as a watermark.
STD_OPERATION = "__OPERATION_TYPE"
STD_TIMESTAMP = "__TIMESTAMP"
STD_LOAD_TYPE = "load_type"
STD_RUN_ID = "run_id"

_HASH = re.compile(r"_[0-9a-f]{32}$", re.IGNORECASE)


@dataclass
class StandardSignal:
    """The connector metadata columns on a standard data product."""
    operation: Column
    timestamp: Column
    load_type: Column | None = None
    run_id: Column | None = None

    @property
    def hash_suffixed(self) -> bool:
        """Whether load_type carries the flow hash.

        Its absence is a strong hint that the object is a local copy rather than a
        live share: every corrupted object on the reference tenant had been copied
        into a STANDARD database, and every copy had lost the suffix.
        """
        return bool(self.load_type and _HASH.search(self.load_type.name))

    @property
    def flow_hash(self) -> str | None:
        if not self.load_type:
            return None
        m = _HASH.search(self.load_type.name)
        return m.group(0).lstrip("_") if m else None


def find_standard_signal(o: Obj) -> StandardSignal | None:
    """The connector metadata columns, if this is a standard data product."""
    op = o.column(STD_OPERATION)
    ts = o.column(STD_TIMESTAMP)
    if not op or not ts:
        return None
    pre = lambda p: next(                                     # noqa: E731
        (c for c in o.columns if c.name.lower().startswith(p)), None)
    return StandardSignal(operation=op, timestamp=ts,
                          load_type=pre(STD_LOAD_TYPE), run_id=pre(STD_RUN_ID))


def track_of(o: Obj) -> str:
    """Which CDC track an object belongs to.

    STANDARD  connector metadata columns are present
    CUSTOM    only a customer extension timestamp is present
    NONE      no change signal at all
    """
    if find_standard_signal(o):
        return "STANDARD"
    return "CUSTOM" if find_watermark(o) else "NONE"


def catalog_linked_databases(s: Session) -> list[str]:
    """Databases that BDC Connect created, found by kind rather than by name."""
    out = []
    for r in s.rows("SHOW DATABASES"):
        kind = str(r.get("kind") or r.get("KIND") or "").upper()
        if "CATALOG-LINKED" in kind or "CATALOG LINKED" in kind:
            out.append(str(r.get("name") or r.get("NAME")))
    return sorted(out)


def objects_in(s: Session, database: str) -> list[Obj]:
    out: list[Obj] = []
    for r in s.rows(f"SHOW TABLES IN DATABASE {fqn(database)}"):
        out.append(Obj(database=database,
                       schema=str(r.get("schema_name") or r.get("SCHEMA_NAME")),
                       name=str(r.get("name") or r.get("NAME"))))
    return out


def describe(s: Session, o: Obj) -> Obj:
    """Populate the column list. DESCRIBE is the only route on these objects.

    A share can advertise a table that has never been materialised, and describing one
    fails with "table 'refresh_table' is not initialized". That is a property of the
    share rather than a fault in the caller, so it is recorded on the object and the
    survey continues - one unlinked table must not stop an account-wide assessment.
    """
    try:
        o.columns = [Column(name=str(r.get("name")), type=str(r.get("type")))
                     for r in s.rows(f"DESCRIBE TABLE {o.fqn}")]
    except Exception as e:
        o.columns = []
        o.error = " ".join(str(e).split())
    return o


def find_watermark(o: Obj) -> Column | None:
    """The replication write timestamp, if the share carries one.

    Preference order: a known extension-field name, then any TIMESTAMP column whose
    name suggests load or change rather than a business event. A business date such as
    PostingDate is deliberately never returned - it is not a watermark, and using one
    as though it were is the most common and most expensive mistake in this pattern.
    """
    c = o.find(*WATERMARK_CANDIDATES)
    if c and c.is_temporal:
        return c
    if c:
        return c   # present but not typed as a timestamp; the caller should warn
    hints = ("lastchg", "lastchange", "loadts", "load_ts", "replicat", "extract",
             "ingest", "_ts", "updated")
    cands = [x for x in o.columns
             if x.is_temporal and any(h in x.name.lower() for h in hints)]
    return cands[0] if cands else None


def inventory(s: Session, describe_all: bool = True) -> list[Obj]:
    """Every object in every catalog-linked database, optionally with columns."""
    objs: list[Obj] = []
    for db in catalog_linked_databases(s):
        for o in objects_in(s, db):
            objs.append(describe(s, o) if describe_all else o)
    return objs
