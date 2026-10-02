# Masking and privileges — the two silent blockers

Both of these were hit while testing this skill against a live SAP BDC share.
Neither raises an error at the point where the damage is done.

## 1. Masked key columns collapse Silver

Catalog-linked databases can arrive with **tag-based masking policies** already
attached. On the test share, BDC Connect created a `PII` tag and a
`PII_MASK_STRING` policy inside the database's `SNOWFLAKE$` schema and applied
them to identifying columns — including the key column `Customer`:

```sql
CASE WHEN SYSTEM$GET_TAG('<CLD>.SNOWFLAKE$.UNMASK_PII', CURRENT_ROLE(), 'ROLE') = 'true'
     THEN VAL ELSE '********' END
```

ACCOUNTADMIN is **not** exempt. For any role without the unmask tag, every
`Customer` value reads as `********`, so:

| check | masked role | unmasked role |
| --- | --- | --- |
| rows in `customer` | 3,427 | 3,427 |
| `COUNT(DISTINCT "Customer")` | **1** | 3,427 |

A Silver dynamic table that de-duplicates with
`QUALIFY ROW_NUMBER() OVER (PARTITION BY "Customer" ...) = 1` would keep **one
row per masked value** — an entire customer master reduced to a single row, with
a successful refresh status. Gold built on it is wrong in the same silent way.

The dynamic table evaluates the policy **as its owner role**, so what matters is
the role that *owns* Silver, not the role you query with.

**Detect** — `scripts/verify_grain.py --role <capture role>` reports
`BLOCKED` when a key column is masked for that role. `profile_share.py` lists
every masked column per object. By hand:

```sql
SELECT REF_COLUMN_NAME, POLICY_NAME
FROM TABLE(<CLD>.INFORMATION_SCHEMA.POLICY_REFERENCES(
       REF_ENTITY_NAME => '<CLD>."<schema>"."<table>"', REF_ENTITY_DOMAIN => 'table'))
WHERE POLICY_KIND = 'MASKING_POLICY';
```

**Fix** — run capture as a dedicated role that is allowed to see the keys, and
tag it (needs the privilege to apply the tag; usually the CLD owner):

```sql
CREATE ROLE IF NOT EXISTS SAP_BDC_CAPTURE_ROLE;
ALTER ROLE SAP_BDC_CAPTURE_ROLE SET TAG <CLD>.SNOWFLAKE$.UNMASK_PII = 'true';
```

This is a **data-governance decision, not a technical workaround** — the role
will see PII. Agree it with whoever owns the policy. If PII must stay masked in
consumption, keep it masked in Gold with your own policy on the Gold columns;
the capture layer still needs real keys to de-duplicate correctly.

Masking can be added *after* a build is deployed (on the test share it appeared
weeks after the first build). Re-run `verify_grain.py --role <capture role>` as
part of daily assurance, and treat a sudden drop in distinct keys as masking
until proven otherwise.

## 2. Dynamic tables refresh with the owner's PRIMARY role only

A role can appear to read the share interactively and still fail every
dynamic-table refresh:

```
Object '<CLD>."customer"."customer"' does not exist or not authorized.
Your primary role ... must have at least one privilege granted on TABLE ...
(Note: the dynamic table runs as owner role ...)
```

Two causes, both seen in testing:

- **Iceberg tables need an Iceberg grant.** `GRANT SELECT ON ALL TABLES IN
  DATABASE <CLD>` grants nothing on a catalog-linked database, because its
  objects are Iceberg tables. Interactive queries still worked through
  *secondary* roles, which hides the problem until the first refresh.
- **The zero-copy connector needs USAGE** for table discovery and refresh when
  the role does not own the CLD.

Minimum grants for a capture role:

```sql
GRANT USAGE ON WAREHOUSE <wh>                               TO ROLE SAP_BDC_CAPTURE_ROLE;
GRANT CREATE DATABASE ON ACCOUNT                           TO ROLE SAP_BDC_CAPTURE_ROLE;  -- or own the target DB
GRANT EXECUTE TASK ON ACCOUNT                              TO ROLE SAP_BDC_CAPTURE_ROLE;  -- to resume the tasks
GRANT USAGE ON DATABASE <CLD>                              TO ROLE SAP_BDC_CAPTURE_ROLE;
GRANT USAGE ON ALL SCHEMAS IN DATABASE <CLD>               TO ROLE SAP_BDC_CAPTURE_ROLE;
GRANT SELECT ON ALL ICEBERG TABLES IN DATABASE <CLD>       TO ROLE SAP_BDC_CAPTURE_ROLE;
GRANT USAGE ON DATABASE <connector db>                     TO ROLE SAP_BDC_CAPTURE_ROLE;
GRANT USAGE ON SCHEMA <connector db>.<schema>              TO ROLE SAP_BDC_CAPTURE_ROLE;
GRANT USAGE ON ZEROCOPY CONNECTOR <connector fqn>          TO ROLE SAP_BDC_CAPTURE_ROLE;
```

Find the connector with `SHOW ZEROCOPY CONNECTORS IN ACCOUNT`. New objects
SAP adds to the product later need the Iceberg grant again (or a future grant
where your governance model allows it).

Test the role the way a dynamic table uses it — with secondary roles off:

```sql
USE ROLE SAP_BDC_CAPTURE_ROLE;
USE SECONDARY ROLES NONE;
SELECT COUNT(*) FROM <CLD>."<schema>"."<table>";
```
