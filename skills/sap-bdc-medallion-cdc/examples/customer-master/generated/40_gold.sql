/* ============================================================================
   40  Gold — conformed dimension and consumption marts
   ----------------------------------------------------------------------------
   Gold is where SAP's field names become business names. Silver deliberately
   preserved them (traceability, and no column silently dropped at the first
   layer that could drop one), which means Silver still needs double quotes.
   This file is the last place that quoting appears; everything a consumer or a
   BI tool touches from here on is UPPER_SNAKE_CASE and unquoted.

   The same two rules from Silver still apply and are worth restating because
   they are easy to lose at this layer, where the temptation to add "as of"
   columns and running counts is strongest:

     - REFRESH_MODE = INCREMENTAL is pinned explicitly. A FULL dynamic table
       cannot support change tracking, so one FULL object here would force
       anything built on top of it to FULL as well.
     - No SYSDATE(), CURRENT_DATE() or CURRENT_TIMESTAMP() anywhere. A single
       non-deterministic expression blocks incremental refresh for the whole
       object. Where an age or a lag is genuinely needed it belongs in a plain
       view over the dynamic table, not in the dynamic table — VW_CUSTOMER_RISK
       at the end of this file is that pattern.

   TARGET_LAG is an explicit backstop rather than DOWNSTREAM. DOWNSTREAM means
   "refresh only as needed to keep downstream dynamic tables fresh", and nothing
   downstream of Gold is a dynamic table — the consumers are queries and BI. So
   DOWNSTREAM here would mean these objects never refresh on their own. The
   watermark poll in 50_ops.sql drives the chain; the lag catches the case where the
   task is suspended.
   ========================================================================== */

USE DATABASE SAP_CUSTOMER_360;

/* ---------------------------------------------------------------------------
   DIM_CUSTOMER_360 — one row per customer, with the child objects rolled up.

   The rollups are pre-aggregated in CTEs and joined on the customer key, rather
   than joined at row level and aggregated afterwards. That ordering matters:
   customercompanycode holds up to 10,549 rows and customersalesareatax 39,462,
   so a row-level join before aggregation would fan the master out and every
   count taken from it would be wrong.
   --------------------------------------------------------------------------- */
CREATE OR REPLACE DYNAMIC TABLE GOLD.DIM_CUSTOMER_360
  TARGET_LAG   = '24 hours'
  WAREHOUSE    = COMPUTE_WH
  REFRESH_MODE = INCREMENTAL
  COMMENT      = 'Conformed customer dimension: master attributes plus company-code, sales-area, tax and dunning rollups. One row per customer.'
AS
WITH cc AS (
    SELECT "Customer"                                   AS CUSTOMER_ID,
           COUNT(*)                                     AS COMPANY_CODE_COUNT,
           COUNT(DISTINCT "CompanyCode")                AS DISTINCT_COMPANY_CODES,
           MAX(IFF(IS_PAYMENT_BLOCKED, 1, 0))           AS ANY_PAYMENT_BLOCKED,
           MAX(IFF(IS_DUNNING_BLOCKED, 1, 0))           AS ANY_CC_DUNNING_BLOCKED,
           MAX(CDC_TIMESTAMP_UTC)                       AS CC_LAST_CHANGED_UTC
    FROM   SILVER.DT_CUSTOMERCOMPANYCODE
    GROUP  BY 1
),
sa AS (
    SELECT "Customer"                                   AS CUSTOMER_ID,
           COUNT(*)                                     AS SALES_AREA_COUNT,
           COUNT(DISTINCT "SalesOrganization")           AS DISTINCT_SALES_ORGS,
           MAX(IFF(IS_SALES_BLOCKED, 1, 0))             AS ANY_SALES_BLOCKED,
           MIN("CustomerABCClassification")             AS ABC_CLASSIFICATION_MIN,
           MAX(CDC_TIMESTAMP_UTC)                       AS SA_LAST_CHANGED_UTC
    FROM   SILVER.DT_CUSTOMERSALESAREA
    GROUP  BY 1
),
tax AS (
    SELECT "Customer"                                   AS CUSTOMER_ID,
           COUNT(*)                                     AS TAX_CLASSIFICATION_COUNT,
           COUNT(DISTINCT "DepartureCountry")            AS DISTINCT_DEPARTURE_COUNTRIES,
           MAX(CDC_TIMESTAMP_UTC)                       AS TAX_LAST_CHANGED_UTC
    FROM   SILVER.DT_CUSTOMERSALESAREATAX
    GROUP  BY 1
),
dun AS (
    SELECT "Customer"                                   AS CUSTOMER_ID,
           COUNT(*)                                     AS DUNNING_RECORD_COUNT,
           MAX(DUNNING_LEVEL_NUM)                       AS MAX_DUNNING_LEVEL,
           MAX(IFF(IS_DUNNING_BLOCKED, 1, 0))           AS ANY_DUNNING_BLOCKED,
           MAX("LastDunnedOn")                          AS LAST_DUNNED_ON,
           MAX(CDC_TIMESTAMP_UTC)                       AS DUN_LAST_CHANGED_UTC
    FROM   SILVER.DT_CUSTOMERDUNNING
    GROUP  BY 1
)
SELECT
    /* --- Identity ------------------------------------------------------- */
    c."Customer"                                        AS CUSTOMER_ID,
    c."CustomerName"                                    AS CUSTOMER_NAME,
    c."CustomerFullName"                                AS CUSTOMER_FULL_NAME,
    c."CustomerCorporateGroup"                          AS CORPORATE_GROUP,
    c."CustomerAccountGroup"                            AS ACCOUNT_GROUP,
    c."CustomerClassification"                          AS CUSTOMER_CLASSIFICATION,
    c."Industry"                                        AS INDUSTRY,
    /* --- Location ------------------------------------------------------- */
    c."Country"                                         AS COUNTRY,
    c."Region"                                          AS REGION,
    c."CityName"                                        AS CITY_NAME,
    c."PostalCode"                                      AS POSTAL_CODE,
    c."StreetName"                                      AS STREET_NAME,
    c."County"                                          AS COUNTY,
    /* --- Tax and registration -------------------------------------------- */
    c."VATRegistration"                                 AS VAT_REGISTRATION,
    c."TaxJurisdiction"                                 AS TAX_JURISDICTION,
    /* --- Lifecycle ------------------------------------------------------- */
    c."CreationDate"                                    AS CREATED_ON,
    c."CreatedByUser"                                   AS CREATED_BY_USER,
    COALESCE(c."IsOneTimeAccount", FALSE)               AS IS_ONE_TIME_ACCOUNT,
    COALESCE(c."IsConsumer", FALSE)                     AS IS_CONSUMER,
    COALESCE(c."IsCompetitor", FALSE)                   AS IS_COMPETITOR,
    COALESCE(c."IsSalesProspect", FALSE)                AS IS_SALES_PROSPECT,
    c.IS_FLAGGED_DELETED                                AS IS_FLAGGED_DELETED,
    c.IS_BLOCKED_ANY                                    AS IS_BLOCKED_AT_MASTER,
    /* --- Rollups. COALESCE to 0 so "no company code" reads as zero rather
           than NULL, which is what a consumer filtering on > 0 expects. --- */
    COALESCE(cc.COMPANY_CODE_COUNT, 0)                  AS COMPANY_CODE_COUNT,
    COALESCE(cc.DISTINCT_COMPANY_CODES, 0)              AS DISTINCT_COMPANY_CODES,
    COALESCE(sa.SALES_AREA_COUNT, 0)                    AS SALES_AREA_COUNT,
    COALESCE(sa.DISTINCT_SALES_ORGS, 0)                 AS DISTINCT_SALES_ORGS,
    sa.ABC_CLASSIFICATION_MIN                           AS ABC_CLASSIFICATION,
    COALESCE(tax.TAX_CLASSIFICATION_COUNT, 0)           AS TAX_CLASSIFICATION_COUNT,
    COALESCE(tax.DISTINCT_DEPARTURE_COUNTRIES, 0)       AS DISTINCT_DEPARTURE_COUNTRIES,
    COALESCE(dun.DUNNING_RECORD_COUNT, 0)               AS DUNNING_RECORD_COUNT,
    dun.MAX_DUNNING_LEVEL                               AS MAX_DUNNING_LEVEL,
    dun.LAST_DUNNED_ON                                  AS LAST_DUNNED_ON,
    /* --- Derived state --------------------------------------------------- */
    IFF(COALESCE(cc.ANY_PAYMENT_BLOCKED, 0) = 1, TRUE, FALSE)   AS IS_PAYMENT_BLOCKED,
    IFF(COALESCE(sa.ANY_SALES_BLOCKED, 0) = 1, TRUE, FALSE)     AS IS_SALES_BLOCKED,
    IFF(COALESCE(cc.ANY_CC_DUNNING_BLOCKED, 0) = 1
        OR COALESCE(dun.ANY_DUNNING_BLOCKED, 0) = 1, TRUE, FALSE) AS IS_DUNNING_BLOCKED,
    IFF(COALESCE(sa.SALES_AREA_COUNT, 0) = 0
        AND COALESCE(cc.COMPANY_CODE_COUNT, 0) = 0, TRUE, FALSE) AS IS_UNASSIGNED,
    /* --- Data quality ---------------------------------------------------- */
    c.DQ_NAME_MISSING                                   AS DQ_NAME_MISSING,
    c.DQ_COUNTRY_MISSING                                AS DQ_COUNTRY_MISSING,
    /* --- Change lineage. The master's own change stamp, and the latest across
           the whole customer including its children, so a consumer can tell
           when anything about this customer last moved. -------------------- */
    c.CDC_OPERATION                                     AS CDC_OPERATION,
    c.CDC_LOAD_TYPE                                     AS CDC_LOAD_TYPE,
    c.CDC_TIMESTAMP_UTC                                 AS MASTER_CHANGED_UTC,
    GREATEST(
        c.CDC_TIMESTAMP_UTC,
        COALESCE(cc.CC_LAST_CHANGED_UTC,  c.CDC_TIMESTAMP_UTC),
        COALESCE(sa.SA_LAST_CHANGED_UTC,  c.CDC_TIMESTAMP_UTC),
        COALESCE(tax.TAX_LAST_CHANGED_UTC, c.CDC_TIMESTAMP_UTC),
        COALESCE(dun.DUN_LAST_CHANGED_UTC, c.CDC_TIMESTAMP_UTC)
    )                                                   AS ANY_CHANGED_UTC
FROM        SILVER.DT_CUSTOMER c
LEFT JOIN   cc  ON cc.CUSTOMER_ID  = c."Customer"
LEFT JOIN   sa  ON sa.CUSTOMER_ID  = c."Customer"
LEFT JOIN   tax ON tax.CUSTOMER_ID = c."Customer"
LEFT JOIN   dun ON dun.CUSTOMER_ID = c."Customer";


/* ---------------------------------------------------------------------------
   BRG_CUSTOMER_SALES_AREA — the sales-area bridge, at its own grain.

   Kept separate from the dimension on purpose. Rolling sales areas into
   DIM_CUSTOMER_360 answers "how many", but a consumer that needs to filter by
   sales organisation or distribution channel needs the rows. Joining this to
   the dimension is a fan-out by design, and being a distinct object makes that
   explicit rather than accidental.
   --------------------------------------------------------------------------- */
CREATE OR REPLACE DYNAMIC TABLE GOLD.BRG_CUSTOMER_SALES_AREA
  TARGET_LAG   = '24 hours'
  WAREHOUSE    = COMPUTE_WH
  REFRESH_MODE = INCREMENTAL
  COMMENT      = 'Customer-to-sales-area bridge with tax classification counts. Grain: Customer, SalesOrganization, DistributionChannel, Division.'
AS
WITH tax_by_area AS (
    SELECT "Customer"            AS CUSTOMER_ID,
           "SalesOrganization"   AS SALES_ORGANIZATION,
           "DistributionChannel" AS DISTRIBUTION_CHANNEL,
           "Division"            AS DIVISION,
           COUNT(*)              AS TAX_ROW_COUNT,
           COUNT(DISTINCT "DepartureCountry")    AS DISTINCT_DEPARTURE_COUNTRIES,
           COUNT(DISTINCT "CustomerTaxCategory") AS DISTINCT_TAX_CATEGORIES
    FROM   SILVER.DT_CUSTOMERSALESAREATAX
    GROUP  BY 1,2,3,4
)
SELECT
    s."Customer"                        AS CUSTOMER_ID,
    s."SalesOrganization"               AS SALES_ORGANIZATION,
    s."DistributionChannel"             AS DISTRIBUTION_CHANNEL,
    s."Division"                        AS DIVISION,
    s."SalesOffice"                     AS SALES_OFFICE,
    s."SalesGroup"                      AS SALES_GROUP,
    s."SalesDistrict"                   AS SALES_DISTRICT,
    s."CustomerGroup"                   AS CUSTOMER_GROUP,
    s."CustomerABCClassification"       AS ABC_CLASSIFICATION,
    s."Currency"                        AS CURRENCY,
    s."CustomerPriceGroup"              AS PRICE_GROUP,
    s."PriceListType"                   AS PRICE_LIST_TYPE,
    s."CustomerPaymentTerms"            AS PAYMENT_TERMS,
    s."DeliveryPriority"                AS DELIVERY_PRIORITY,
    s."ShippingCondition"               AS SHIPPING_CONDITION,
    s."IncotermsClassification"         AS INCOTERMS,
    s."CreditControlArea"               AS CREDIT_CONTROL_AREA,
    s."SupplyingPlant"                  AS SUPPLYING_PLANT,
    s.IS_SALES_BLOCKED                  AS IS_SALES_BLOCKED,
    s.IS_FLAGGED_DELETED                AS IS_FLAGGED_DELETED,
    COALESCE(t.TAX_ROW_COUNT, 0)                AS TAX_ROW_COUNT,
    COALESCE(t.DISTINCT_DEPARTURE_COUNTRIES, 0) AS DISTINCT_DEPARTURE_COUNTRIES,
    COALESCE(t.DISTINCT_TAX_CATEGORIES, 0)      AS DISTINCT_TAX_CATEGORIES,
    s.CDC_OPERATION                     AS CDC_OPERATION,
    s.CDC_TIMESTAMP_UTC                 AS CHANGED_UTC
FROM      SILVER.DT_CUSTOMERSALESAREA s
LEFT JOIN tax_by_area t
       ON  t.CUSTOMER_ID           = s."Customer"
       AND t.SALES_ORGANIZATION    = s."SalesOrganization"
       AND t.DISTRIBUTION_CHANNEL  = s."DistributionChannel"
       AND t.DIVISION              = s."Division";


/* ---------------------------------------------------------------------------
   VW_CUSTOMER_RISK — a plain view, deliberately.

   Everything here depends on "now": days since the customer was last dunned,
   how stale the capture is. Putting CURRENT_DATE() or SYSDATE() into a dynamic
   table would block its incremental refresh, so time-relative logic lives in a
   view over the dimension instead. The view costs nothing to keep current.
   --------------------------------------------------------------------------- */
CREATE OR REPLACE VIEW GOLD.VW_CUSTOMER_RISK
  COMMENT = 'Blocked, dunned and unassigned customers with time-relative measures. A view, not a dynamic table, because CURRENT_DATE would block incremental refresh.'
AS
SELECT
    CUSTOMER_ID,
    CUSTOMER_NAME,
    COUNTRY,
    ACCOUNT_GROUP,
    COMPANY_CODE_COUNT,
    SALES_AREA_COUNT,
    MAX_DUNNING_LEVEL,
    LAST_DUNNED_ON,
    DATEDIFF('day', LAST_DUNNED_ON, CURRENT_DATE())     AS DAYS_SINCE_DUNNED,
    IS_PAYMENT_BLOCKED,
    IS_SALES_BLOCKED,
    IS_DUNNING_BLOCKED,
    IS_BLOCKED_AT_MASTER,
    IS_FLAGGED_DELETED,
    IS_UNASSIGNED,
    ANY_CHANGED_UTC,
    DATEDIFF('minute', ANY_CHANGED_UTC, SYSDATE())      AS MINUTES_SINCE_CHANGE,
    /* A single ordering for review queues: hard blocks first, then dunning
       severity, then customers carrying no assignment at all. */
    CASE
      WHEN IS_FLAGGED_DELETED                    THEN 'DELETED'
      WHEN IS_PAYMENT_BLOCKED OR IS_SALES_BLOCKED
           OR IS_BLOCKED_AT_MASTER               THEN 'BLOCKED'
      WHEN COALESCE(MAX_DUNNING_LEVEL, 0) >= 3   THEN 'DUNNING_SEVERE'
      WHEN COALESCE(MAX_DUNNING_LEVEL, 0) > 0    THEN 'DUNNING'
      WHEN IS_UNASSIGNED                         THEN 'UNASSIGNED'
      ELSE 'OK'
    END                                                 AS RISK_BUCKET
FROM GOLD.DIM_CUSTOMER_360;
