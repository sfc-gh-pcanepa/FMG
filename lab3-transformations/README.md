# Lab 3: Medallion Architecture & Data Transformations

**Time**: ~25 minutes  
**Prerequisites**: Data share consumed (`FMG_SHARED_DATA` database exists)

⚠️ **This lab is INDEPENDENT** - run it in any order!

## What You'll See

| Feature | Why It Matters |
|---------|---------------|
| **Medallion Architecture** | Bronze → Silver → Gold data layers |
| **Dynamic Tables** | Auto-refreshing transformations - no ETL scheduling! |
| **Zero-Copy Cloning** | Instant dev environments, no extra storage |
| **SWAP** | Atomic blue/green deployments - zero downtime! |
| **Time Travel** | Query and recover historical data |

## Medallion Architecture Overview

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   BRONZE    │────▶│   SILVER    │────▶│    GOLD     │
│  Raw Data   │     │  Cleansed   │     │  Business   │
│  As-Is      │     │  Validated  │     │  Aggregates │
└─────────────┘     └─────────────┘     └─────────────┘

 • Landing zone      • Deduped         • KPIs & Metrics
 • Schema-on-read    • Typed           • Consumption-ready  
 • Audit trail       • Joined          • BI/ML optimized
```

## Why Snowflake for Medallion?

| Traditional Approach | Snowflake Approach |
|---------------------|-------------------|
| ❌ Airflow DAGs for each layer | ✅ Dynamic Tables auto-chain |
| ❌ Manual incremental logic | ✅ Automatic incremental refresh |
| ❌ Complex cluster management | ✅ Serverless compute |
| ❌ Separate Delta Lake layer | ✅ Native in Snowflake |

## Quick Start

1. Accept the data share and create `FMG_SHARED_DATA` database
2. Open Snowsight and create a new SQL Worksheet
3. Copy/paste `lab3_complete.sql`
4. Run each section and observe the results

## The "Wow" Moments

### Bronze Layer (Raw Data Landing)
```sql
-- Ingest raw JSON with full lineage
CREATE TABLE RAW_CUSTOMERS AS
SELECT 
    OBJECT_CONSTRUCT(*) AS _raw_data,
    'salesforce_crm' AS _source_system,
    CURRENT_TIMESTAMP() AS _ingested_at
FROM FMG_SHARED_DATA.FMG.CUSTOMERS;
```

### Silver Layer (Auto-Cleansing)
```sql
CREATE DYNAMIC TABLE SILVER.CUSTOMERS
    TARGET_LAG = '1 minute'  -- Auto-refresh!
AS
SELECT 
    _raw_data:CUSTOMER_ID::VARCHAR AS customer_id,
    UPPER(_raw_data:SEGMENT::VARCHAR) AS segment
FROM BRONZE.RAW_CUSTOMERS
QUALIFY ROW_NUMBER() OVER (...) = 1;  -- Deduplicated
```

### Gold Layer (Chained Dynamic Tables)
```sql
CREATE DYNAMIC TABLE GOLD.CUSTOMER_360
    TARGET_LAG = '2 minutes'
AS
SELECT ... FROM SILVER.CUSTOMERS  -- Auto-refreshes end-to-end!
JOIN SILVER.SUBSCRIPTIONS ...
```

### SWAP (Zero-Downtime Data Refresh)
```sql
-- Load new data into staging
CREATE TABLE RAW_CUSTOMERS_STAGING AS SELECT * FROM RAW_CUSTOMERS;
INSERT INTO RAW_CUSTOMERS_STAGING ...  -- Add new batch

-- Atomic swap - instant cutover, no downtime!
ALTER TABLE RAW_CUSTOMERS_STAGING SWAP WITH RAW_CUSTOMERS;

-- Dynamic Tables auto-refresh from new Bronze data!
```

## Key Takeaways

- **Medallion Architecture** provides clear data quality progression
- **Dynamic Tables** eliminate ETL complexity - no Airflow needed!
- **Chained Dynamic Tables** auto-refresh Bronze → Silver → Gold
- **Cloning is instant** regardless of data size
- **Time Travel** = built-in disaster recovery

## Other Labs

All labs are independent - try any of them!

- [Lab 1: Getting Started](../lab1-getting-started/)
- [Lab 2: Governance & FinOps](../lab2-governance-finops/)
- [Lab 4: AI & Cortex](../lab4-agents-intelligence/)
