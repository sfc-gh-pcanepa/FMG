/*=============================================================================
  FMG SUITE - LAB 3: MEDALLION ARCHITECTURE & DATA TRANSFORMATIONS
  
  What you'll see:
  ✅ Medallion Architecture - Bronze → Silver → Gold layers
  ✅ Dynamic Tables - auto-refreshing transformations (no scheduling!)
  ✅ Zero-copy cloning - instant dev environments
  ✅ Time Travel - query/recover historical data
  
  Time: ~25 minutes
  Prerequisites: Data share consumed (FMG_SHARED_DATA database exists)
  
  ⚠️  This lab is INDEPENDENT - run it in any order!
=============================================================================*/

/*
  ┌──────────────────────────────────────────────────────────────────────────┐
  │                      MEDALLION ARCHITECTURE OVERVIEW                      │
  │                                                                           │
  │   ┌─────────────┐     ┌─────────────┐     ┌─────────────┐                │
  │   │   BRONZE    │────▶│   SILVER    │────▶│    GOLD     │                │
  │   │  Raw Data   │     │  Cleansed   │     │  Business   │                │
  │   │  As-Is      │     │  Validated  │     │  Aggregates │                │
  │   └─────────────┘     └─────────────┘     └─────────────┘                │
  │                                                                           │
  │   • Landing zone       • Deduped        • KPIs & Metrics                 │
  │   • Schema-on-read     • Typed          • Consumption-ready              │
  │   • Audit trail        • Joined         • BI/ML optimized                │
  └──────────────────────────────────────────────────────────────────────────┘
  
  WHY SNOWFLAKE + DYNAMIC TABLES FOR MEDALLION?
  • Dynamic Tables auto-refresh across layers - no Airflow/Orchestration needed
  • Snowflake manages incremental processing automatically
  • Built-in freshness SLAs (TARGET_LAG)
  • Simple SQL declarations, not complex ETL code
*/

-- ============================================================================
-- SETUP: CREATE LAB ENVIRONMENT FROM SHARED DATA
-- ============================================================================
USE ROLE ACCOUNTADMIN;

-- Create lab database with Medallion schema structure
CREATE DATABASE IF NOT EXISTS FMG_LAB3;
CREATE SCHEMA IF NOT EXISTS FMG_LAB3.BRONZE COMMENT = 'Raw data landing zone';
CREATE SCHEMA IF NOT EXISTS FMG_LAB3.SILVER COMMENT = 'Cleansed, validated data';
CREATE SCHEMA IF NOT EXISTS FMG_LAB3.GOLD   COMMENT = 'Business aggregates';

-- Create warehouse
CREATE WAREHOUSE IF NOT EXISTS FMG_ANALYTICS_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

-- Create role
CREATE ROLE IF NOT EXISTS FMG_ADMIN;
GRANT ROLE FMG_ADMIN TO ROLE ACCOUNTADMIN;
GRANT USAGE ON WAREHOUSE FMG_ANALYTICS_WH TO ROLE FMG_ADMIN;
GRANT ALL ON DATABASE FMG_LAB3 TO ROLE FMG_ADMIN;

USE ROLE FMG_ADMIN;
USE WAREHOUSE FMG_ANALYTICS_WH;

-- ============================================================================
-- STEP 1: BRONZE LAYER - Raw Data Ingestion
-- ============================================================================
/*
    BRONZE LAYER PRINCIPLES:
    • Raw data exactly as received from source systems
    • Append-only (preserve history)
    • Include metadata: ingestion timestamp, source, batch ID
    • Schema-on-read flexibility
*/

USE SCHEMA FMG_LAB3.BRONZE;

-- Ingest raw data as VARIANT (simulating JSON ingestion from source systems)
CREATE OR REPLACE TABLE RAW_CUSTOMERS AS
SELECT 
    OBJECT_CONSTRUCT(*) AS _raw_data,
    'salesforce_crm' AS _source_system,
    CURRENT_TIMESTAMP() AS _ingested_at,
    'batch_2024_001' AS _batch_id
FROM FMG_SHARED_DATA.FMG.CUSTOMERS;

CREATE OR REPLACE TABLE RAW_SUBSCRIPTIONS AS
SELECT 
    OBJECT_CONSTRUCT(*) AS _raw_data,
    'stripe_billing' AS _source_system,
    CURRENT_TIMESTAMP() AS _ingested_at,
    'batch_2024_001' AS _batch_id
FROM FMG_SHARED_DATA.FMG.SUBSCRIPTIONS;

CREATE OR REPLACE TABLE RAW_USERS AS
SELECT 
    OBJECT_CONSTRUCT(*) AS _raw_data,
    'identity_system' AS _source_system,
    CURRENT_TIMESTAMP() AS _ingested_at,
    'batch_2024_001' AS _batch_id
FROM FMG_SHARED_DATA.FMG.USERS;

-- Add a duplicate to demonstrate Silver deduplication
INSERT INTO RAW_CUSTOMERS
SELECT 
    OBJECT_CONSTRUCT('CUSTOMER_ID', 'C001', 'COMPANY_NAME', 'Acme Financial Advisors', 
                     'SEGMENT', 'Enterprise', 'INDUSTRY', 'RIA', 'MRR', 2600.00, 
                     'HEALTH_SCORE', 87, 'CREATED_DATE', '2022-01-15'),
    'salesforce_crm',
    CURRENT_TIMESTAMP(),
    'batch_2024_002';  -- Later batch with updated data

-- View raw Bronze data
SELECT * FROM RAW_CUSTOMERS LIMIT 5;

-- 🎯 Key insight: Bronze preserves raw data exactly as received, with full lineage

-- ============================================================================
-- STEP 2: SILVER LAYER - Cleansed & Validated (Dynamic Tables!)
-- ============================================================================
/*
    SILVER LAYER PRINCIPLES:
    • Cleansed, validated, deduplicated data
    • Strongly typed columns (extracted from VARIANT)
    • Standardized values (e.g., UPPER case for consistency)
    • Business keys for joining across entities
    
    Using Dynamic Tables: Auto-refreshes when Bronze changes!
*/

USE SCHEMA FMG_LAB3.SILVER;

-- SILVER: Cleansed Customers (deduped, typed, validated)
CREATE OR REPLACE DYNAMIC TABLE CUSTOMERS
    TARGET_LAG = '1 minute'
    WAREHOUSE = FMG_ANALYTICS_WH
AS
SELECT 
    _raw_data:CUSTOMER_ID::VARCHAR AS customer_id,
    _raw_data:COMPANY_NAME::VARCHAR AS company_name,
    UPPER(_raw_data:SEGMENT::VARCHAR) AS segment,
    _raw_data:INDUSTRY::VARCHAR AS industry,
    _raw_data:MRR::DECIMAL(10,2) AS mrr,
    _raw_data:HEALTH_SCORE::INTEGER AS health_score,
    TRY_TO_DATE(_raw_data:CREATED_DATE::VARCHAR) AS created_date,
    _source_system,
    _ingested_at,
    CASE 
        WHEN _raw_data:HEALTH_SCORE::INTEGER BETWEEN 0 AND 100 THEN 'VALID'
        ELSE 'INVALID_HEALTH_SCORE'
    END AS _dq_status
FROM FMG_LAB3.BRONZE.RAW_CUSTOMERS
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY _raw_data:CUSTOMER_ID::VARCHAR 
    ORDER BY _ingested_at DESC
) = 1;

-- SILVER: Cleansed Subscriptions
CREATE OR REPLACE DYNAMIC TABLE SUBSCRIPTIONS
    TARGET_LAG = '1 minute'
    WAREHOUSE = FMG_ANALYTICS_WH
AS
SELECT 
    _raw_data:SUBSCRIPTION_ID::VARCHAR AS subscription_id,
    _raw_data:CUSTOMER_ID::VARCHAR AS customer_id,
    _raw_data:PRODUCT::VARCHAR AS product_name,
    _raw_data:MRR::DECIMAL(10,2) AS mrr,
    UPPER(_raw_data:STATUS::VARCHAR) AS status,
    TRY_TO_DATE(_raw_data:START_DATE::VARCHAR) AS start_date,
    _source_system,
    _ingested_at
FROM FMG_LAB3.BRONZE.RAW_SUBSCRIPTIONS;

-- SILVER: Cleansed Users
CREATE OR REPLACE DYNAMIC TABLE USERS
    TARGET_LAG = '1 minute'
    WAREHOUSE = FMG_ANALYTICS_WH
AS
SELECT 
    _raw_data:USER_ID::VARCHAR AS user_id,
    _raw_data:CUSTOMER_ID::VARCHAR AS customer_id,
    _raw_data:EMAIL::VARCHAR AS email,
    _raw_data:PHONE::VARCHAR AS phone,
    _raw_data:FULL_NAME::VARCHAR AS full_name,
    _raw_data:ROLE::VARCHAR AS user_role,
    _source_system,
    _ingested_at
FROM FMG_LAB3.BRONZE.RAW_USERS;

-- Verify Silver layer (notice: C001 is deduplicated with latest MRR)
SELECT * FROM CUSTOMERS ORDER BY customer_id;

-- 🎯 Key insight: Silver auto-refreshes when Bronze changes. No ETL scheduling!

-- ============================================================================
-- STEP 3: GOLD LAYER - Business Aggregates (Chained Dynamic Tables!)
-- ============================================================================
/*
    GOLD LAYER PRINCIPLES:
    • Business-ready aggregates and KPIs
    • Optimized for consumption (BI tools, ML models)
    • Denormalized for query performance
    • Domain-specific data products
    
    Chained Dynamic Tables: Gold reads from Silver, which reads from Bronze!
*/

USE SCHEMA FMG_LAB3.GOLD;

-- GOLD: Customer 360 View
CREATE OR REPLACE DYNAMIC TABLE CUSTOMER_360
    TARGET_LAG = '2 minutes'
    WAREHOUSE = FMG_ANALYTICS_WH
AS
SELECT 
    c.customer_id,
    c.company_name,
    c.segment,
    c.industry,
    c.health_score,
    c.created_date,
    DATEDIFF('month', c.created_date, CURRENT_DATE()) AS tenure_months,
    
    -- Subscription aggregates
    COUNT(DISTINCT s.subscription_id) AS product_count,
    SUM(s.mrr) AS total_mrr,
    SUM(s.mrr) * 12 AS total_arr,
    LISTAGG(DISTINCT s.product_name, ', ') WITHIN GROUP (ORDER BY s.product_name) AS products_owned,
    
    -- User metrics
    COUNT(DISTINCT u.user_id) AS user_count,
    
    -- Health classification
    CASE 
        WHEN c.health_score >= 80 THEN 'HEALTHY'
        WHEN c.health_score >= 60 THEN 'AT RISK'
        ELSE 'CRITICAL'
    END AS health_status,
    
    CURRENT_TIMESTAMP() AS _refreshed_at

FROM FMG_LAB3.SILVER.CUSTOMERS c
LEFT JOIN FMG_LAB3.SILVER.SUBSCRIPTIONS s 
    ON c.customer_id = s.customer_id AND s.status = 'ACTIVE'
LEFT JOIN FMG_LAB3.SILVER.USERS u 
    ON c.customer_id = u.customer_id
GROUP BY 
    c.customer_id, c.company_name, c.segment, c.industry,
    c.health_score, c.created_date;

-- GOLD: Revenue Summary by Segment
CREATE OR REPLACE DYNAMIC TABLE REVENUE_BY_SEGMENT
    TARGET_LAG = '2 minutes'
    WAREHOUSE = FMG_ANALYTICS_WH
AS
SELECT 
    c.segment,
    COUNT(DISTINCT c.customer_id) AS customer_count,
    SUM(s.mrr) AS total_mrr,
    SUM(s.mrr) * 12 AS total_arr,
    AVG(s.mrr) AS avg_mrr_per_subscription,
    COUNT(DISTINCT s.subscription_id) AS subscription_count,
    AVG(c.health_score) AS avg_health_score,
    CURRENT_TIMESTAMP() AS _refreshed_at
FROM FMG_LAB3.SILVER.CUSTOMERS c
LEFT JOIN FMG_LAB3.SILVER.SUBSCRIPTIONS s 
    ON c.customer_id = s.customer_id AND s.status = 'ACTIVE'
GROUP BY c.segment;

-- GOLD: Product Performance
CREATE OR REPLACE DYNAMIC TABLE PRODUCT_METRICS
    TARGET_LAG = '2 minutes'
    WAREHOUSE = FMG_ANALYTICS_WH
AS
SELECT 
    s.product_name,
    COUNT(DISTINCT s.customer_id) AS customer_count,
    COUNT(s.subscription_id) AS subscription_count,
    SUM(CASE WHEN s.status = 'ACTIVE' THEN s.mrr ELSE 0 END) AS active_mrr,
    SUM(CASE WHEN s.status = 'CANCELLED' THEN s.mrr ELSE 0 END) AS churned_mrr,
    ROUND(
        SUM(CASE WHEN s.status = 'CANCELLED' THEN 1 ELSE 0 END) * 100.0 / 
        NULLIF(COUNT(*), 0), 2
    ) AS churn_rate_pct,
    CURRENT_TIMESTAMP() AS _refreshed_at
FROM FMG_LAB3.SILVER.SUBSCRIPTIONS s
GROUP BY s.product_name;

-- Query Gold layer - ready for executive dashboards!
SELECT * FROM CUSTOMER_360 ORDER BY total_mrr DESC;
SELECT * FROM REVENUE_BY_SEGMENT ORDER BY total_arr DESC;
SELECT * FROM PRODUCT_METRICS ORDER BY active_mrr DESC;

-- 🎯 Key insight: Three-layer chain refreshes automatically. Bronze → Silver → Gold!

-- ============================================================================
-- STEP 4: SEE MEDALLION AUTO-REFRESH IN ACTION
-- ============================================================================

-- Add new data to Bronze (simulating a new data load)
USE SCHEMA FMG_LAB3.BRONZE;

INSERT INTO RAW_CUSTOMERS (_raw_data, _source_system, _ingested_at, _batch_id)
SELECT 
    OBJECT_CONSTRUCT('CUSTOMER_ID', 'C010', 'COMPANY_NAME', 'Pinnacle Wealth Management', 
                     'SEGMENT', 'Enterprise', 'INDUSTRY', 'RIA', 'MRR', 3500.00, 
                     'HEALTH_SCORE', 91, 'CREATED_DATE', CURRENT_DATE()),
    'salesforce_crm',
    CURRENT_TIMESTAMP(),
    'batch_2024_003';

INSERT INTO RAW_SUBSCRIPTIONS (_raw_data, _source_system, _ingested_at, _batch_id)
SELECT 
    OBJECT_CONSTRUCT('SUBSCRIPTION_ID', 'S020', 'CUSTOMER_ID', 'C010', 
                     'PRODUCT', 'Marketing Suite', 'MRR', 2000.00, 
                     'STATUS', 'Active', 'START_DATE', CURRENT_DATE()),
    'stripe_billing',
    CURRENT_TIMESTAMP(),
    'batch_2024_003';

-- Wait a moment, then check - all layers refresh automatically!
SELECT 'SILVER' AS layer, COUNT(*) AS customer_count FROM FMG_LAB3.SILVER.CUSTOMERS
UNION ALL
SELECT 'GOLD', COUNT(*) FROM FMG_LAB3.GOLD.CUSTOMER_360;

-- Check the new customer flows through
SELECT * FROM FMG_LAB3.GOLD.CUSTOMER_360 WHERE customer_id = 'C010';

-- View refresh history
SELECT 
    name AS table_name,
    schema_name AS layer,
    refresh_start_time,
    DATEDIFF('second', refresh_start_time, refresh_end_time) AS refresh_seconds
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
WHERE schema_name IN ('SILVER', 'GOLD')
ORDER BY refresh_start_time DESC
LIMIT 10;

-- ============================================================================
-- STEP 5: ZERO-COPY CLONING (Clone Entire Medallion Architecture!)
-- ============================================================================
USE ROLE ACCOUNTADMIN;

-- Clone the entire Medallion architecture for development - INSTANT!
CREATE DATABASE FMG_LAB3_DEV CLONE FMG_LAB3;

-- Verify all three layers are cloned
SHOW SCHEMAS IN DATABASE FMG_LAB3_DEV;

-- The clone is independent - experiment without affecting production
DELETE FROM FMG_LAB3_DEV.BRONZE.RAW_CUSTOMERS 
WHERE _raw_data:SEGMENT::VARCHAR = 'SMB';

-- Original is untouched
SELECT 'DEV' AS env, COUNT(*) AS count FROM FMG_LAB3_DEV.BRONZE.RAW_CUSTOMERS
UNION ALL
SELECT 'PROD', COUNT(*) FROM FMG_LAB3.BRONZE.RAW_CUSTOMERS;

-- 🎯 Key insight: Clone is instant regardless of data size. Only stores the DELTA.

-- ============================================================================
-- STEP 5b: SWAP TABLES (Blue/Green Deployments!)
-- ============================================================================
/*
    SWAP is perfect for:
    • Blue/Green deployments - test in dev, swap to prod atomically
    • Schema migrations - build new table, swap when ready
    • Data refreshes - load into staging, swap to production
    
    The swap is ATOMIC - no downtime, no partial states!
*/

-- Scenario: We've improved our PRODUCT_METRICS logic in DEV and want to promote to PROD

-- First, let's enhance the DEV version with a new column
USE DATABASE FMG_LAB3_DEV;
USE SCHEMA GOLD;

-- Create an improved version of the Gold table in DEV
CREATE OR REPLACE TABLE PRODUCT_METRICS_V2 AS
SELECT 
    s.product_name,
    COUNT(DISTINCT s.customer_id) AS customer_count,
    COUNT(s.subscription_id) AS subscription_count,
    SUM(CASE WHEN s.status = 'ACTIVE' THEN s.mrr ELSE 0 END) AS active_mrr,
    SUM(CASE WHEN s.status = 'CANCELLED' THEN s.mrr ELSE 0 END) AS churned_mrr,
    ROUND(
        SUM(CASE WHEN s.status = 'CANCELLED' THEN 1 ELSE 0 END) * 100.0 / 
        NULLIF(COUNT(*), 0), 2
    ) AS churn_rate_pct,
    -- NEW: Added revenue tier classification
    CASE 
        WHEN SUM(CASE WHEN s.status = 'ACTIVE' THEN s.mrr ELSE 0 END) > 5000 THEN 'HIGH'
        WHEN SUM(CASE WHEN s.status = 'ACTIVE' THEN s.mrr ELSE 0 END) > 2000 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS revenue_tier,
    CURRENT_TIMESTAMP() AS _refreshed_at
FROM FMG_LAB3_DEV.SILVER.SUBSCRIPTIONS s
GROUP BY s.product_name;

-- Verify the new table in DEV
SELECT * FROM FMG_LAB3_DEV.GOLD.PRODUCT_METRICS_V2;

-- Now SWAP the tables atomically in PROD!
USE DATABASE FMG_LAB3;
USE SCHEMA GOLD;

-- First, clone the improved table from DEV to PROD
CREATE OR REPLACE TABLE PRODUCT_METRICS_NEW CLONE FMG_LAB3_DEV.GOLD.PRODUCT_METRICS_V2;

-- SWAP: Atomically replace the old table with the new one
ALTER TABLE PRODUCT_METRICS_NEW SWAP WITH PRODUCT_METRICS;

-- Verify: PROD now has the new schema with revenue_tier column!
SELECT * FROM FMG_LAB3.GOLD.PRODUCT_METRICS;

-- The old version is now in PRODUCT_METRICS_NEW (can drop or keep as backup)
-- DROP TABLE PRODUCT_METRICS_NEW;  -- Uncomment to clean up

-- 🎯 Key insight: SWAP is atomic - zero downtime, instant cutover!

/*
    SWAP USE CASES:
    
    1. BLUE/GREEN DEPLOYMENT:
       - Build new version in dev/staging
       - Test thoroughly
       - SWAP to production atomically
       
    2. LARGE DATA REFRESH:
       - Load new data into _STAGING table
       - SWAP with production when complete
       - No partial states visible to users
       
    3. SCHEMA MIGRATION:
       - Create new table with updated schema
       - Backfill data
       - SWAP to go live instantly
       
    4. ROLLBACK:
       - Keep old table after swap
       - If issues, SWAP back immediately
*/

-- ============================================================================
-- STEP 6: TIME TRAVEL (Query Historical Medallion States!)
-- ============================================================================
USE ROLE FMG_ADMIN;
USE DATABASE FMG_LAB3;

-- "Accidentally" delete critical data from Bronze
DELETE FROM BRONZE.RAW_CUSTOMERS WHERE _raw_data:SEGMENT::VARCHAR = 'Enterprise';

-- Oh no! Check current state
SELECT _raw_data:SEGMENT::VARCHAR AS segment, COUNT(*) 
FROM BRONZE.RAW_CUSTOMERS 
GROUP BY 1;

-- Time Travel: See what it looked like before
SELECT _raw_data:SEGMENT::VARCHAR AS segment, COUNT(*) 
FROM BRONZE.RAW_CUSTOMERS AT(OFFSET => -60)
GROUP BY 1;

-- Restore the deleted data
INSERT INTO BRONZE.RAW_CUSTOMERS
SELECT * FROM BRONZE.RAW_CUSTOMERS AT(OFFSET => -60)
WHERE _raw_data:SEGMENT::VARCHAR = 'Enterprise';

-- Verify restoration
SELECT _raw_data:SEGMENT::VARCHAR AS segment, COUNT(*) 
FROM BRONZE.RAW_CUSTOMERS 
GROUP BY 1;

-- ============================================================================
-- CLEANUP (Optional)
-- ============================================================================
USE ROLE ACCOUNTADMIN;
DROP DATABASE IF EXISTS FMG_LAB3_DEV;

-- To fully clean up:
-- DROP DATABASE FMG_LAB3;

-- ============================================================================
-- 🎉 LAB 3 COMPLETE!
-- ============================================================================
/*
  MEDALLION ARCHITECTURE SUMMARY:
  
  ┌─────────────────────────────────────────────────────────────────────────┐
  │ LAYER    │ PURPOSE              │ SNOWFLAKE IMPLEMENTATION              │
  ├─────────────────────────────────────────────────────────────────────────┤
  │ 🥉 BRONZE │ Raw data landing    │ Tables with VARIANT columns           │
  │           │ Schema-on-read      │ Append-only, full audit trail         │
  ├─────────────────────────────────────────────────────────────────────────┤
  │ 🥈 SILVER │ Cleansed/Validated  │ Dynamic Tables (auto-refresh!)        │
  │           │ Typed, deduplicated │ TARGET_LAG for freshness SLA          │
  ├─────────────────────────────────────────────────────────────────────────┤
  │ 🥇 GOLD   │ Business aggregates │ Chained Dynamic Tables                │
  │           │ Analytics-ready     │ Optimized for BI/ML consumption       │
  └─────────────────────────────────────────────────────────────────────────┘
  
  KEY SNOWFLAKE DIFFERENTIATORS:
  
  ✅ Dynamic Tables auto-refresh across layers - NO Airflow/Orchestration
  ✅ Incremental processing is automatic - Snowflake handles it
  ✅ Built-in freshness SLAs with TARGET_LAG  
  ✅ Zero-copy cloning for instant dev/test of entire architecture
  ✅ SWAP for atomic blue/green deployments - zero downtime cutover
  ✅ Time Travel across all layers for audit and recovery
  ✅ Single platform - no separate tools for each layer
  
  COMPARED TO TRADITIONAL MEDALLION IMPLEMENTATIONS:
  
  Traditional (Spark/Databricks):     Snowflake:
  ─────────────────────────────       ─────────────────────
  ❌ Airflow DAGs for each layer      ✅ Dynamic Tables auto-chain
  ❌ Manual incremental logic         ✅ Automatic incremental refresh
  ❌ Complex cluster management       ✅ Serverless compute
  ❌ Separate Delta Lake layer        ✅ Native in Snowflake
  
  Ready for more? Try any other lab - they're all independent!
*/
