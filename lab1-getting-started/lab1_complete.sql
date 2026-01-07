/*=============================================================================
  FMG SUITE - LAB 1: GETTING STARTED WITH SNOWFLAKE
  
  What you'll see:
  ✅ Create roles with different privileges in seconds
  ✅ Separation of compute (multiple warehouses)
  ✅ Instant warehouse resizing
  ✅ Role-based access control in action
  
  Time: ~20 minutes
  Prerequisites: Data share consumed (FMG_SHARED_DATA database exists)
  
  ⚠️  This lab is INDEPENDENT - run it in any order!
=============================================================================*/

-- ============================================================================
-- SETUP: CREATE LAB ENVIRONMENT FROM SHARED DATA
-- ============================================================================
USE ROLE ACCOUNTADMIN;

-- Create lab-specific database (copies from shared data)
CREATE DATABASE IF NOT EXISTS FMG_LAB1;
CREATE SCHEMA IF NOT EXISTS FMG_LAB1.PRODUCTION;

-- Create warehouses for different workloads
CREATE WAREHOUSE IF NOT EXISTS FMG_ANALYTICS_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    COMMENT = 'For BI and reporting workloads';

CREATE WAREHOUSE IF NOT EXISTS FMG_LOADING_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    COMMENT = 'For data loading and ETL';

-- Create roles
CREATE ROLE IF NOT EXISTS FMG_ADMIN;
CREATE ROLE IF NOT EXISTS FMG_ANALYST;
CREATE ROLE IF NOT EXISTS FMG_ENGINEER;

-- Set up role hierarchy
GRANT ROLE FMG_ANALYST TO ROLE FMG_ADMIN;
GRANT ROLE FMG_ENGINEER TO ROLE FMG_ADMIN;
GRANT ROLE FMG_ADMIN TO ROLE ACCOUNTADMIN;

-- Grant warehouse access
GRANT USAGE ON WAREHOUSE FMG_ANALYTICS_WH TO ROLE FMG_ADMIN;
GRANT USAGE ON WAREHOUSE FMG_ANALYTICS_WH TO ROLE FMG_ANALYST;
GRANT USAGE ON WAREHOUSE FMG_LOADING_WH TO ROLE FMG_ENGINEER;

-- Grant database access
GRANT ALL ON DATABASE FMG_LAB1 TO ROLE FMG_ADMIN;
GRANT USAGE ON DATABASE FMG_LAB1 TO ROLE FMG_ANALYST;
GRANT USAGE ON DATABASE FMG_LAB1 TO ROLE FMG_ENGINEER;
GRANT USAGE ON SCHEMA FMG_LAB1.PRODUCTION TO ROLE FMG_ANALYST;
GRANT USAGE ON SCHEMA FMG_LAB1.PRODUCTION TO ROLE FMG_ENGINEER;
GRANT ALL ON SCHEMA FMG_LAB1.PRODUCTION TO ROLE FMG_ADMIN;

USE WAREHOUSE FMG_ANALYTICS_WH;
USE SCHEMA FMG_LAB1.PRODUCTION;

-- Copy data from share into lab database
CREATE OR REPLACE TABLE CUSTOMERS AS SELECT * FROM FMG_SHARED_DATA.FMG.CUSTOMERS;
CREATE OR REPLACE TABLE SUBSCRIPTIONS AS SELECT * FROM FMG_SHARED_DATA.FMG.SUBSCRIPTIONS;

-- Grant table access by role
GRANT SELECT ON ALL TABLES IN SCHEMA FMG_LAB1.PRODUCTION TO ROLE FMG_ANALYST;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA FMG_LAB1.PRODUCTION TO ROLE FMG_ENGINEER;
GRANT ALL ON ALL TABLES IN SCHEMA FMG_LAB1.PRODUCTION TO ROLE FMG_ADMIN;

-- Verify data loaded
SELECT 'CUSTOMERS' AS table_name, COUNT(*) AS Number_rows FROM CUSTOMERS
UNION ALL
SELECT 'SUBSCRIPTIONS', COUNT(*) FROM SUBSCRIPTIONS;

-- ============================================================================
-- STEP 1: EXPLORE SEPARATION OF COMPUTE
-- ============================================================================

-- You have two INDEPENDENT warehouses:
-- FMG_ANALYTICS_WH - For BI and reporting
-- FMG_LOADING_WH - For data loading/ETL

-- Key insight: These run independently - no resource contention!
SHOW WAREHOUSES LIKE 'FMG%';

-- ============================================================================
-- STEP 2: PERFORMANCE COMPARISON - XSMALL vs MEDIUM WAREHOUSE
-- ============================================================================

/*
    Let's run a complex analytical query and compare performance.
    This query does:
    - Multiple JOINs across tables
    - Window functions for ranking
    - Multiple aggregations
    - Self-join for cohort analysis
*/

-- First, ensure we're on XSMALL
ALTER WAREHOUSE FMG_ANALYTICS_WH SET WAREHOUSE_SIZE = 'XSMALL';
ALTER WAREHOUSE FMG_ANALYTICS_WH SUSPEND;
ALTER WAREHOUSE FMG_ANALYTICS_WH RESUME;

-- ═══════════════════════════════════════════════════════════════════════════
-- RUN 1: Complex Query on XSMALL (note the execution time!)
-- ═══════════════════════════════════════════════════════════════════════════

SELECT 
    '⏱️ XSMALL WAREHOUSE' AS test_run,
    CURRENT_TIMESTAMP() AS started_at;

-- Complex analytical query
WITH customer_metrics AS (
    SELECT 
        c.customer_id,
        c.company_name,
        c.segment,
        c.industry,
        c.health_score,
        c.created_date,
        COUNT(s.subscription_id) AS product_count,
        SUM(s.mrr) AS total_mrr,
        SUM(CASE WHEN s.status = 'Active' THEN s.mrr ELSE 0 END) AS active_mrr,
        SUM(CASE WHEN s.status = 'Cancelled' THEN s.mrr ELSE 0 END) AS churned_mrr,
        MIN(s.start_date) AS first_subscription,
        MAX(s.start_date) AS latest_subscription
    FROM CUSTOMERS c
    LEFT JOIN SUBSCRIPTIONS s ON c.customer_id = s.customer_id
    GROUP BY c.customer_id, c.company_name, c.segment, c.industry, c.health_score, c.created_date
),
ranked_customers AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY total_mrr DESC) AS segment_rank,
        PERCENT_RANK() OVER (PARTITION BY segment ORDER BY total_mrr) AS mrr_percentile,
        AVG(total_mrr) OVER (PARTITION BY segment) AS segment_avg_mrr,
        SUM(total_mrr) OVER (PARTITION BY segment) AS segment_total_mrr,
        COUNT(*) OVER (PARTITION BY segment) AS segment_customer_count,
        LAG(total_mrr) OVER (PARTITION BY segment ORDER BY total_mrr DESC) AS prev_customer_mrr,
        LEAD(total_mrr) OVER (PARTITION BY segment ORDER BY total_mrr DESC) AS next_customer_mrr
    FROM customer_metrics
),
segment_summary AS (
    SELECT 
        segment,
        industry,
        COUNT(DISTINCT customer_id) AS customers,
        SUM(total_mrr) AS mrr,
        AVG(health_score) AS avg_health,
        AVG(product_count) AS avg_products,
        SUM(churned_mrr) / NULLIF(SUM(total_mrr), 0) * 100 AS churn_rate
    FROM ranked_customers
    GROUP BY segment, industry
),
cross_segment AS (
    -- Cross-join for segment comparison matrix
    SELECT 
        a.segment AS segment_a,
        b.segment AS segment_b,
        a.mrr AS mrr_a,
        b.mrr AS mrr_b,
        a.mrr - b.mrr AS mrr_diff
    FROM segment_summary a
    CROSS JOIN segment_summary b
    WHERE a.segment != b.segment
)
SELECT 
    r.segment,
    r.industry,
    COUNT(*) AS customer_count,
    SUM(r.total_mrr) AS total_mrr,
    ROUND(AVG(r.health_score), 2) AS avg_health,
    ROUND(AVG(r.mrr_percentile), 4) AS avg_percentile,
    SUM(CASE WHEN r.segment_rank <= 10 THEN 1 ELSE 0 END) AS top_10_customers,
    ROUND(AVG(r.segment_avg_mrr), 2) AS segment_benchmark_mrr
FROM ranked_customers r
JOIN segment_summary s ON r.segment = s.segment AND r.industry = s.industry
GROUP BY r.segment, r.industry
HAVING COUNT(*) > 5
ORDER BY total_mrr DESC;

-- Check query history for timing
SELECT 
    query_id,
    warehouse_size,
    ROUND(total_elapsed_time / 1000, 2) AS seconds,
    rows_produced
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE query_text ILIKE '%customer_metrics%'
  AND query_text NOT ILIKE '%INFORMATION_SCHEMA%'
ORDER BY start_time DESC
LIMIT 1;

-- ═══════════════════════════════════════════════════════════════════════════
-- RUN 2: Same Query on MEDIUM (2x the compute power!)
-- ═══════════════════════════════════════════════════════════════════════════

-- Resize to MEDIUM - happens in seconds!
ALTER WAREHOUSE FMG_ANALYTICS_WH SET WAREHOUSE_SIZE = 'MEDIUM';

SELECT 
    '⏱️ MEDIUM WAREHOUSE' AS test_run,
    CURRENT_TIMESTAMP() AS started_at;

-- Run the EXACT same complex query
WITH customer_metrics AS (
    SELECT 
        c.customer_id,
        c.company_name,
        c.segment,
        c.industry,
        c.health_score,
        c.created_date,
        COUNT(s.subscription_id) AS product_count,
        SUM(s.mrr) AS total_mrr,
        SUM(CASE WHEN s.status = 'Active' THEN s.mrr ELSE 0 END) AS active_mrr,
        SUM(CASE WHEN s.status = 'Cancelled' THEN s.mrr ELSE 0 END) AS churned_mrr,
        MIN(s.start_date) AS first_subscription,
        MAX(s.start_date) AS latest_subscription
    FROM CUSTOMERS c
    LEFT JOIN SUBSCRIPTIONS s ON c.customer_id = s.customer_id
    GROUP BY c.customer_id, c.company_name, c.segment, c.industry, c.health_score, c.created_date
),
ranked_customers AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY total_mrr DESC) AS segment_rank,
        PERCENT_RANK() OVER (PARTITION BY segment ORDER BY total_mrr) AS mrr_percentile,
        AVG(total_mrr) OVER (PARTITION BY segment) AS segment_avg_mrr,
        SUM(total_mrr) OVER (PARTITION BY segment) AS segment_total_mrr,
        COUNT(*) OVER (PARTITION BY segment) AS segment_customer_count,
        LAG(total_mrr) OVER (PARTITION BY segment ORDER BY total_mrr DESC) AS prev_customer_mrr,
        LEAD(total_mrr) OVER (PARTITION BY segment ORDER BY total_mrr DESC) AS next_customer_mrr
    FROM customer_metrics
),
segment_summary AS (
    SELECT 
        segment,
        industry,
        COUNT(DISTINCT customer_id) AS customers,
        SUM(total_mrr) AS mrr,
        AVG(health_score) AS avg_health,
        AVG(product_count) AS avg_products,
        SUM(churned_mrr) / NULLIF(SUM(total_mrr), 0) * 100 AS churn_rate
    FROM ranked_customers
    GROUP BY segment, industry
),
cross_segment AS (
    SELECT 
        a.segment AS segment_a,
        b.segment AS segment_b,
        a.mrr AS mrr_a,
        b.mrr AS mrr_b,
        a.mrr - b.mrr AS mrr_diff
    FROM segment_summary a
    CROSS JOIN segment_summary b
    WHERE a.segment != b.segment
)
SELECT 
    r.segment,
    r.industry,
    COUNT(*) AS customer_count,
    SUM(r.total_mrr) AS total_mrr,
    ROUND(AVG(r.health_score), 2) AS avg_health,
    ROUND(AVG(r.mrr_percentile), 4) AS avg_percentile,
    SUM(CASE WHEN r.segment_rank <= 10 THEN 1 ELSE 0 END) AS top_10_customers,
    ROUND(AVG(r.segment_avg_mrr), 2) AS segment_benchmark_mrr
FROM ranked_customers r
JOIN segment_summary s ON r.segment = s.segment AND r.industry = s.industry
GROUP BY r.segment, r.industry
HAVING COUNT(*) > 5
ORDER BY total_mrr DESC;

-- ═══════════════════════════════════════════════════════════════════════════
-- COMPARE RESULTS: Side-by-side timing comparison
-- ═══════════════════════════════════════════════════════════════════════════

SELECT 
    warehouse_size,
    ROUND(total_elapsed_time / 1000, 2) AS execution_seconds,
    rows_produced,
    bytes_scanned / 1024 / 1024 AS mb_scanned,
    start_time
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE query_text ILIKE '%customer_metrics%'
  AND query_text NOT ILIKE '%INFORMATION_SCHEMA%'
ORDER BY start_time DESC
LIMIT 2;

-- Scale back down when done (cost savings!)
ALTER WAREHOUSE FMG_ANALYTICS_WH SET WAREHOUSE_SIZE = 'XSMALL';

/*
    🎯 KEY INSIGHTS:
    
    XSMALL vs MEDIUM Performance:
    ┌──────────────┬─────────────┬──────────────┬─────────────┐
    │ Warehouse    │ vCPUs       │ Relative     │ Cost/Hour   │
    │              │             │ Speed        │ (1 credit)  │
    ├──────────────┼─────────────┼──────────────┼─────────────┤
    │ XSMALL       │ 1           │ 1x           │ 1 credit    │
    │ SMALL        │ 2           │ ~2x          │ 2 credits   │
    │ MEDIUM       │ 4           │ ~4x          │ 4 credits   │
    │ LARGE        │ 8           │ ~8x          │ 8 credits   │
    └──────────────┴─────────────┴──────────────┴─────────────┘
    
    ✅ Scaling is LINEAR - 2x compute = 2x speed = 2x cost
    ✅ Resize happens in SECONDS - no downtime
    ✅ Use small for dev, scale up for production loads
    ✅ Auto-suspend saves money when idle
*/

-- ============================================================================
-- STEP 3: EXPLORE THE DATA
-- ============================================================================
USE ROLE FMG_ADMIN;
USE WAREHOUSE FMG_ANALYTICS_WH;
USE SCHEMA FMG_LAB1.PRODUCTION;

-- See FMG customer data
SELECT * FROM CUSTOMERS ORDER BY mrr DESC;

-- Revenue by segment
SELECT segment, COUNT(*) AS customers, SUM(mrr) AS total_mrr
FROM CUSTOMERS
GROUP BY segment
ORDER BY total_mrr DESC;

-- Customer product breakdown
SELECT 
    c.company_name,
    c.segment,
    COUNT(s.subscription_id) AS products,
    SUM(s.mrr) AS total_mrr
FROM CUSTOMERS c
JOIN SUBSCRIPTIONS s ON c.customer_id = s.customer_id
WHERE s.status = 'Active'
GROUP BY c.company_name, c.segment
ORDER BY total_mrr DESC;

-- ============================================================================
-- STEP 4: TEST ROLE PERMISSIONS
-- ============================================================================

-- Test as ANALYST (read-only)
USE ROLE FMG_ANALYST;
USE SECONDARY ROLES NONE;
USE WAREHOUSE FMG_ANALYTICS_WH;

-- ✅ This works (SELECT)
SELECT segment, COUNT(*) AS customers, SUM(mrr) AS total_mrr
FROM FMG_LAB1.PRODUCTION.CUSTOMERS
GROUP BY segment;

-- ❌ This should FAIL (uncomment to test)
-- INSERT INTO FMG_LAB1.PRODUCTION.CUSTOMERS VALUES ('TEST', 'Test', 'SMB', 'RIA', 100, 50, CURRENT_DATE());

-- Test as ENGINEER (read + write)
USE ROLE FMG_ENGINEER;
USE SECONDARY ROLES NONE;
USE WAREHOUSE FMG_LOADING_WH;

-- ✅ This works (INSERT)
INSERT INTO FMG_LAB1.PRODUCTION.CUSTOMERS VALUES 
    ('C009', 'New Advisory Firm', 'SMB', 'RIA', 299.00, 90, CURRENT_DATE());

-- Verify the insert
SELECT * FROM FMG_LAB1.PRODUCTION.CUSTOMERS WHERE customer_id = 'C009';

-- ============================================================================
-- CLEANUP (Optional - remove lab resources)
-- ============================================================================
-- USE ROLE ACCOUNTADMIN;
-- DROP DATABASE FMG_LAB1;

-- ============================================================================
-- 🎉 LAB 1 COMPLETE!
-- ============================================================================
/*
  What you just saw:
  
  ✅ Consumed shared data instantly (no data movement!)
  ✅ Two independent warehouses (separation of compute)
  ✅ Performance scaling: XSMALL vs MEDIUM side-by-side comparison
  ✅ Resized a warehouse instantly with zero downtime
  ✅ RBAC in action - analyst can read, engineer can write
  
  Key Snowflake Benefits:
  • Data Sharing - instant access, no copies, always current
  • Linear scaling - 2x compute = 2x speed (and 2x cost)
  • Instant resize - scale up for heavy workloads, scale down to save money
  • Role-based security is intuitive and fast
  • Compute is separate from storage - scale independently
  • No resource contention between workloads
  • Pay only for what you use (auto-suspend)
  
  Ready for more? Try any other lab - they're all independent!
*/
