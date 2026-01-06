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
SELECT 'CUSTOMERS' AS table_name, COUNT(*) AS rows FROM CUSTOMERS
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
-- STEP 2: INSTANT WAREHOUSE RESIZING (No downtime!)
-- ============================================================================

-- Need more power? Resize in seconds with ZERO downtime
ALTER WAREHOUSE FMG_ANALYTICS_WH SET WAREHOUSE_SIZE = 'MEDIUM';

-- Check the change
SHOW WAREHOUSES LIKE 'FMG_ANALYTICS%';

-- Scale back down when done
ALTER WAREHOUSE FMG_ANALYTICS_WH SET WAREHOUSE_SIZE = 'XSMALL';

-- 🎯 Key insight: Resize takes seconds, queries keep running!

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
  ✅ Resized a warehouse instantly with zero downtime
  ✅ RBAC in action - analyst can read, engineer can write
  
  Key Snowflake Benefits:
  • Data Sharing - instant access, no copies, always current
  • Role-based security is intuitive and fast
  • Compute is separate from storage - scale independently
  • No resource contention between workloads
  • Pay only for what you use (auto-suspend)
  
  Ready for more? Try any other lab - they're all independent!
*/
