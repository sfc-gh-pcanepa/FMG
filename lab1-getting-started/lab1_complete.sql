/*=============================================================================
  FMG SUITE - LAB 1: GETTING STARTED WITH SNOWFLAKE
  
  What you'll see:
  ✅ Create roles with different privileges in seconds
  ✅ Separation of compute (multiple warehouses)
  ✅ Instant warehouse resizing
  ✅ Role-based access control in action
  
  Time: ~20 minutes
  Prerequisites: Run setup/prospect_setup.sql first
=============================================================================*/

-- ============================================================================
-- STEP 1: VERIFY SETUP
-- ============================================================================
USE ROLE ACCOUNTADMIN;

-- Confirm roles exist (created in prospect_setup.sql)
SHOW ROLES LIKE 'FMG%';

-- Confirm warehouses exist
SHOW WAREHOUSES LIKE 'FMG%';

-- Confirm data is loaded
SELECT COUNT(*) AS customer_count FROM FMG_LABS.PRODUCTION.CUSTOMERS;

-- ============================================================================
-- STEP 2: EXPLORE SEPARATION OF COMPUTE
-- ============================================================================

-- You have two INDEPENDENT warehouses:
-- FMG_ANALYTICS_WH - For BI and reporting
-- FMG_LOADING_WH - For data loading/ETL

-- Key insight: These run independently - no resource contention!
SHOW WAREHOUSES LIKE 'FMG%';

-- ============================================================================
-- STEP 3: INSTANT WAREHOUSE RESIZING (No downtime!)
-- ============================================================================

-- Need more power? Resize in seconds with ZERO downtime
ALTER WAREHOUSE FMG_ANALYTICS_WH SET WAREHOUSE_SIZE = 'MEDIUM';

-- Check the change
SHOW WAREHOUSES LIKE 'FMG_ANALYTICS%';

-- Scale back down when done
ALTER WAREHOUSE FMG_ANALYTICS_WH SET WAREHOUSE_SIZE = 'XSMALL';

-- 🎯 Key insight: Resize takes seconds, queries keep running!

-- ============================================================================
-- STEP 4: EXPLORE THE DATA
-- ============================================================================
USE ROLE FMG_ADMIN;
USE WAREHOUSE FMG_ANALYTICS_WH;
USE SCHEMA FMG_LABS.PRODUCTION;

-- See FMG customer data
SELECT * FROM CUSTOMERS ORDER BY mrr DESC;

-- Revenue by segment
SELECT segment, COUNT(*) AS customers, SUM(mrr) AS total_mrr
FROM CUSTOMERS
GROUP BY segment
ORDER BY total_mrr DESC;

-- ============================================================================
-- STEP 5: TEST ROLE PERMISSIONS
-- ============================================================================

-- Test as ANALYST (read-only)
USE ROLE FMG_ANALYST;
USE SECONDARY ROLES NONE;  -- Important: isolate to just this role
USE WAREHOUSE FMG_ANALYTICS_WH;

-- ✅ This works (SELECT)
SELECT segment, COUNT(*) AS customers, SUM(mrr) AS total_mrr
FROM FMG_LABS.PRODUCTION.CUSTOMERS
GROUP BY segment;

-- ❌ This should FAIL (uncomment to test)
-- INSERT INTO FMG_LABS.PRODUCTION.CUSTOMERS VALUES ('TEST', 'Test', 'SMB', 'RIA', 100, 50, CURRENT_DATE());

-- Test as ENGINEER (read + write)
USE ROLE FMG_ENGINEER;
USE SECONDARY ROLES NONE;
USE WAREHOUSE FMG_LOADING_WH;

-- ✅ This works (INSERT)
INSERT INTO FMG_LABS.PRODUCTION.CUSTOMERS VALUES 
    ('C009', 'New Advisory Firm', 'SMB', 'RIA', 299.00, 90, CURRENT_DATE());

-- Verify the insert
SELECT * FROM FMG_LABS.PRODUCTION.CUSTOMERS WHERE customer_id = 'C009';

-- ============================================================================
-- 🎉 LAB 1 COMPLETE!
-- ============================================================================
/*
  What you just saw:
  
  ✅ Two independent warehouses (separation of compute)
  ✅ Resized a warehouse instantly with zero downtime
  ✅ RBAC in action - analyst can read, engineer can write
  ✅ Simple GRANT statements control everything
  
  Key Snowflake Benefits:
  • Role-based security is intuitive and fast
  • Compute is separate from storage - scale independently
  • No resource contention between workloads
  • Pay only for what you use (auto-suspend)
  
  Next: Lab 2 - Governance & FinOps
*/

