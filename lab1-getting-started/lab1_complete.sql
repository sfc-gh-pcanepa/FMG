/*=============================================================================
  FMG SUITE - LAB 1: GETTING STARTED WITH SNOWFLAKE
  
  What you'll see:
  ✅ Create roles with different privileges in seconds
  ✅ Separation of compute (multiple warehouses)
  ✅ Instant warehouse resizing
  ✅ Role-based access control in action
  
  Time: ~20 minutes
=============================================================================*/

-- ============================================================================
-- STEP 1: CREATE ROLES (30 seconds!)
-- ============================================================================
USE ROLE ACCOUNTADMIN;

-- Create three roles with different access levels
CREATE ROLE IF NOT EXISTS FMG_ADMIN COMMENT = 'Full access to all FMG data';
CREATE ROLE IF NOT EXISTS FMG_ANALYST COMMENT = 'Read-only access for reporting';
CREATE ROLE IF NOT EXISTS FMG_ENGINEER COMMENT = 'Read/write for data pipelines';

-- Set up hierarchy (analyst inherits viewer privileges)
GRANT ROLE FMG_ANALYST TO ROLE FMG_ADMIN;
GRANT ROLE FMG_ENGINEER TO ROLE FMG_ADMIN;
GRANT ROLE FMG_ADMIN TO ROLE SYSADMIN;

-- That's it! Roles created in seconds ✅

-- ============================================================================
-- STEP 2: CREATE WAREHOUSES (Separation of Compute)
-- ============================================================================

-- Create two separate warehouses for different workloads
CREATE WAREHOUSE IF NOT EXISTS FMG_ANALYTICS_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60          -- Suspends after 1 min idle (saves $$$)
    AUTO_RESUME = TRUE         -- Starts automatically when needed
    COMMENT = 'For BI and reporting';

CREATE WAREHOUSE IF NOT EXISTS FMG_LOADING_WH
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 120
    AUTO_RESUME = TRUE
    COMMENT = 'For data loading/ETL';

-- Key insight: These are INDEPENDENT - one can be running while the other is off
-- No resource contention between BI users and data pipelines!

-- ============================================================================
-- STEP 3: INSTANT WAREHOUSE RESIZING (No downtime!)
-- ============================================================================

-- Need more power? Resize in seconds with ZERO downtime
ALTER WAREHOUSE FMG_ANALYTICS_WH SET WAREHOUSE_SIZE = 'MEDIUM';

-- Check the change
SHOW WAREHOUSES LIKE 'FMG%';

-- Scale back down when done
ALTER WAREHOUSE FMG_ANALYTICS_WH SET WAREHOUSE_SIZE = 'XSMALL';

-- ============================================================================
-- STEP 4: GRANT PRIVILEGES (Easy & Intuitive)
-- ============================================================================

-- Create database and schema
CREATE DATABASE IF NOT EXISTS FMG_DATA;
CREATE SCHEMA IF NOT EXISTS FMG_DATA.PRODUCTION;

-- ADMIN: Full access
GRANT ALL ON DATABASE FMG_DATA TO ROLE FMG_ADMIN;
GRANT ALL ON SCHEMA FMG_DATA.PRODUCTION TO ROLE FMG_ADMIN;
GRANT USAGE ON WAREHOUSE FMG_ANALYTICS_WH TO ROLE FMG_ADMIN;
GRANT USAGE ON WAREHOUSE FMG_LOADING_WH TO ROLE FMG_ADMIN;

-- ANALYST: Read-only (SELECT only, no INSERT/UPDATE/DELETE)
GRANT USAGE ON DATABASE FMG_DATA TO ROLE FMG_ANALYST;
GRANT USAGE ON SCHEMA FMG_DATA.PRODUCTION TO ROLE FMG_ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA FMG_DATA.PRODUCTION TO ROLE FMG_ANALYST;
GRANT SELECT ON FUTURE TABLES IN SCHEMA FMG_DATA.PRODUCTION TO ROLE FMG_ANALYST;
GRANT USAGE ON WAREHOUSE FMG_ANALYTICS_WH TO ROLE FMG_ANALYST;

-- ENGINEER: Read + Write
GRANT USAGE ON DATABASE FMG_DATA TO ROLE FMG_ENGINEER;
GRANT USAGE ON SCHEMA FMG_DATA.PRODUCTION TO ROLE FMG_ENGINEER;
GRANT ALL ON ALL TABLES IN SCHEMA FMG_DATA.PRODUCTION TO ROLE FMG_ENGINEER;
GRANT ALL ON FUTURE TABLES IN SCHEMA FMG_DATA.PRODUCTION TO ROLE FMG_ENGINEER;
GRANT USAGE ON WAREHOUSE FMG_LOADING_WH TO ROLE FMG_ENGINEER;

-- ============================================================================
-- STEP 5: CREATE SAMPLE DATA
-- ============================================================================
USE ROLE FMG_ADMIN;
USE WAREHOUSE FMG_ANALYTICS_WH;
USE SCHEMA FMG_DATA.PRODUCTION;

-- Create customers table
CREATE OR REPLACE TABLE CUSTOMERS (
    customer_id VARCHAR(20),
    company_name VARCHAR(200),
    segment VARCHAR(50),
    mrr DECIMAL(10,2),
    health_score INT,
    created_date DATE
);

-- Insert sample FMG customers
INSERT INTO CUSTOMERS VALUES
    ('C001', 'Acme Financial Advisors', 'Enterprise', 2500.00, 85, '2022-01-15'),
    ('C002', 'Summit Wealth Management', 'Mid-Market', 899.00, 72, '2022-03-20'),
    ('C003', 'Peak Advisory Group', 'SMB', 299.00, 91, '2023-06-01'),
    ('C004', 'Horizon Financial', 'Enterprise', 3200.00, 68, '2021-11-10'),
    ('C005', 'Cascade Investments', 'Mid-Market', 599.00, 88, '2023-01-25');

-- ============================================================================
-- STEP 6: TEST ROLE PERMISSIONS
-- ============================================================================

-- Test as ANALYST (read-only)
USE ROLE FMG_ANALYST;
USE SECONDARY ROLES NONE;  -- Important: isolate to just this role
USE WAREHOUSE FMG_ANALYTICS_WH;

-- ✅ This works (SELECT)
SELECT segment, COUNT(*) as customers, SUM(mrr) as total_mrr
FROM FMG_DATA.PRODUCTION.CUSTOMERS
GROUP BY segment;

-- ❌ This would FAIL (uncomment to test)
-- INSERT INTO FMG_DATA.PRODUCTION.CUSTOMERS VALUES ('TEST', 'Test', 'SMB', 100, 50, CURRENT_DATE());

-- Test as ENGINEER (read + write)
USE ROLE FMG_ENGINEER;
USE SECONDARY ROLES NONE;
USE WAREHOUSE FMG_LOADING_WH;

-- ✅ This works (INSERT)
INSERT INTO FMG_DATA.PRODUCTION.CUSTOMERS VALUES 
    ('C006', 'Alpine Advisors', 'SMB', 199.00, 95, CURRENT_DATE());

-- Verify
SELECT * FROM FMG_DATA.PRODUCTION.CUSTOMERS;

-- ============================================================================
-- 🎉 LAB 1 COMPLETE!
-- ============================================================================
/*
  What you just saw:
  
  ✅ Created 3 roles with different privileges in seconds
  ✅ Created 2 independent warehouses (separation of compute)
  ✅ Resized a warehouse instantly with zero downtime
  ✅ Set up RBAC with simple GRANT statements
  ✅ Tested that permissions work correctly
  
  Key Snowflake Benefits:
  • Role-based security is intuitive and fast
  • Compute is separate from storage - scale independently
  • No resource contention between workloads
  • Pay only for what you use (auto-suspend)
*/

