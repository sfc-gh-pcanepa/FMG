/*=============================================================================
  FMG SUITE - PROSPECT ACCOUNT SETUP
  
  Run this in the PROSPECT'S trial account to:
  1. Get your account locator (give this to the Snowflake team)
  2. Create the database from the shared data
  3. Set up roles and warehouses for the labs
  
  Time: ~5 minutes
=============================================================================*/

USE ROLE ACCOUNTADMIN;

-- ============================================================================
-- STEP 1: GET YOUR ACCOUNT LOCATOR
-- ============================================================================
-- Give this value to the Snowflake team so they can share data with you
SELECT CURRENT_ORGANIZATION_NAME() || '.' || CURRENT_ACCOUNT_NAME() AS YOUR_ACCOUNT_LOCATOR;

-- ============================================================================
-- STEP 2: VIEW AVAILABLE SHARES (run after Snowflake team adds your account)
-- ============================================================================
SHOW SHARES;

-- ============================================================================
-- STEP 3: CREATE DATABASE FROM SHARE
-- ============================================================================
-- Replace <provider_account> with the account that shared the data
-- Example: CREATE DATABASE FMG_DATA FROM SHARE XXXXX.YYYYY.FMG_LABS_SHARE;

-- CREATE DATABASE FMG_DATA FROM SHARE <provider_account>.FMG_LABS_SHARE;

-- ============================================================================
-- STEP 4: CREATE ROLES
-- ============================================================================
CREATE ROLE IF NOT EXISTS FMG_ADMIN COMMENT = 'Full access to all FMG data';
CREATE ROLE IF NOT EXISTS FMG_ANALYST COMMENT = 'Read-only access for reporting';
CREATE ROLE IF NOT EXISTS FMG_ENGINEER COMMENT = 'Read/write for data pipelines';

-- Set up hierarchy
GRANT ROLE FMG_ANALYST TO ROLE FMG_ADMIN;
GRANT ROLE FMG_ENGINEER TO ROLE FMG_ADMIN;
GRANT ROLE FMG_ADMIN TO ROLE SYSADMIN;

-- Grant roles to yourself
GRANT ROLE FMG_ADMIN TO USER IDENTIFIER(CURRENT_USER());

-- ============================================================================
-- STEP 5: CREATE WAREHOUSES
-- ============================================================================
CREATE WAREHOUSE IF NOT EXISTS FMG_ANALYTICS_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    COMMENT = 'For BI and reporting';

CREATE WAREHOUSE IF NOT EXISTS FMG_LOADING_WH
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 120
    AUTO_RESUME = TRUE
    COMMENT = 'For data loading/ETL';

-- ============================================================================
-- STEP 6: CREATE LOCAL SCHEMA FOR LAB WORK
-- ============================================================================
-- The shared data is read-only, so we create a local schema for labs
CREATE DATABASE IF NOT EXISTS FMG_LABS;
CREATE SCHEMA IF NOT EXISTS FMG_LABS.PRODUCTION;
CREATE SCHEMA IF NOT EXISTS FMG_LABS.GOVERNANCE;

-- ============================================================================
-- STEP 7: COPY SHARED DATA TO LOCAL TABLES (for write operations in labs)
-- ============================================================================
USE WAREHOUSE FMG_ANALYTICS_WH;

-- Copy tables from shared database to local database
-- Run these AFTER Step 3 creates FMG_DATA from the share

CREATE OR REPLACE TABLE FMG_LABS.PRODUCTION.CUSTOMERS AS 
    SELECT * FROM FMG_DATA.FMG.CUSTOMERS;

CREATE OR REPLACE TABLE FMG_LABS.PRODUCTION.USERS AS 
    SELECT * FROM FMG_DATA.FMG.USERS;

CREATE OR REPLACE TABLE FMG_LABS.PRODUCTION.SUBSCRIPTIONS AS 
    SELECT * FROM FMG_DATA.FMG.SUBSCRIPTIONS;

CREATE OR REPLACE TABLE FMG_LABS.PRODUCTION.CUSTOMER_FEEDBACK AS 
    SELECT * FROM FMG_DATA.FMG.CUSTOMER_FEEDBACK;

CREATE OR REPLACE TABLE FMG_LABS.PRODUCTION.KNOWLEDGE_BASE AS 
    SELECT * FROM FMG_DATA.FMG.KNOWLEDGE_BASE;

-- ============================================================================
-- STEP 8: GRANT PERMISSIONS
-- ============================================================================

-- ADMIN: Full access
GRANT ALL ON DATABASE FMG_LABS TO ROLE FMG_ADMIN;
GRANT ALL ON SCHEMA FMG_LABS.PRODUCTION TO ROLE FMG_ADMIN;
GRANT ALL ON SCHEMA FMG_LABS.GOVERNANCE TO ROLE FMG_ADMIN;
GRANT USAGE ON WAREHOUSE FMG_ANALYTICS_WH TO ROLE FMG_ADMIN;
GRANT USAGE ON WAREHOUSE FMG_LOADING_WH TO ROLE FMG_ADMIN;

-- ANALYST: Read-only
GRANT USAGE ON DATABASE FMG_LABS TO ROLE FMG_ANALYST;
GRANT USAGE ON SCHEMA FMG_LABS.PRODUCTION TO ROLE FMG_ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA FMG_LABS.PRODUCTION TO ROLE FMG_ANALYST;
GRANT SELECT ON FUTURE TABLES IN SCHEMA FMG_LABS.PRODUCTION TO ROLE FMG_ANALYST;
GRANT USAGE ON WAREHOUSE FMG_ANALYTICS_WH TO ROLE FMG_ANALYST;

-- ENGINEER: Read + Write
GRANT USAGE ON DATABASE FMG_LABS TO ROLE FMG_ENGINEER;
GRANT USAGE ON SCHEMA FMG_LABS.PRODUCTION TO ROLE FMG_ENGINEER;
GRANT ALL ON ALL TABLES IN SCHEMA FMG_LABS.PRODUCTION TO ROLE FMG_ENGINEER;
GRANT ALL ON FUTURE TABLES IN SCHEMA FMG_LABS.PRODUCTION TO ROLE FMG_ENGINEER;
GRANT USAGE ON WAREHOUSE FMG_LOADING_WH TO ROLE FMG_ENGINEER;

-- Transfer ownership so roles work properly
GRANT OWNERSHIP ON ALL TABLES IN SCHEMA FMG_LABS.PRODUCTION TO ROLE FMG_ADMIN COPY CURRENT GRANTS;

-- Revoke write from analyst (ensure read-only)
REVOKE INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA FMG_LABS.PRODUCTION FROM ROLE FMG_ANALYST;

-- ============================================================================
-- STEP 9: VERIFY SETUP
-- ============================================================================
SHOW DATABASES LIKE 'FMG%';
SHOW WAREHOUSES LIKE 'FMG%';
SHOW ROLES LIKE 'FMG%';

-- Test data access
USE ROLE FMG_ADMIN;
USE WAREHOUSE FMG_ANALYTICS_WH;

SELECT 'CUSTOMERS' AS table_name, COUNT(*) AS rows FROM FMG_LABS.PRODUCTION.CUSTOMERS
UNION ALL SELECT 'USERS', COUNT(*) FROM FMG_LABS.PRODUCTION.USERS
UNION ALL SELECT 'SUBSCRIPTIONS', COUNT(*) FROM FMG_LABS.PRODUCTION.SUBSCRIPTIONS
UNION ALL SELECT 'CUSTOMER_FEEDBACK', COUNT(*) FROM FMG_LABS.PRODUCTION.CUSTOMER_FEEDBACK
UNION ALL SELECT 'KNOWLEDGE_BASE', COUNT(*) FROM FMG_LABS.PRODUCTION.KNOWLEDGE_BASE;

-- ============================================================================
-- ✅ SETUP COMPLETE!
-- ============================================================================
SELECT '✅ Prospect Setup Complete! Ready for Labs.' AS STATUS;

/*
  You now have:
  - FMG_DATA database (read-only, from share)
  - FMG_LABS database (read/write, for lab exercises)
  - 3 roles: FMG_ADMIN, FMG_ANALYST, FMG_ENGINEER
  - 2 warehouses: FMG_ANALYTICS_WH, FMG_LOADING_WH
  
  Start with Lab 1: lab1-getting-started/lab1_complete.sql
  
  NOTE: In the lab scripts, replace "FMG_DATA" with "FMG_LABS" 
        since you'll be working in your local copy.
*/

