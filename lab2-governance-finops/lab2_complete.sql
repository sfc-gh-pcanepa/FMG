/*=============================================================================
  FMG SUITE - LAB 2: GOVERNANCE & FINOPS
  
  What you'll see:
  ✅ Tag data for classification in seconds
  ✅ Dynamic data masking (same query, different results by role)
  ✅ Resource monitors to control costs
  ✅ Query audit history
  
  Time: ~20 minutes
  Prerequisites: Data share consumed (FMG_SHARED_DATA database exists)
  
  ⚠️  This lab is INDEPENDENT - run it in any order!
=============================================================================*/

-- ============================================================================
-- SETUP: CREATE LAB ENVIRONMENT FROM SHARED DATA
-- ============================================================================
USE ROLE ACCOUNTADMIN;

-- Create lab-specific database
CREATE DATABASE IF NOT EXISTS FMG_LAB2;
CREATE SCHEMA IF NOT EXISTS FMG_LAB2.PRODUCTION;
CREATE SCHEMA IF NOT EXISTS FMG_LAB2.GOVERNANCE;

-- Create warehouse
CREATE WAREHOUSE IF NOT EXISTS FMG_ANALYTICS_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

-- Create roles
CREATE ROLE IF NOT EXISTS FMG_ADMIN;
CREATE ROLE IF NOT EXISTS FMG_ANALYST;
GRANT ROLE FMG_ADMIN TO ROLE ACCOUNTADMIN;
GRANT USAGE ON WAREHOUSE FMG_ANALYTICS_WH TO ROLE FMG_ADMIN;
GRANT USAGE ON WAREHOUSE FMG_ANALYTICS_WH TO ROLE FMG_ANALYST;
GRANT ALL ON DATABASE FMG_LAB2 TO ROLE FMG_ADMIN;
GRANT USAGE ON DATABASE FMG_LAB2 TO ROLE FMG_ANALYST;
GRANT USAGE ON SCHEMA FMG_LAB2.PRODUCTION TO ROLE FMG_ANALYST;

USE WAREHOUSE FMG_ANALYTICS_WH;
USE SCHEMA FMG_LAB2.PRODUCTION;

-- Copy data from share
CREATE OR REPLACE TABLE CUSTOMERS AS SELECT * FROM FMG_SHARED_DATA.FMG.CUSTOMERS;
CREATE OR REPLACE TABLE USERS AS SELECT * FROM FMG_SHARED_DATA.FMG.USERS;
CREATE OR REPLACE TABLE SUBSCRIPTIONS AS SELECT * FROM FMG_SHARED_DATA.FMG.SUBSCRIPTIONS;

-- Grant table access
GRANT SELECT ON ALL TABLES IN SCHEMA FMG_LAB2.PRODUCTION TO ROLE FMG_ANALYST;
GRANT ALL ON ALL TABLES IN SCHEMA FMG_LAB2.PRODUCTION TO ROLE FMG_ADMIN;

-- Verify data
SELECT 'CUSTOMERS' AS table_name, COUNT(*) AS rows FROM CUSTOMERS
UNION ALL SELECT 'USERS', COUNT(*) FROM USERS
UNION ALL SELECT 'SUBSCRIPTIONS', COUNT(*) FROM SUBSCRIPTIONS;

-- ============================================================================
-- STEP 1: CREATE GOVERNANCE TAGS
-- ============================================================================

-- Create a sensitivity tag
CREATE OR REPLACE TAG FMG_LAB2.GOVERNANCE.SENSITIVITY
    ALLOWED_VALUES = 'PUBLIC', 'INTERNAL', 'CONFIDENTIAL', 'PII'
    COMMENT = 'Data sensitivity classification';

-- Apply tags to PII columns (instant!)
ALTER TABLE FMG_LAB2.PRODUCTION.USERS MODIFY COLUMN email 
    SET TAG FMG_LAB2.GOVERNANCE.SENSITIVITY = 'PII';
ALTER TABLE FMG_LAB2.PRODUCTION.USERS MODIFY COLUMN phone 
    SET TAG FMG_LAB2.GOVERNANCE.SENSITIVITY = 'PII';
ALTER TABLE FMG_LAB2.PRODUCTION.USERS MODIFY COLUMN full_name 
    SET TAG FMG_LAB2.GOVERNANCE.SENSITIVITY = 'PII';

-- Verify tags
SELECT * FROM TABLE(FMG_LAB2.INFORMATION_SCHEMA.TAG_REFERENCES(
    'FMG_LAB2.PRODUCTION.USERS', 'TABLE'));

-- ============================================================================
-- STEP 2: DYNAMIC DATA MASKING (The Magic!)
-- ============================================================================

-- Create masking policies: Admins see real data, others see masked
CREATE OR REPLACE MASKING POLICY FMG_LAB2.GOVERNANCE.EMAIL_MASK AS (val STRING)
RETURNS STRING ->
    CASE 
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'FMG_ADMIN') THEN val
        ELSE REGEXP_REPLACE(val, '.+@', '****@')
    END;

CREATE OR REPLACE MASKING POLICY FMG_LAB2.GOVERNANCE.PHONE_MASK AS (val STRING)
RETURNS STRING ->
    CASE 
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'FMG_ADMIN') THEN val
        ELSE '(***) ***-' || RIGHT(val, 4)
    END;

CREATE OR REPLACE MASKING POLICY FMG_LAB2.GOVERNANCE.NAME_MASK AS (val STRING)
RETURNS STRING ->
    CASE 
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'FMG_ADMIN') THEN val
        ELSE LEFT(val, 1) || '. ' || SPLIT_PART(val, ' ', -1)
    END;

-- Apply policies to columns
ALTER TABLE FMG_LAB2.PRODUCTION.USERS MODIFY COLUMN email 
    SET MASKING POLICY FMG_LAB2.GOVERNANCE.EMAIL_MASK;
ALTER TABLE FMG_LAB2.PRODUCTION.USERS MODIFY COLUMN phone 
    SET MASKING POLICY FMG_LAB2.GOVERNANCE.PHONE_MASK;
ALTER TABLE FMG_LAB2.PRODUCTION.USERS MODIFY COLUMN full_name 
    SET MASKING POLICY FMG_LAB2.GOVERNANCE.NAME_MASK;

-- ============================================================================
-- STEP 3: SEE MASKING IN ACTION
-- ============================================================================

-- As ADMIN: See full data
USE ROLE FMG_ADMIN;
USE SECONDARY ROLES NONE;
USE WAREHOUSE FMG_ANALYTICS_WH;
SELECT user_id, full_name, email, phone, role FROM FMG_LAB2.PRODUCTION.USERS LIMIT 5;
-- Result: John Smith, john.smith@acmefinancial.com, (555) 123-4567

-- As ANALYST: See masked data (SAME QUERY, DIFFERENT RESULT!)
USE ROLE FMG_ANALYST;
USE SECONDARY ROLES NONE;
USE WAREHOUSE FMG_ANALYTICS_WH;
SELECT user_id, full_name, email, phone, role FROM FMG_LAB2.PRODUCTION.USERS LIMIT 5;
-- Result: J. Smith, ****@acmefinancial.com, (***) ***-4567

-- 🎯 Key insight: No code changes needed! Security is automatic based on role.

-- ============================================================================
-- STEP 4: RESOURCE MONITOR (Cost Control)
-- ============================================================================
USE ROLE ACCOUNTADMIN;

-- Create a budget guard - get alerted and auto-suspend at limits
CREATE OR REPLACE RESOURCE MONITOR FMG_LAB2_BUDGET
    WITH CREDIT_QUOTA = 100
    TRIGGERS
        ON 75 PERCENT DO NOTIFY
        ON 90 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND;

-- Apply to warehouse
ALTER WAREHOUSE FMG_ANALYTICS_WH SET RESOURCE_MONITOR = FMG_LAB2_BUDGET;

-- Check status
SHOW RESOURCE MONITORS LIKE 'FMG%';

-- ============================================================================
-- STEP 5: AUDIT - WHO DID WHAT?
-- ============================================================================

-- See recent queries (built-in audit trail)
SELECT 
    query_id,
    user_name,
    role_name,
    warehouse_name,
    LEFT(query_text, 50) AS query_preview,
    execution_status,
    total_elapsed_time/1000 AS seconds
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time > DATEADD('hour', -1, CURRENT_TIMESTAMP())
ORDER BY start_time DESC
LIMIT 10;

-- See who accessed what data
SELECT 
    query_start_time,
    user_name,
    direct_objects_accessed
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
WHERE query_start_time > DATEADD('hour', -1, CURRENT_TIMESTAMP())
LIMIT 10;

-- ============================================================================
-- CLEANUP (Optional)
-- ============================================================================
-- USE ROLE ACCOUNTADMIN;
-- DROP DATABASE FMG_LAB2;
-- DROP RESOURCE MONITOR FMG_LAB2_BUDGET;

-- ============================================================================
-- 🎉 LAB 2 COMPLETE!
-- ============================================================================
/*
  What you just saw:
  
  ✅ Tagged sensitive data in seconds
  ✅ Dynamic masking - same query returns different results by role
  ✅ No application code changes needed for security
  ✅ Resource monitors prevent runaway costs
  ✅ Built-in audit trail for compliance
  
  Key Snowflake Benefits:
  • Governance is built-in, not bolted-on
  • Security policies follow the data automatically
  • Cost control with budget alerts and auto-suspend
  • Complete audit trail for compliance (365 days)
  
  Ready for more? Try any other lab - they're all independent!
*/
