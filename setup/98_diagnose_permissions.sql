/*=============================================================================
  FMG SUITE - Permission Diagnostic Script
  
  Run this to diagnose why FMG_ANALYST might have write access
=============================================================================*/

USE ROLE ACCOUNTADMIN;

-- ============================================================================
-- DIAGNOSTIC 1: Check if Secondary Roles are enabled
-- ============================================================================
-- If DEFAULT_SECONDARY_ROLES = 'ALL', the user gets privileges from ALL their roles
-- combined, not just the current role!

-- First, see who you are
SELECT CURRENT_USER() AS YOUR_USERNAME, CURRENT_ROLE() AS YOUR_CURRENT_ROLE;

-- Check secondary roles setting (run this separately with your username)
SHOW PARAMETERS LIKE 'DEFAULT_SECONDARY_ROLES' IN ACCOUNT;

-- Check current session secondary roles
SELECT CURRENT_SECONDARY_ROLES() AS ACTIVE_SECONDARY_ROLES;

-- ============================================================================
-- DIAGNOSTIC 2: Check what roles YOUR USER has
-- ============================================================================
-- Replace <YOUR_USERNAME> with your actual username from the query above
-- SHOW GRANTS TO USER <YOUR_USERNAME>;

-- ============================================================================
-- DIAGNOSTIC 3: Check what roles FMG_ANALYST has/inherits
-- ============================================================================
-- Roles granted TO FMG_ANALYST (what it inherits FROM)
SHOW GRANTS TO ROLE FMG_ANALYST;

-- Roles that FMG_ANALYST is granted TO (who inherits from it)
SHOW GRANTS OF ROLE FMG_ANALYST;

-- ============================================================================
-- DIAGNOSTIC 4: Check table ownership
-- ============================================================================
-- If your user OWNS the table, you have full access regardless of role
SELECT 
    TABLE_CATALOG,
    TABLE_SCHEMA,
    TABLE_NAME,
    TABLE_OWNER
FROM FMG_PRODUCTION.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'RAW';

-- ============================================================================
-- DIAGNOSTIC 5: Check specific grants on CUSTOMERS table
-- ============================================================================
SHOW GRANTS ON TABLE FMG_PRODUCTION.RAW.CUSTOMERS;

-- ============================================================================
-- DIAGNOSTIC 6: Check future grants that might be causing issues
-- ============================================================================
SHOW FUTURE GRANTS IN SCHEMA FMG_PRODUCTION.RAW;

-- ============================================================================
-- FIX 1: Disable Secondary Roles for your user (IMPORTANT!)
-- ============================================================================
-- This ensures when you USE ROLE FMG_ANALYST, you ONLY get FMG_ANALYST privileges
-- Replace <YOUR_USERNAME> with your actual username
-- ALTER USER <YOUR_USERNAME> SET DEFAULT_SECONDARY_ROLES = ('NONE');

-- Or run this which works for current user:
ALTER SESSION SET DEFAULT_SECONDARY_ROLES = NONE;

-- ============================================================================
-- FIX 2: Transfer table ownership to SYSADMIN
-- ============================================================================
-- This removes your personal ownership of the tables
GRANT OWNERSHIP ON ALL TABLES IN SCHEMA FMG_PRODUCTION.RAW 
    TO ROLE SYSADMIN COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL TABLES IN SCHEMA FMG_PRODUCTION.STAGING 
    TO ROLE SYSADMIN COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL TABLES IN SCHEMA FMG_PRODUCTION.CURATED 
    TO ROLE SYSADMIN COPY CURRENT GRANTS;

-- ============================================================================
-- FIX 3: Explicitly revoke any write privileges from FMG_ANALYST
-- ============================================================================
REVOKE INSERT, UPDATE, DELETE ON ALL TABLES IN DATABASE FMG_PRODUCTION FROM ROLE FMG_ANALYST;
REVOKE INSERT, UPDATE, DELETE ON ALL TABLES IN DATABASE FMG_ANALYTICS FROM ROLE FMG_ANALYST;

-- Also revoke from future tables
REVOKE INSERT, UPDATE, DELETE ON FUTURE TABLES IN DATABASE FMG_PRODUCTION FROM ROLE FMG_ANALYST;
REVOKE INSERT, UPDATE, DELETE ON FUTURE TABLES IN DATABASE FMG_ANALYTICS FROM ROLE FMG_ANALYST;

-- ============================================================================
-- TEST: Now try as FMG_ANALYST
-- ============================================================================
USE ROLE FMG_ANALYST;
USE WAREHOUSE FMG_DEV_XS;

-- Disable secondary roles for this session
USE SECONDARY ROLES NONE;

-- Verify only FMG_ANALYST is active
SELECT CURRENT_ROLE(), CURRENT_SECONDARY_ROLES();

-- This should WORK (SELECT)
SELECT COUNT(*) FROM FMG_PRODUCTION.RAW.CUSTOMERS;

-- This should FAIL (INSERT)
-- Uncomment to test:
-- INSERT INTO FMG_PRODUCTION.RAW.CUSTOMERS (customer_id) VALUES ('TEST-FAIL');

SELECT '✅ Diagnostic Complete - Check results above' AS STATUS;

