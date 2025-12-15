/*=============================================================================
  FMG SUITE - LAB 3: TRANSFORMATIONS IN SNOWFLAKE
  Script 3: Zero-Copy Cloning & Time Travel (Undrop)
  
  Description: Create instant dev/test environments and recover data
  Prerequisites: FMG databases created
  Duration: ~10 minutes
=============================================================================*/

-- ============================================================================
-- SECTION 1: SET CONTEXT
-- ============================================================================

USE ROLE FMG_ADMIN;
USE WAREHOUSE FMG_DEV_XS;

-- ============================================================================
-- SECTION 2: UNDERSTANDING ZERO-COPY CLONING
-- ============================================================================

/*
    Zero-Copy Cloning creates instant copies of:
    - Databases
    - Schemas
    - Tables
    - Stages
    - File Formats
    - Sequences
    - Streams (as of the clone point)
    
    KEY CONCEPTS:
    - Clone shares storage with source (zero additional cost)
    - Modifications to clone are independent
    - Only changed data in clone uses new storage
    - Clone metadata is separate from source
    
    USE CASES:
    - Development environments
    - Testing schema changes
    - Pre-production validation
    - Backup before migrations
    - Creating sandboxes for experiments
*/

-- ============================================================================
-- SECTION 3: CLONE A DATABASE (Full Environment Copy)
-- ============================================================================

-- Create a complete development clone of production
CREATE DATABASE FMG_PRODUCTION_DEV_CLONE
    CLONE FMG_PRODUCTION
    COMMENT = 'Development clone of production for testing - created ' || CURRENT_DATE();

-- This happens INSTANTLY regardless of data size!
-- A 500GB database clones in seconds, not hours

-- Verify the clone
SHOW DATABASES LIKE 'FMG_PRODUCTION%';

-- Compare object counts
SELECT 'PRODUCTION' AS environment, COUNT(*) AS table_count
FROM FMG_PRODUCTION.INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'RAW'
UNION ALL
SELECT 'DEV_CLONE', COUNT(*)
FROM FMG_PRODUCTION_DEV_CLONE.INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'RAW';

-- ============================================================================
-- SECTION 4: CLONE A SCHEMA
-- ============================================================================

-- Clone just the RAW schema for specific testing
CREATE SCHEMA FMG_PRODUCTION.RAW_BACKUP
    CLONE FMG_PRODUCTION.RAW
    COMMENT = 'Backup of RAW schema before migration';

-- Verify
SHOW SCHEMAS IN DATABASE FMG_PRODUCTION;

-- ============================================================================
-- SECTION 5: CLONE A TABLE
-- ============================================================================

-- Clone a single table for testing
CREATE TABLE FMG_PRODUCTION.RAW.CUSTOMERS_BACKUP
    CLONE FMG_PRODUCTION.RAW.CUSTOMERS
    COMMENT = 'Backup before customer data cleanup';

-- Make changes to the clone (original is unaffected)
UPDATE FMG_PRODUCTION.RAW.CUSTOMERS_BACKUP
SET account_status = 'TEST_STATUS'
WHERE customer_id LIKE 'CUST-001%';

-- Verify clone is independent
SELECT 
    'ORIGINAL' AS source,
    COUNT(*) AS total,
    COUNT(CASE WHEN account_status = 'TEST_STATUS' THEN 1 END) AS test_status_count
FROM FMG_PRODUCTION.RAW.CUSTOMERS
UNION ALL
SELECT 
    'CLONE',
    COUNT(*),
    COUNT(CASE WHEN account_status = 'TEST_STATUS' THEN 1 END)
FROM FMG_PRODUCTION.RAW.CUSTOMERS_BACKUP;

-- ============================================================================
-- SECTION 6: CLONE WITH TIME TRAVEL
-- ============================================================================

/*
    You can clone objects as they existed at a point in the past!
    This combines cloning with Time Travel.
    
    Options:
    - AT(TIMESTAMP => '2024-01-15 10:00:00')
    - AT(OFFSET => -3600)  -- 1 hour ago
    - BEFORE(STATEMENT => '<query_id>')
*/

-- Clone table as it existed 1 hour ago (if within Time Travel retention)
-- CREATE TABLE FMG_PRODUCTION.RAW.CUSTOMERS_1HR_AGO
--     CLONE FMG_PRODUCTION.RAW.CUSTOMERS
--     AT(OFFSET => -3600);

-- Clone table from specific timestamp
-- CREATE TABLE FMG_PRODUCTION.RAW.CUSTOMERS_YESTERDAY
--     CLONE FMG_PRODUCTION.RAW.CUSTOMERS
--     AT(TIMESTAMP => DATEADD('day', -1, CURRENT_TIMESTAMP()));

-- ============================================================================
-- SECTION 7: TIME TRAVEL QUERIES
-- ============================================================================

/*
    Time Travel allows querying historical data:
    
    Standard Edition: 1 day retention
    Enterprise Edition: Up to 90 days
    
    Syntax:
    - AT(TIMESTAMP => ...)
    - AT(OFFSET => ...)  -- Seconds
    - BEFORE(STATEMENT => '<query_id>')
*/

-- Query customers table as of 10 minutes ago
SELECT COUNT(*), MAX(_loaded_at) AS last_load
FROM FMG_PRODUCTION.RAW.CUSTOMERS
AT(OFFSET => -600);

-- Compare current vs historical
SELECT 
    'NOW' AS time_point,
    COUNT(*) AS customer_count,
    COUNT(CASE WHEN account_status = 'Active' THEN 1 END) AS active_count
FROM FMG_PRODUCTION.RAW.CUSTOMERS
UNION ALL
SELECT 
    '10 MIN AGO',
    COUNT(*),
    COUNT(CASE WHEN account_status = 'Active' THEN 1 END)
FROM FMG_PRODUCTION.RAW.CUSTOMERS
AT(OFFSET => -600);

-- Find specific timestamp of when data changed
-- SELECT * FROM FMG_PRODUCTION.RAW.CUSTOMERS
-- AT(TIMESTAMP => '2024-12-14 15:30:00'::TIMESTAMP_NTZ)
-- WHERE customer_id = 'CUST-001000';

-- ============================================================================
-- SECTION 8: UNDROP (RECOVER DELETED OBJECTS)
-- ============================================================================

/*
    UNDROP recovers dropped objects within Time Travel window:
    - Tables
    - Schemas
    - Databases
    
    Objects are fully recovered with all data!
*/

-- First, let's drop something (carefully!)
DROP TABLE FMG_PRODUCTION.RAW.CUSTOMERS_BACKUP;

-- Verify it's gone
SHOW TABLES LIKE 'CUSTOMERS_BACKUP' IN SCHEMA FMG_PRODUCTION.RAW;

-- UNDROP to recover!
UNDROP TABLE FMG_PRODUCTION.RAW.CUSTOMERS_BACKUP;

-- Verify it's back
SHOW TABLES LIKE 'CUSTOMERS_BACKUP' IN SCHEMA FMG_PRODUCTION.RAW;

-- Works for schemas too:
-- DROP SCHEMA FMG_PRODUCTION.RAW_BACKUP;
-- UNDROP SCHEMA FMG_PRODUCTION.RAW_BACKUP;

-- And databases:
-- DROP DATABASE FMG_PRODUCTION_DEV_CLONE;
-- UNDROP DATABASE FMG_PRODUCTION_DEV_CLONE;

-- ============================================================================
-- SECTION 9: RESTORE DATA FROM TIME TRAVEL
-- ============================================================================

-- Scenario: Someone accidentally deleted important customers!
-- Step 1: Check what was deleted
SELECT COUNT(*) AS current_count
FROM FMG_PRODUCTION.RAW.CUSTOMERS;

-- Step 2: Delete some data (simulating accident)
DELETE FROM FMG_PRODUCTION.RAW.CUSTOMERS
WHERE customer_id IN (
    SELECT customer_id FROM FMG_PRODUCTION.RAW.CUSTOMERS LIMIT 5
);

-- Step 3: Check the damage
SELECT COUNT(*) AS after_delete_count
FROM FMG_PRODUCTION.RAW.CUSTOMERS;

-- Step 4: Find the data from before the delete
SELECT COUNT(*) AS before_delete_count
FROM FMG_PRODUCTION.RAW.CUSTOMERS
AT(OFFSET => -60);  -- 1 minute ago

-- Step 5: Restore the deleted rows
INSERT INTO FMG_PRODUCTION.RAW.CUSTOMERS
SELECT * FROM FMG_PRODUCTION.RAW.CUSTOMERS AT(OFFSET => -60)
WHERE customer_id NOT IN (SELECT customer_id FROM FMG_PRODUCTION.RAW.CUSTOMERS);

-- Step 6: Verify restoration
SELECT COUNT(*) AS restored_count
FROM FMG_PRODUCTION.RAW.CUSTOMERS;

-- ============================================================================
-- SECTION 10: DATA RETENTION SETTINGS
-- ============================================================================

-- Check current retention settings
SHOW PARAMETERS LIKE 'DATA_RETENTION_TIME_IN_DAYS' IN DATABASE FMG_PRODUCTION;

-- Set Time Travel retention (Enterprise: up to 90 days)
ALTER DATABASE FMG_PRODUCTION SET DATA_RETENTION_TIME_IN_DAYS = 7;

-- Set different retention for specific tables
ALTER TABLE FMG_PRODUCTION.RAW.CUSTOMERS 
    SET DATA_RETENTION_TIME_IN_DAYS = 30;  -- Critical table, keep longer

-- Check storage used by Time Travel
SELECT 
    table_catalog,
    table_schema,
    table_name,
    active_bytes / POWER(1024, 3) AS active_gb,
    time_travel_bytes / POWER(1024, 3) AS time_travel_gb,
    failsafe_bytes / POWER(1024, 3) AS failsafe_gb
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
WHERE table_catalog = 'FMG_PRODUCTION'
AND active_bytes > 0
ORDER BY time_travel_bytes DESC
LIMIT 10;

-- ============================================================================
-- SECTION 11: CLONE BEST PRACTICES
-- ============================================================================

/*
    FMG CLONING BEST PRACTICES:
    
    1. DEV/TEST ENVIRONMENTS
       - Clone production weekly for dev
       - Clone before major deployments
       - Use for QA and UAT
    
    2. NAMING CONVENTIONS
       - FMG_PRODUCTION_DEV_CLONE
       - FMG_PRODUCTION_QA_CLONE
       - _BACKUP suffix for safety copies
    
    3. CLEANUP CLONES
       - Set calendar reminders to drop old clones
       - Monitor storage from diverged clones
       - Document clone purpose and expiry
    
    4. TIME TRAVEL USAGE
       - Keep critical tables at max retention
       - Use for auditing and debugging
       - Don't rely on it for DR (use fail-safe)
    
    5. SECURITY
       - Clones inherit permissions at clone time
       - Update permissions if needed
       - Mask PII in dev clones if required
*/

-- ============================================================================
-- SECTION 12: CLEANUP
-- ============================================================================

-- Clean up the clones we created (optional)
-- DROP TABLE IF EXISTS FMG_PRODUCTION.RAW.CUSTOMERS_BACKUP;
-- DROP SCHEMA IF EXISTS FMG_PRODUCTION.RAW_BACKUP;
-- DROP DATABASE IF EXISTS FMG_PRODUCTION_DEV_CLONE;

-- For now, let's just drop the test schema
DROP SCHEMA IF EXISTS FMG_PRODUCTION.RAW_BACKUP;

-- ============================================================================
-- SCRIPT COMPLETE!
-- ============================================================================

SELECT '✅ Cloning & Time Travel Lab Complete!' AS STATUS,
       'Remember: Clones are instant and free until modified' AS KEY_TAKEAWAY,
       CURRENT_TIMESTAMP() AS COMPLETED_AT;

