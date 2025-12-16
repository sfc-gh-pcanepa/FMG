/*=============================================================================
  FMG SUITE - LAB 3: DATA TRANSFORMATIONS
  
  What you'll see:
  ✅ Dynamic Tables - auto-refreshing aggregations (no scheduling!)
  ✅ Zero-copy cloning - instant dev environments
  ✅ Time Travel - query/recover historical data
  
  Time: ~20 minutes
  Prerequisites: Labs 1-2 completed
=============================================================================*/

-- ============================================================================
-- STEP 1: ADD MORE DATA
-- ============================================================================
USE ROLE FMG_ADMIN;
USE WAREHOUSE FMG_ANALYTICS_WH;
USE SCHEMA FMG_LABS.PRODUCTION;

-- Create subscriptions table
CREATE OR REPLACE TABLE SUBSCRIPTIONS (
    subscription_id VARCHAR(20),
    customer_id VARCHAR(20),
    product VARCHAR(50),
    mrr DECIMAL(10,2),
    status VARCHAR(20),
    start_date DATE
);

INSERT INTO SUBSCRIPTIONS VALUES
    ('S001', 'C001', 'Marketing Suite', 1500.00, 'Active', '2022-01-15'),
    ('S002', 'C001', 'Website Pro', 500.00, 'Active', '2022-01-15'),
    ('S003', 'C001', 'MyRepChat', 500.00, 'Active', '2022-06-01'),
    ('S004', 'C002', 'Marketing Suite', 599.00, 'Active', '2022-03-20'),
    ('S005', 'C002', 'MyRepChat', 300.00, 'Active', '2022-03-20'),
    ('S006', 'C003', 'Marketing Suite', 299.00, 'Active', '2023-06-01'),
    ('S007', 'C004', 'Marketing Suite', 1800.00, 'Active', '2021-11-10'),
    ('S008', 'C004', 'Website Pro', 800.00, 'Active', '2021-11-10'),
    ('S009', 'C004', 'Do It For Me', 600.00, 'Cancelled', '2021-11-10'),
    ('S010', 'C005', 'Marketing Suite', 599.00, 'Active', '2023-01-25');

-- ============================================================================
-- STEP 2: DYNAMIC TABLE (Auto-Refreshing Aggregation!)
-- ============================================================================

-- Create a customer summary that AUTOMATICALLY stays up to date
CREATE OR REPLACE DYNAMIC TABLE FMG_LABS.PRODUCTION.CUSTOMER_360
    TARGET_LAG = '1 minute'  -- Refresh within 1 minute of source changes
    WAREHOUSE = FMG_ANALYTICS_WH
AS
SELECT 
    c.customer_id,
    c.company_name,
    c.segment,
    c.health_score,
    COUNT(s.subscription_id) AS product_count,
    SUM(s.mrr) AS total_mrr,
    SUM(CASE WHEN s.status = 'Active' THEN s.mrr ELSE 0 END) AS active_mrr
FROM CUSTOMERS c
LEFT JOIN SUBSCRIPTIONS s ON c.customer_id = s.customer_id
GROUP BY c.customer_id, c.company_name, c.segment, c.health_score;

-- Query the dynamic table
SELECT * FROM CUSTOMER_360 ORDER BY total_mrr DESC;

-- 🎯 Key insight: This table auto-refreshes. No cron jobs, no Airflow, no maintenance!

-- ============================================================================
-- STEP 3: SEE AUTO-REFRESH IN ACTION
-- ============================================================================

-- Add a new subscription
INSERT INTO SUBSCRIPTIONS VALUES
    ('S011', 'C003', 'Website Pro', 199.00, 'Active', CURRENT_DATE());

-- Wait a moment, then query again - it updates automatically!
-- (In production, you'd set TARGET_LAG based on your freshness needs)
SELECT * FROM CUSTOMER_360 WHERE customer_id = 'C003';

-- Check refresh history
SELECT * FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
WHERE NAME = 'CUSTOMER_360'
ORDER BY REFRESH_START_TIME DESC
LIMIT 5;

-- ============================================================================
-- STEP 4: ZERO-COPY CLONING (Instant Dev Environment!)
-- ============================================================================

-- Clone the entire database for development - INSTANT, NO EXTRA STORAGE!
CREATE DATABASE FMG_LABS_DEV CLONE FMG_LABS;

-- Verify it's there
SHOW DATABASES LIKE 'FMG_LABS%';

-- The clone is independent - changes don't affect the original
USE DATABASE FMG_LABS_DEV;
DELETE FROM PRODUCTION.CUSTOMERS WHERE segment = 'SMB';

-- Original is untouched
SELECT COUNT(*) AS dev_count FROM FMG_LABS_DEV.PRODUCTION.CUSTOMERS;
SELECT COUNT(*) AS prod_count FROM FMG_LABS.PRODUCTION.CUSTOMERS;

-- 🎯 Key insight: Clone is instant regardless of data size. Only stores the DELTA.

-- ============================================================================
-- STEP 5: TIME TRAVEL (Undo Mistakes!)
-- ============================================================================
USE DATABASE FMG_LABS;
USE SCHEMA PRODUCTION;

-- "Accidentally" delete data
DELETE FROM CUSTOMERS WHERE segment = 'Enterprise';

-- Oh no! How many did we lose?
SELECT COUNT(*) FROM CUSTOMERS;  -- Missing Enterprise customers!

-- Time Travel to the rescue - query data from 5 minutes ago
SELECT COUNT(*) FROM CUSTOMERS AT(OFFSET => -300);  -- 300 seconds ago

-- Restore the deleted data
INSERT INTO CUSTOMERS
SELECT * FROM CUSTOMERS AT(OFFSET => -300)
WHERE segment = 'Enterprise';

-- Verify restoration
SELECT segment, COUNT(*) FROM CUSTOMERS GROUP BY segment;

-- ============================================================================
-- STEP 6: UNDROP (Recover Dropped Objects!)
-- ============================================================================

-- "Accidentally" drop a table
DROP TABLE SUBSCRIPTIONS;

-- Gone?
SHOW TABLES LIKE 'SUBSCRIPTIONS';

-- Nope! Undrop it
UNDROP TABLE SUBSCRIPTIONS;

-- It's back!
SELECT COUNT(*) FROM SUBSCRIPTIONS;

-- ============================================================================
-- CLEANUP
-- ============================================================================
DROP DATABASE FMG_LABS_DEV;  -- Remove dev clone

-- ============================================================================
-- 🎉 LAB 3 COMPLETE!
-- ============================================================================
/*
  What you just saw:
  
  ✅ Dynamic Tables auto-refresh - no ETL scheduling needed
  ✅ Zero-copy cloning - instant dev/test environments
  ✅ Time Travel - query and recover historical data
  ✅ Undrop - recover accidentally dropped objects
  
  Key Snowflake Benefits:
  • Dynamic Tables eliminate ETL complexity
  • Cloning is instant regardless of data size
  • Time Travel provides built-in disaster recovery
  • No additional cost for clones (pay only for changes)
*/

