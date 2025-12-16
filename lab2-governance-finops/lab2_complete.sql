/*=============================================================================
  FMG SUITE - LAB 2: GOVERNANCE & FINOPS
  
  What you'll see:
  ✅ Tag data for classification in seconds
  ✅ Dynamic data masking (same query, different results by role)
  ✅ Resource monitors to control costs
  ✅ Query audit history
  
  Time: ~20 minutes
  Prerequisites: Lab 1 completed
=============================================================================*/

-- ============================================================================
-- STEP 1: ADD SENSITIVE DATA
-- ============================================================================
USE ROLE FMG_ADMIN;
USE WAREHOUSE FMG_ANALYTICS_WH;
USE SCHEMA FMG_LABS.PRODUCTION;

-- Add users table with PII (email, phone)
CREATE OR REPLACE TABLE USERS (
    user_id VARCHAR(20),
    customer_id VARCHAR(20),
    email VARCHAR(200),
    phone VARCHAR(20),
    full_name VARCHAR(100),
    role VARCHAR(50)
);

INSERT INTO USERS VALUES
    ('U001', 'C001', 'john.smith@acmefinancial.com', '(555) 123-4567', 'John Smith', 'Admin'),
    ('U002', 'C001', 'sarah.jones@acmefinancial.com', '(555) 234-5678', 'Sarah Jones', 'Advisor'),
    ('U003', 'C002', 'mike.chen@summitwm.com', '(555) 345-6789', 'Mike Chen', 'Admin'),
    ('U004', 'C003', 'lisa.park@peakadvisory.com', '(555) 456-7890', 'Lisa Park', 'Advisor'),
    ('U005', 'C004', 'david.wilson@horizonfin.com', '(555) 567-8901', 'David Wilson', 'Compliance');

-- ============================================================================
-- STEP 2: CREATE GOVERNANCE TAGS
-- ============================================================================
USE ROLE ACCOUNTADMIN;

CREATE SCHEMA IF NOT EXISTS FMG_LABS.GOVERNANCE;

-- Create a sensitivity tag
CREATE OR REPLACE TAG FMG_LABS.GOVERNANCE.SENSITIVITY
    ALLOWED_VALUES = 'PUBLIC', 'INTERNAL', 'CONFIDENTIAL', 'PII'
    COMMENT = 'Data sensitivity classification';

-- Apply tags to columns (instant!)
ALTER TABLE FMG_LABS.PRODUCTION.USERS MODIFY COLUMN email 
    SET TAG FMG_LABS.GOVERNANCE.SENSITIVITY = 'PII';
ALTER TABLE FMG_LABS.PRODUCTION.USERS MODIFY COLUMN phone 
    SET TAG FMG_LABS.GOVERNANCE.SENSITIVITY = 'PII';

-- Verify tags
SELECT * FROM TABLE(FMG_LABS.INFORMATION_SCHEMA.TAG_REFERENCES(
    'FMG_LABS.PRODUCTION.USERS', 'TABLE'));

-- ============================================================================
-- STEP 3: DYNAMIC DATA MASKING (The Magic!)
-- ============================================================================

-- Create masking policy: Admins see real data, others see masked
CREATE OR REPLACE MASKING POLICY FMG_LABS.GOVERNANCE.EMAIL_MASK AS (val STRING)
RETURNS STRING ->
    CASE 
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'FMG_ADMIN') THEN val
        ELSE '****@****.***'
    END;

CREATE OR REPLACE MASKING POLICY FMG_LABS.GOVERNANCE.PHONE_MASK AS (val STRING)
RETURNS STRING ->
    CASE 
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'FMG_ADMIN') THEN val
        ELSE '(***) ***-' || RIGHT(val, 4)  -- Show last 4 digits only
    END;

-- Apply policies to columns
ALTER TABLE FMG_LABS.PRODUCTION.USERS MODIFY COLUMN email 
    SET MASKING POLICY FMG_LABS.GOVERNANCE.EMAIL_MASK;
ALTER TABLE FMG_LABS.PRODUCTION.USERS MODIFY COLUMN phone 
    SET MASKING POLICY FMG_LABS.GOVERNANCE.PHONE_MASK;

-- ============================================================================
-- STEP 4: SEE MASKING IN ACTION
-- ============================================================================

-- As ADMIN: See full data
USE ROLE FMG_ADMIN;
USE SECONDARY ROLES NONE;
SELECT user_id, full_name, email, phone FROM FMG_LABS.PRODUCTION.USERS;
-- Result: john.smith@acmefinancial.com, (555) 123-4567

-- As ANALYST: See masked data (SAME QUERY, DIFFERENT RESULT!)
USE ROLE FMG_ANALYST;
USE SECONDARY ROLES NONE;
USE WAREHOUSE FMG_ANALYTICS_WH;
SELECT user_id, full_name, email, phone FROM FMG_LABS.PRODUCTION.USERS;
-- Result: ****@****.***,  (***) ***-4567

-- 🎯 Key insight: No code changes needed! Security is automatic based on role.

-- ============================================================================
-- STEP 5: RESOURCE MONITOR (Cost Control)
-- ============================================================================
USE ROLE ACCOUNTADMIN;

-- Create a budget guard - get alerted and auto-suspend at limits
CREATE OR REPLACE RESOURCE MONITOR FMG_BUDGET
    WITH CREDIT_QUOTA = 100  -- Monthly limit
    TRIGGERS
        ON 75 PERCENT DO NOTIFY           -- Alert at 75%
        ON 90 PERCENT DO NOTIFY           -- Alert at 90%
        ON 100 PERCENT DO SUSPEND;        -- Stop spending at 100%

-- Apply to warehouse
ALTER WAREHOUSE FMG_ANALYTICS_WH SET RESOURCE_MONITOR = FMG_BUDGET;

-- Check status
SHOW RESOURCE MONITORS;

-- ============================================================================
-- STEP 6: AUDIT - WHO DID WHAT?
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
*/

