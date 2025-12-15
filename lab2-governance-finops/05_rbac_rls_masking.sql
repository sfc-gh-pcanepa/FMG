/*=============================================================================
  FMG SUITE - LAB 2: GOVERNANCE + FINOPS
  Script 5: RBAC + Row-Level Security + Data Masking
  
  Description: Implement fine-grained access control with RLS and masking policies
  Prerequisites: FMG roles created, data loaded
  Duration: ~15 minutes
=============================================================================*/

-- ============================================================================
-- SECTION 1: SET CONTEXT
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE FMG_DEV_XS;
USE DATABASE FMG_PRODUCTION;
USE SCHEMA GOVERNANCE;

-- ============================================================================
-- SECTION 2: UNDERSTANDING ACCESS CONTROL LAYERS
-- ============================================================================

/*
    Snowflake Access Control Layers:
    
    ┌─────────────────────────────────────────────────────────────────────────┐
    │                      ACCESS CONTROL STACK                                │
    ├─────────────────────────────────────────────────────────────────────────┤
    │                                                                          │
    │  Layer 1: RBAC (Role-Based Access Control)                              │
    │  ───────────────────────────────────────────                            │
    │  WHO can access WHAT objects (databases, schemas, tables)               │
    │  Implemented via: GRANT/REVOKE on roles                                 │
    │                                                                          │
    │  Layer 2: Row Access Policies (RLS)                                     │
    │  ───────────────────────────────────────────                            │
    │  WHICH ROWS can a user see within a table                               │
    │  Implemented via: CREATE ROW ACCESS POLICY                              │
    │                                                                          │
    │  Layer 3: Column Masking Policies                                       │
    │  ───────────────────────────────────────────                            │
    │  HOW column values appear (masked, partial, full)                       │
    │  Implemented via: CREATE MASKING POLICY                                 │
    │                                                                          │
    │  Layer 4: Secure Views                                                  │
    │  ───────────────────────────────────────────                            │
    │  Additional query-time logic and aggregation                            │
    │  Implemented via: CREATE SECURE VIEW                                    │
    │                                                                          │
    └─────────────────────────────────────────────────────────────────────────┘
*/

-- ============================================================================
-- SECTION 3: CREATE MASKING POLICIES
-- ============================================================================

/*
    FMG Masking Requirements:
    
    Data Type          | FMG_ADMIN | FMG_COMPLIANCE | FMG_ANALYST | FMG_VIEWER
    ───────────────────|───────────|────────────────|─────────────|────────────
    Email              | Full      | Full           | Masked      | Masked
    Phone              | Full      | Full           | Partial     | Masked
    Company Name       | Full      | Full           | Full        | Full
    Revenue (MRR/ARR)  | Full      | Full           | Full        | Masked
    Health Score       | Full      | Full           | Full        | Masked
*/

-- Email Masking Policy
CREATE OR REPLACE MASKING POLICY FMG_EMAIL_MASK AS (val STRING) 
RETURNS STRING ->
    CASE 
        -- Full access for admins and compliance
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'FMG_ADMIN', 'FMG_COMPLIANCE_OFFICER') 
            THEN val
        -- Mask the local part, show domain
        WHEN CURRENT_ROLE() IN ('FMG_ANALYST', 'FMG_ENGINEER') 
            THEN REGEXP_REPLACE(val, '^[^@]+', '****')
        -- Fully masked for viewers
        ELSE '****@****.***'
    END;

-- Phone Masking Policy (show last 4 digits)
CREATE OR REPLACE MASKING POLICY FMG_PHONE_MASK AS (val STRING) 
RETURNS STRING ->
    CASE 
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'FMG_ADMIN', 'FMG_COMPLIANCE_OFFICER') 
            THEN val
        WHEN CURRENT_ROLE() IN ('FMG_ANALYST', 'FMG_ENGINEER') 
            THEN REGEXP_REPLACE(val, '\\d(?=\\d{4})', '*')  -- Show last 4
        ELSE '(***) ***-****'
    END;

-- Revenue Masking Policy
CREATE OR REPLACE MASKING POLICY FMG_REVENUE_MASK AS (val NUMBER) 
RETURNS NUMBER ->
    CASE 
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'FMG_ADMIN', 'FMG_COMPLIANCE_OFFICER', 'FMG_ANALYST', 'FMG_ENGINEER') 
            THEN val
        -- Show rounded bucket for viewers
        WHEN CURRENT_ROLE() = 'FMG_VIEWER' 
            THEN ROUND(val, -2)  -- Round to nearest 100
        ELSE NULL
    END;

-- Name Masking Policy (for first/last names)
CREATE OR REPLACE MASKING POLICY FMG_NAME_MASK AS (val STRING) 
RETURNS STRING ->
    CASE 
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'FMG_ADMIN', 'FMG_COMPLIANCE_OFFICER') 
            THEN val
        WHEN CURRENT_ROLE() IN ('FMG_ANALYST', 'FMG_ENGINEER') 
            THEN LEFT(val, 1) || '***'  -- Show first initial only
        ELSE '***'
    END;

-- ============================================================================
-- SECTION 4: APPLY MASKING POLICIES TO COLUMNS
-- ============================================================================

-- Apply email masking to USERS table
ALTER TABLE FMG_PRODUCTION.RAW.USERS 
    MODIFY COLUMN email SET MASKING POLICY FMG_EMAIL_MASK;

-- Apply phone masking to USERS table
ALTER TABLE FMG_PRODUCTION.RAW.USERS 
    MODIFY COLUMN phone SET MASKING POLICY FMG_PHONE_MASK;

-- Apply name masking
ALTER TABLE FMG_PRODUCTION.RAW.USERS 
    MODIFY COLUMN first_name SET MASKING POLICY FMG_NAME_MASK;
ALTER TABLE FMG_PRODUCTION.RAW.USERS 
    MODIFY COLUMN last_name SET MASKING POLICY FMG_NAME_MASK;

-- Apply revenue masking to subscriptions
ALTER TABLE FMG_PRODUCTION.RAW.SUBSCRIPTIONS 
    MODIFY COLUMN mrr_amount SET MASKING POLICY FMG_REVENUE_MASK;
ALTER TABLE FMG_PRODUCTION.RAW.SUBSCRIPTIONS 
    MODIFY COLUMN arr_amount SET MASKING POLICY FMG_REVENUE_MASK;

-- Verify masking policies are applied
SELECT 
    policy_name,
    policy_kind,
    ref_database_name,
    ref_schema_name,
    ref_entity_name AS table_name,
    ref_column_name AS column_name
FROM TABLE(INFORMATION_SCHEMA.POLICY_REFERENCES(
    policy_name => 'FMG_PRODUCTION.GOVERNANCE.FMG_EMAIL_MASK'
));

-- ============================================================================
-- SECTION 5: TEST MASKING POLICIES
-- ============================================================================

-- Test as FMG_ADMIN (should see full data)
USE ROLE FMG_ADMIN;
USE WAREHOUSE FMG_DEV_XS;

SELECT user_id, email, phone, first_name, last_name 
FROM FMG_PRODUCTION.RAW.USERS 
LIMIT 5;

-- Test as FMG_ANALYST (should see partial masking)
USE ROLE FMG_ANALYST;

SELECT user_id, email, phone, first_name, last_name 
FROM FMG_PRODUCTION.RAW.USERS 
LIMIT 5;

-- Test as FMG_VIEWER (should see full masking)
USE ROLE FMG_VIEWER;

SELECT user_id, email, phone, first_name, last_name 
FROM FMG_PRODUCTION.RAW.USERS 
LIMIT 5;

-- Reset to admin
USE ROLE ACCOUNTADMIN;

-- ============================================================================
-- SECTION 6: CREATE ROW ACCESS POLICIES
-- ============================================================================

/*
    FMG Row-Level Security Requirements:
    
    Scenario 1: CSM Team members can only see customers they own
    Scenario 2: Regional managers see only their region's data
    Scenario 3: Analysts see only active customers (not churned)
*/

USE SCHEMA FMG_PRODUCTION.GOVERNANCE;

-- Create a mapping table for CSM assignments
CREATE OR REPLACE TABLE CSM_USER_MAPPING (
    snowflake_user VARCHAR(100),
    csm_name VARCHAR(100),
    PRIMARY KEY (snowflake_user)
);

-- Insert sample mappings (in production, sync from HR system)
INSERT INTO CSM_USER_MAPPING VALUES 
    ('FMG_DEMO_ANALYST', 'Sarah Mitchell'),
    ('FMG_DEMO_COMPLIANCE', NULL),  -- Compliance sees all
    ('FMG_DEMO_EXEC', NULL);  -- Execs see all

-- Row Access Policy: CSMs see only their customers
CREATE OR REPLACE ROW ACCESS POLICY CSM_CUSTOMER_ACCESS AS (csm_owner VARCHAR) 
RETURNS BOOLEAN ->
    CASE
        -- Admins and compliance see all
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'FMG_ADMIN', 'FMG_COMPLIANCE_OFFICER', 'FMG_DATA_SCIENTIST') 
            THEN TRUE
        -- CSMs see only their customers
        WHEN CURRENT_ROLE() IN ('FMG_ANALYST', 'FMG_VIEWER') 
            THEN csm_owner = (
                SELECT csm_name 
                FROM FMG_PRODUCTION.GOVERNANCE.CSM_USER_MAPPING 
                WHERE snowflake_user = CURRENT_USER()
            )
            OR EXISTS (
                SELECT 1 
                FROM FMG_PRODUCTION.GOVERNANCE.CSM_USER_MAPPING 
                WHERE snowflake_user = CURRENT_USER() 
                AND csm_name IS NULL  -- NULL means see all
            )
        -- Engineers see all for data work
        WHEN CURRENT_ROLE() = 'FMG_ENGINEER' THEN TRUE
        ELSE FALSE
    END;

-- Row Access Policy: Only show active customers to certain roles
CREATE OR REPLACE ROW ACCESS POLICY ACTIVE_CUSTOMER_ACCESS AS (account_status VARCHAR) 
RETURNS BOOLEAN ->
    CASE
        -- Admins see all statuses
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'FMG_ADMIN', 'FMG_COMPLIANCE_OFFICER', 'FMG_ENGINEER') 
            THEN TRUE
        -- Analysts and viewers only see active/paused
        WHEN CURRENT_ROLE() IN ('FMG_ANALYST', 'FMG_VIEWER', 'FMG_DATA_SCIENTIST') 
            THEN account_status IN ('Active', 'Paused', 'Trial')
        ELSE FALSE
    END;

-- ============================================================================
-- SECTION 7: APPLY ROW ACCESS POLICIES
-- ============================================================================

-- Apply active customer filter to CUSTOMERS table
ALTER TABLE FMG_PRODUCTION.RAW.CUSTOMERS 
    ADD ROW ACCESS POLICY ACTIVE_CUSTOMER_ACCESS ON (account_status);

-- Note: Can only have one row access policy per table
-- If you need multiple conditions, combine them in a single policy

-- Verify row access policy is applied
SELECT 
    policy_name,
    policy_kind,
    ref_entity_name AS table_name,
    ref_arg_column_names AS policy_columns
FROM TABLE(INFORMATION_SCHEMA.POLICY_REFERENCES(
    ref_entity_domain => 'TABLE',
    ref_entity_name => 'FMG_PRODUCTION.RAW.CUSTOMERS'
));

-- ============================================================================
-- SECTION 8: TEST ROW ACCESS POLICIES
-- ============================================================================

-- Count all customers as admin
USE ROLE ACCOUNTADMIN;
SELECT account_status, COUNT(*) 
FROM FMG_PRODUCTION.RAW.CUSTOMERS 
GROUP BY 1;

-- Count as analyst (should not see churned)
USE ROLE FMG_ANALYST;
SELECT account_status, COUNT(*) 
FROM FMG_PRODUCTION.RAW.CUSTOMERS 
GROUP BY 1;

-- Reset to admin
USE ROLE ACCOUNTADMIN;

-- ============================================================================
-- SECTION 9: TAG-BASED POLICIES
-- ============================================================================

/*
    Tag-based policies allow you to:
    - Apply policies based on column tags (e.g., PII_CATEGORY = 'DIRECT_IDENTIFIER')
    - Automatically protect new columns that get tagged
    - Centralize policy management
*/

-- Example: Create a tag-based masking policy
CREATE OR REPLACE MASKING POLICY TAG_BASED_PII_MASK AS (val STRING) 
RETURNS STRING ->
    CASE 
        -- Check if current column has PII tag
        WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('FMG_PRODUCTION.GOVERNANCE.PII_CATEGORY') = 'DIRECT_IDENTIFIER'
            AND CURRENT_ROLE() NOT IN ('ACCOUNTADMIN', 'FMG_ADMIN', 'FMG_COMPLIANCE_OFFICER')
            THEN '***REDACTED***'
        WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('FMG_PRODUCTION.GOVERNANCE.PII_CATEGORY') = 'QUASI_IDENTIFIER'
            AND CURRENT_ROLE() NOT IN ('ACCOUNTADMIN', 'FMG_ADMIN', 'FMG_COMPLIANCE_OFFICER')
            THEN LEFT(val, 1) || '***'
        ELSE val
    END;

-- ============================================================================
-- SECTION 10: CREATE GOVERNANCE SUMMARY VIEW
-- ============================================================================

-- View of all policies in effect
CREATE OR REPLACE VIEW V_POLICY_SUMMARY AS
SELECT 
    policy_name,
    policy_kind,
    'MASKING' AS policy_type,
    ref_database_name || '.' || ref_schema_name || '.' || ref_entity_name AS full_table_name,
    ref_column_name AS column_or_policy_column,
    NULL AS policy_condition
FROM TABLE(INFORMATION_SCHEMA.POLICY_REFERENCES(
    policy_name => 'FMG_PRODUCTION.GOVERNANCE.FMG_EMAIL_MASK'
))
UNION ALL
SELECT 
    policy_name,
    policy_kind,
    'MASKING',
    ref_database_name || '.' || ref_schema_name || '.' || ref_entity_name,
    ref_column_name,
    NULL
FROM TABLE(INFORMATION_SCHEMA.POLICY_REFERENCES(
    policy_name => 'FMG_PRODUCTION.GOVERNANCE.FMG_PHONE_MASK'
))
UNION ALL
SELECT 
    policy_name,
    policy_kind,
    'MASKING',
    ref_database_name || '.' || ref_schema_name || '.' || ref_entity_name,
    ref_column_name,
    NULL
FROM TABLE(INFORMATION_SCHEMA.POLICY_REFERENCES(
    policy_name => 'FMG_PRODUCTION.GOVERNANCE.FMG_NAME_MASK'
))
UNION ALL
SELECT 
    policy_name,
    policy_kind,
    'MASKING',
    ref_database_name || '.' || ref_schema_name || '.' || ref_entity_name,
    ref_column_name,
    NULL
FROM TABLE(INFORMATION_SCHEMA.POLICY_REFERENCES(
    policy_name => 'FMG_PRODUCTION.GOVERNANCE.FMG_REVENUE_MASK'
))
UNION ALL
SELECT 
    policy_name,
    policy_kind,
    'ROW_ACCESS',
    ref_database_name || '.' || ref_schema_name || '.' || ref_entity_name,
    ARRAY_TO_STRING(ref_arg_column_names, ', '),
    NULL
FROM TABLE(INFORMATION_SCHEMA.POLICY_REFERENCES(
    policy_name => 'FMG_PRODUCTION.GOVERNANCE.ACTIVE_CUSTOMER_ACCESS'
));

-- View all policies
SELECT * FROM V_POLICY_SUMMARY;

-- ============================================================================
-- SECTION 11: CLEANUP POLICIES (If Needed)
-- ============================================================================

/*
    To remove policies:
    
    -- Remove masking policy from column
    ALTER TABLE FMG_PRODUCTION.RAW.USERS 
        MODIFY COLUMN email UNSET MASKING POLICY;
    
    -- Remove row access policy from table
    ALTER TABLE FMG_PRODUCTION.RAW.CUSTOMERS 
        DROP ROW ACCESS POLICY ACTIVE_CUSTOMER_ACCESS;
    
    -- Drop the policy
    DROP MASKING POLICY FMG_EMAIL_MASK;
    DROP ROW ACCESS POLICY ACTIVE_CUSTOMER_ACCESS;
*/

-- ============================================================================
-- SECTION 12: BEST PRACTICES SUMMARY
-- ============================================================================

/*
    FMG DATA PROTECTION BEST PRACTICES:
    
    1. DEFENSE IN DEPTH
       - Layer 1: RBAC for object access
       - Layer 2: Row policies for data filtering
       - Layer 3: Masking for column protection
       - Layer 4: Secure views for complex logic
    
    2. LEAST PRIVILEGE
       - Start with no access, grant as needed
       - Regular access reviews
       - Remove unused permissions
    
    3. CONSISTENT CLASSIFICATION
       - Tag all sensitive columns
       - Use standard categories (PII, CONFIDENTIAL, etc.)
       - Automate where possible
    
    4. POLICY GOVERNANCE
       - Document all policies
       - Test policies before production
       - Audit policy effectiveness
    
    5. MONITOR AND AUDIT
       - Track who sees what data
       - Alert on unusual access patterns
       - Regular compliance reviews
*/

-- ============================================================================
-- SCRIPT COMPLETE!
-- ============================================================================

SELECT '✅ RBAC, RLS, and Masking Setup Complete!' AS STATUS,
       'Test with different roles to verify access' AS NEXT_STEP,
       CURRENT_TIMESTAMP() AS COMPLETED_AT;

