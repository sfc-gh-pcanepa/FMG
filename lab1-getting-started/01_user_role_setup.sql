/*=============================================================================
  FMG SUITE - LAB 1: GETTING STARTED WITH SNOWFLAKE
  Script 1: User Creation & Role Assignment
  
  Description: Create demo users and service accounts for FMG labs
  Prerequisites: Run setup/00_environment_setup.sql first
=============================================================================*/

-- ============================================================================
-- SECTION 1: SET CONTEXT
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- Verify environment setup was completed
SHOW ROLES LIKE 'FMG%';
-- You should see: FMG_ADMIN, FMG_ANALYST, FMG_VIEWER, FMG_ENGINEER, 
--                 FMG_COMPLIANCE_OFFICER, FMG_DATA_SCIENTIST

-- ============================================================================
-- SECTION 2: CREATE DEMO USERS
-- ============================================================================

/*
    Create sample users representing different teams at FMG.
    In production, users would typically be provisioned via SSO/SCIM.
    
    NOTE: Change passwords before using in any real environment!
*/

-- Data Engineer User
CREATE USER IF NOT EXISTS FMG_DEMO_ENGINEER
    PASSWORD = 'TempPassword123!'  -- Change in production!
    DEFAULT_ROLE = FMG_ENGINEER
    DEFAULT_WAREHOUSE = FMG_DEV_XS
    DEFAULT_NAMESPACE = FMG_PRODUCTION.RAW
    MUST_CHANGE_PASSWORD = TRUE
    COMMENT = 'Demo user for FMG data engineering team';

-- Analyst User
CREATE USER IF NOT EXISTS FMG_DEMO_ANALYST
    PASSWORD = 'TempPassword123!'  -- Change in production!
    DEFAULT_ROLE = FMG_ANALYST
    DEFAULT_WAREHOUSE = FMG_PROD_S
    DEFAULT_NAMESPACE = FMG_ANALYTICS.MARKETING
    MUST_CHANGE_PASSWORD = TRUE
    COMMENT = 'Demo user for FMG analytics team';

-- Compliance Officer User
CREATE USER IF NOT EXISTS FMG_DEMO_COMPLIANCE
    PASSWORD = 'TempPassword123!'  -- Change in production!
    DEFAULT_ROLE = FMG_COMPLIANCE_OFFICER
    DEFAULT_WAREHOUSE = FMG_PROD_S
    DEFAULT_NAMESPACE = FMG_PRODUCTION.COMPLIANCE
    MUST_CHANGE_PASSWORD = TRUE
    COMMENT = 'Demo user for FMG compliance team';

-- Data Scientist User
CREATE USER IF NOT EXISTS FMG_DEMO_DS
    PASSWORD = 'TempPassword123!'  -- Change in production!
    DEFAULT_ROLE = FMG_DATA_SCIENTIST
    DEFAULT_WAREHOUSE = FMG_ML_L
    DEFAULT_NAMESPACE = FMG_ANALYTICS.ADVISOR_360
    MUST_CHANGE_PASSWORD = TRUE
    COMMENT = 'Demo user for FMG data science team';

-- Executive Viewer User
CREATE USER IF NOT EXISTS FMG_DEMO_EXEC
    PASSWORD = 'TempPassword123!'  -- Change in production!
    DEFAULT_ROLE = FMG_VIEWER
    DEFAULT_WAREHOUSE = FMG_PROD_S
    DEFAULT_NAMESPACE = FMG_ANALYTICS.EXECUTIVE
    MUST_CHANGE_PASSWORD = TRUE
    COMMENT = 'Demo user for FMG executive stakeholders';

-- ============================================================================
-- SECTION 3: ASSIGN ROLES TO USERS
-- ============================================================================

GRANT ROLE FMG_ENGINEER TO USER FMG_DEMO_ENGINEER;
GRANT ROLE FMG_ANALYST TO USER FMG_DEMO_ANALYST;
GRANT ROLE FMG_COMPLIANCE_OFFICER TO USER FMG_DEMO_COMPLIANCE;
GRANT ROLE FMG_DATA_SCIENTIST TO USER FMG_DEMO_DS;
GRANT ROLE FMG_VIEWER TO USER FMG_DEMO_EXEC;

-- Grant your current user access to FMG_ADMIN for testing
-- Replace YOUR_USERNAME with your actual Snowflake username
-- GRANT ROLE FMG_ADMIN TO USER YOUR_USERNAME;
SELECT 'Run: GRANT ROLE FMG_ADMIN TO USER ' || CURRENT_USER() AS GRANT_COMMAND;

-- Verify
SHOW USERS LIKE 'FMG_DEMO%';

-- ============================================================================
-- SECTION 4: CREATE SERVICE ACCOUNT ROLES
-- ============================================================================

/*
    Service accounts are used by applications, ETL tools, and BI platforms.
    They should have their own roles with specific, limited permissions.
*/

-- Service role for ETL pipelines (e.g., Fivetran, Airbyte)
CREATE ROLE IF NOT EXISTS FMG_SVC_ETL
    COMMENT = 'Service role for ETL pipeline tools';

-- Service role for BI tools (e.g., Tableau, Looker)
CREATE ROLE IF NOT EXISTS FMG_SVC_BI
    COMMENT = 'Service role for BI and visualization tools';

-- Grant service roles to FMG_ADMIN
GRANT ROLE FMG_SVC_ETL TO ROLE FMG_ADMIN;
GRANT ROLE FMG_SVC_BI TO ROLE FMG_ADMIN;

-- Grant ETL service role permissions
GRANT USAGE ON DATABASE FMG_PRODUCTION TO ROLE FMG_SVC_ETL;
GRANT USAGE ON ALL SCHEMAS IN DATABASE FMG_PRODUCTION TO ROLE FMG_SVC_ETL;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA FMG_PRODUCTION.RAW TO ROLE FMG_SVC_ETL;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA FMG_PRODUCTION.STAGING TO ROLE FMG_SVC_ETL;
GRANT USAGE ON WAREHOUSE FMG_PROD_S TO ROLE FMG_SVC_ETL;

-- Grant BI service role permissions
GRANT USAGE ON DATABASE FMG_ANALYTICS TO ROLE FMG_SVC_BI;
GRANT USAGE ON ALL SCHEMAS IN DATABASE FMG_ANALYTICS TO ROLE FMG_SVC_BI;
GRANT SELECT ON ALL TABLES IN DATABASE FMG_ANALYTICS TO ROLE FMG_SVC_BI;
GRANT SELECT ON ALL VIEWS IN DATABASE FMG_ANALYTICS TO ROLE FMG_SVC_BI;
GRANT USAGE ON WAREHOUSE FMG_ANALYTICS_M TO ROLE FMG_SVC_BI;

-- ============================================================================
-- SECTION 5: TEST ROLE ACCESS
-- ============================================================================

-- Test as FMG_ANALYST
USE ROLE FMG_ANALYST;
USE WAREHOUSE FMG_DEV_XS;

-- Should work: Read from production
SELECT COUNT(*) AS customer_count FROM FMG_PRODUCTION.RAW.CUSTOMERS;

-- Should work: Read from analytics
SELECT segment, COUNT(*) AS customers
FROM FMG_PRODUCTION.RAW.CUSTOMERS
GROUP BY segment;

-- This would FAIL (analysts are read-only):
-- INSERT INTO FMG_PRODUCTION.RAW.CUSTOMERS (customer_id) VALUES ('TEST');

-- Test as FMG_ADMIN
USE ROLE FMG_ADMIN;

-- Should work: Full access
SELECT account_status, COUNT(*) AS customers
FROM FMG_PRODUCTION.RAW.CUSTOMERS
GROUP BY account_status;

-- Switch back
USE ROLE ACCOUNTADMIN;

-- ============================================================================
-- COMPLETE!
-- ============================================================================

SELECT '✅ User and Role Setup Complete!' AS STATUS,
       CURRENT_TIMESTAMP() AS COMPLETED_AT;
