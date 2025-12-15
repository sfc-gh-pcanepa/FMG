/*=============================================================================
  FMG SUITE - LAB 1: GETTING STARTED WITH SNOWFLAKE
  Script 1: User Creation & Role Granting
  
  Description: Create FMG-specific users, roles, and establish role hierarchy
  Prerequisites: ACCOUNTADMIN access, 00_environment_setup.sql completed
  Duration: ~15 minutes
=============================================================================*/

-- ============================================================================
-- SECTION 1: SET CONTEXT
-- ============================================================================

-- You need ACCOUNTADMIN or SECURITYADMIN to manage users and roles
USE ROLE ACCOUNTADMIN;

-- ============================================================================
-- SECTION 2: UNDERSTAND SYSTEM-DEFINED ROLES
-- ============================================================================

/*
    Snowflake comes with these built-in roles:
    
    ACCOUNTADMIN    - Top-level role, can do everything
    SECURITYADMIN   - Manages users, roles, and privileges  
    SYSADMIN        - Creates databases, warehouses, and objects
    USERADMIN       - Creates and manages users and roles
    PUBLIC          - Automatically granted to all users
    
    Best Practice: Use custom roles for application-specific access
*/

-- View existing roles in your account
SHOW ROLES;

-- See what roles you currently have access to
SELECT CURRENT_ROLE(), CURRENT_USER(), CURRENT_ACCOUNT();

-- ============================================================================
-- SECTION 3: CREATE FMG CUSTOM ROLES
-- ============================================================================

/*
    FMG Role Hierarchy Design:
    
    ACCOUNTADMIN
         │
         └── SYSADMIN
                 │
                 └── FMG_ADMIN (Full FMG access)
                         │
                         ├── FMG_ENGINEER (ETL, transformations, full dev access)
                         │
                         ├── FMG_ANALYST (Read analytics, run queries, create views)
                         │       │
                         │       └── FMG_VIEWER (Read-only access)
                         │
                         ├── FMG_COMPLIANCE_OFFICER (Audit access, PII viewing)
                         │
                         └── FMG_DATA_SCIENTIST (ML/AI workloads, Cortex access)
*/

-- Create roles with descriptive comments
CREATE ROLE IF NOT EXISTS FMG_ADMIN
    COMMENT = 'FMG administrative role with full database access';

CREATE ROLE IF NOT EXISTS FMG_ENGINEER
    COMMENT = 'FMG data engineer role for ETL, transformations, and pipeline development';

CREATE ROLE IF NOT EXISTS FMG_ANALYST
    COMMENT = 'FMG analyst role for reporting, dashboards, and ad-hoc analysis';

CREATE ROLE IF NOT EXISTS FMG_VIEWER
    COMMENT = 'FMG read-only viewer role for stakeholders needing data access';

CREATE ROLE IF NOT EXISTS FMG_COMPLIANCE_OFFICER
    COMMENT = 'FMG compliance officer role with audit log and PII access';

CREATE ROLE IF NOT EXISTS FMG_DATA_SCIENTIST
    COMMENT = 'FMG data scientist role for ML/AI workloads and Cortex features';

-- ============================================================================
-- SECTION 4: ESTABLISH ROLE HIERARCHY
-- ============================================================================

/*
    Role hierarchy allows privileges to "flow up" the chain.
    If FMG_VIEWER can SELECT on a table, and FMG_ANALYST inherits FMG_VIEWER,
    then FMG_ANALYST automatically gets SELECT too.
*/

-- FMG_VIEWER is the base role - inherited by FMG_ANALYST
GRANT ROLE FMG_VIEWER TO ROLE FMG_ANALYST;

-- FMG_ANALYST, FMG_ENGINEER, etc. are inherited by FMG_ADMIN
GRANT ROLE FMG_ANALYST TO ROLE FMG_ADMIN;
GRANT ROLE FMG_ENGINEER TO ROLE FMG_ADMIN;
GRANT ROLE FMG_COMPLIANCE_OFFICER TO ROLE FMG_ADMIN;
GRANT ROLE FMG_DATA_SCIENTIST TO ROLE FMG_ADMIN;

-- FMG_ADMIN reports to SYSADMIN (best practice for non-account-level management)
GRANT ROLE FMG_ADMIN TO ROLE SYSADMIN;

-- Verify the hierarchy
SHOW GRANTS TO ROLE FMG_ANALYST;
SHOW GRANTS TO ROLE FMG_ADMIN;

-- ============================================================================
-- SECTION 5: CREATE DEMO USERS
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
-- SECTION 6: ASSIGN ROLES TO USERS
-- ============================================================================

-- Grant appropriate roles to each user
GRANT ROLE FMG_ENGINEER TO USER FMG_DEMO_ENGINEER;
GRANT ROLE FMG_ANALYST TO USER FMG_DEMO_ANALYST;
GRANT ROLE FMG_COMPLIANCE_OFFICER TO USER FMG_DEMO_COMPLIANCE;
GRANT ROLE FMG_DATA_SCIENTIST TO USER FMG_DEMO_DS;
GRANT ROLE FMG_VIEWER TO USER FMG_DEMO_EXEC;

-- Also grant your current user access to all FMG roles for testing
GRANT ROLE FMG_ADMIN TO USER IDENTIFIER($CURRENT_USER);

-- Verify user-role assignments
SHOW GRANTS TO USER FMG_DEMO_ANALYST;
SHOW GRANTS TO USER FMG_DEMO_ENGINEER;

-- ============================================================================
-- SECTION 7: GRANT DATABASE & SCHEMA PRIVILEGES
-- ============================================================================

-- FMG_ADMIN: Full access to all FMG databases
GRANT ALL PRIVILEGES ON DATABASE FMG_PRODUCTION TO ROLE FMG_ADMIN;
GRANT ALL PRIVILEGES ON DATABASE FMG_DEVELOPMENT TO ROLE FMG_ADMIN;
GRANT ALL PRIVILEGES ON DATABASE FMG_ANALYTICS TO ROLE FMG_ADMIN;

-- Grant on all existing and future schemas
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE FMG_PRODUCTION TO ROLE FMG_ADMIN;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE FMG_DEVELOPMENT TO ROLE FMG_ADMIN;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE FMG_ANALYTICS TO ROLE FMG_ADMIN;

GRANT ALL PRIVILEGES ON FUTURE SCHEMAS IN DATABASE FMG_PRODUCTION TO ROLE FMG_ADMIN;
GRANT ALL PRIVILEGES ON FUTURE SCHEMAS IN DATABASE FMG_DEVELOPMENT TO ROLE FMG_ADMIN;
GRANT ALL PRIVILEGES ON FUTURE SCHEMAS IN DATABASE FMG_ANALYTICS TO ROLE FMG_ADMIN;

-- FMG_ENGINEER: Production read/write, development full access
GRANT USAGE ON DATABASE FMG_PRODUCTION TO ROLE FMG_ENGINEER;
GRANT USAGE ON DATABASE FMG_DEVELOPMENT TO ROLE FMG_ENGINEER;
GRANT USAGE ON DATABASE FMG_ANALYTICS TO ROLE FMG_ENGINEER;

GRANT USAGE ON ALL SCHEMAS IN DATABASE FMG_PRODUCTION TO ROLE FMG_ENGINEER;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE FMG_DEVELOPMENT TO ROLE FMG_ENGINEER;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE FMG_ANALYTICS TO ROLE FMG_ENGINEER;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN DATABASE FMG_PRODUCTION TO ROLE FMG_ENGINEER;
GRANT ALL PRIVILEGES ON ALL TABLES IN DATABASE FMG_DEVELOPMENT TO ROLE FMG_ENGINEER;
GRANT ALL PRIVILEGES ON ALL TABLES IN DATABASE FMG_ANALYTICS TO ROLE FMG_ENGINEER;

-- Future grants for engineer
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE FMG_PRODUCTION TO ROLE FMG_ENGINEER;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN DATABASE FMG_PRODUCTION TO ROLE FMG_ENGINEER;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN DATABASE FMG_DEVELOPMENT TO ROLE FMG_ENGINEER;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN DATABASE FMG_ANALYTICS TO ROLE FMG_ENGINEER;

-- FMG_ANALYST: Read access to production and analytics
GRANT USAGE ON DATABASE FMG_PRODUCTION TO ROLE FMG_ANALYST;
GRANT USAGE ON DATABASE FMG_ANALYTICS TO ROLE FMG_ANALYST;

GRANT USAGE ON ALL SCHEMAS IN DATABASE FMG_PRODUCTION TO ROLE FMG_ANALYST;
GRANT USAGE ON ALL SCHEMAS IN DATABASE FMG_ANALYTICS TO ROLE FMG_ANALYST;

GRANT SELECT ON ALL TABLES IN DATABASE FMG_PRODUCTION TO ROLE FMG_ANALYST;
GRANT SELECT ON ALL TABLES IN DATABASE FMG_ANALYTICS TO ROLE FMG_ANALYST;
GRANT SELECT ON ALL VIEWS IN DATABASE FMG_PRODUCTION TO ROLE FMG_ANALYST;
GRANT SELECT ON ALL VIEWS IN DATABASE FMG_ANALYTICS TO ROLE FMG_ANALYST;

-- Future grants for analyst
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE FMG_PRODUCTION TO ROLE FMG_ANALYST;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE FMG_ANALYTICS TO ROLE FMG_ANALYST;
GRANT SELECT ON FUTURE TABLES IN DATABASE FMG_PRODUCTION TO ROLE FMG_ANALYST;
GRANT SELECT ON FUTURE TABLES IN DATABASE FMG_ANALYTICS TO ROLE FMG_ANALYST;
GRANT SELECT ON FUTURE VIEWS IN DATABASE FMG_PRODUCTION TO ROLE FMG_ANALYST;
GRANT SELECT ON FUTURE VIEWS IN DATABASE FMG_ANALYTICS TO ROLE FMG_ANALYST;

-- FMG_COMPLIANCE_OFFICER: Access to compliance and audit data
GRANT USAGE ON DATABASE FMG_PRODUCTION TO ROLE FMG_COMPLIANCE_OFFICER;
GRANT USAGE ON SCHEMA FMG_PRODUCTION.COMPLIANCE TO ROLE FMG_COMPLIANCE_OFFICER;
GRANT USAGE ON SCHEMA FMG_PRODUCTION.RAW TO ROLE FMG_COMPLIANCE_OFFICER;
GRANT SELECT ON ALL TABLES IN SCHEMA FMG_PRODUCTION.COMPLIANCE TO ROLE FMG_COMPLIANCE_OFFICER;
GRANT SELECT ON ALL TABLES IN SCHEMA FMG_PRODUCTION.RAW TO ROLE FMG_COMPLIANCE_OFFICER;

-- FMG_DATA_SCIENTIST: Analytics and development access for ML
GRANT USAGE ON DATABASE FMG_ANALYTICS TO ROLE FMG_DATA_SCIENTIST;
GRANT USAGE ON DATABASE FMG_DEVELOPMENT TO ROLE FMG_DATA_SCIENTIST;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE FMG_ANALYTICS TO ROLE FMG_DATA_SCIENTIST;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE FMG_DEVELOPMENT TO ROLE FMG_DATA_SCIENTIST;
GRANT ALL PRIVILEGES ON ALL TABLES IN DATABASE FMG_ANALYTICS TO ROLE FMG_DATA_SCIENTIST;
GRANT ALL PRIVILEGES ON ALL TABLES IN DATABASE FMG_DEVELOPMENT TO ROLE FMG_DATA_SCIENTIST;

-- ============================================================================
-- SECTION 8: CREATE SERVICE ACCOUNT ROLES
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

-- Grant appropriate permissions
GRANT USAGE ON DATABASE FMG_PRODUCTION TO ROLE FMG_SVC_ETL;
GRANT USAGE ON ALL SCHEMAS IN DATABASE FMG_PRODUCTION TO ROLE FMG_SVC_ETL;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA FMG_PRODUCTION.RAW TO ROLE FMG_SVC_ETL;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA FMG_PRODUCTION.STAGING TO ROLE FMG_SVC_ETL;

GRANT USAGE ON DATABASE FMG_ANALYTICS TO ROLE FMG_SVC_BI;
GRANT USAGE ON ALL SCHEMAS IN DATABASE FMG_ANALYTICS TO ROLE FMG_SVC_BI;
GRANT SELECT ON ALL TABLES IN DATABASE FMG_ANALYTICS TO ROLE FMG_SVC_BI;
GRANT SELECT ON ALL VIEWS IN DATABASE FMG_ANALYTICS TO ROLE FMG_SVC_BI;

-- ============================================================================
-- SECTION 9: VERIFY ROLE CONFIGURATION
-- ============================================================================

-- View all FMG roles
SHOW ROLES LIKE 'FMG%';

-- Check grants on a specific role
SHOW GRANTS TO ROLE FMG_ANALYST;
SHOW GRANTS TO ROLE FMG_ENGINEER;

-- Check what roles a user has
SHOW GRANTS TO USER FMG_DEMO_ANALYST;

-- Check role hierarchy (what roles does FMG_ADMIN inherit?)
SHOW GRANTS TO ROLE FMG_ADMIN;

-- ============================================================================
-- SECTION 10: TEST ROLE ACCESS (Interactive)
-- ============================================================================

-- Switch to FMG_ANALYST role and verify access
USE ROLE FMG_ANALYST;
USE WAREHOUSE FMG_DEV_XS;

-- Should work: Read from production tables
SELECT COUNT(*) FROM FMG_PRODUCTION.RAW.CUSTOMERS;

-- Should work: Read from analytics
-- SELECT COUNT(*) FROM FMG_ANALYTICS.MARKETING.SOME_TABLE;  -- Uncomment after creating

-- Should fail: Write to production (analysts are read-only)
-- INSERT INTO FMG_PRODUCTION.RAW.CUSTOMERS (customer_id) VALUES ('TEST');  -- This will fail

-- Switch back to admin for rest of lab
USE ROLE ACCOUNTADMIN;

-- ============================================================================
-- SCRIPT COMPLETE!
-- ============================================================================

SELECT '✅ User and Role Setup Complete!' AS STATUS,
       (SELECT COUNT(*) FROM TABLE(RESULT_SCAN(LAST_QUERY_ID(-2)))) AS ROLES_CREATED,
       CURRENT_TIMESTAMP() AS COMPLETED_AT;

