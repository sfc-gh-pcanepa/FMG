/*=============================================================================
  FMG SUITE - SNOWFLAKE HANDS-ON LABS
  Environment Setup Script
  
  Description: Initial environment configuration for all FMG labs
  Prerequisites: ACCOUNTADMIN role access
  Duration: ~5 minutes
=============================================================================*/

-- ============================================================================
-- STEP 1: Set Context
-- ============================================================================
USE ROLE ACCOUNTADMIN;

-- ============================================================================
-- STEP 2: Create the FMG Databases
-- ============================================================================

-- Main production database for FMG data
CREATE DATABASE IF NOT EXISTS FMG_PRODUCTION
    COMMENT = 'FMG Suite production data - marketing campaigns, advisors, compliance';

-- Development/sandbox database for experimentation
CREATE DATABASE IF NOT EXISTS FMG_DEVELOPMENT
    COMMENT = 'FMG Suite development and testing environment';

-- Analytics database for transformed/aggregated data
CREATE DATABASE IF NOT EXISTS FMG_ANALYTICS
    COMMENT = 'FMG Suite analytics and reporting data';

-- ============================================================================
-- STEP 3: Create Schemas
-- ============================================================================

-- Production schemas
CREATE SCHEMA IF NOT EXISTS FMG_PRODUCTION.RAW
    COMMENT = 'Raw ingested data from source systems';

CREATE SCHEMA IF NOT EXISTS FMG_PRODUCTION.STAGING
    COMMENT = 'Staging area for data transformations';

CREATE SCHEMA IF NOT EXISTS FMG_PRODUCTION.CURATED
    COMMENT = 'Curated, business-ready data';

CREATE SCHEMA IF NOT EXISTS FMG_PRODUCTION.COMPLIANCE
    COMMENT = 'Compliance and audit data for financial regulations';

-- Analytics schemas
CREATE SCHEMA IF NOT EXISTS FMG_ANALYTICS.MARKETING
    COMMENT = 'Marketing analytics and campaign performance';

CREATE SCHEMA IF NOT EXISTS FMG_ANALYTICS.ADVISOR_360
    COMMENT = 'Complete advisor view and engagement metrics';

CREATE SCHEMA IF NOT EXISTS FMG_ANALYTICS.EXECUTIVE
    COMMENT = 'Executive dashboards and KPIs';

-- Development schemas
CREATE SCHEMA IF NOT EXISTS FMG_DEVELOPMENT.SANDBOX
    COMMENT = 'General sandbox for experimentation';

CREATE SCHEMA IF NOT EXISTS FMG_DEVELOPMENT.LAB_WORKSPACE
    COMMENT = 'Workspace for hands-on lab exercises';

-- ============================================================================
-- STEP 4: Create Warehouses
-- ============================================================================

-- Extra-small warehouse for lightweight queries and development
CREATE WAREHOUSE IF NOT EXISTS FMG_DEV_XS
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Development warehouse - auto-suspends after 1 minute';

-- Small warehouse for production queries
CREATE WAREHOUSE IF NOT EXISTS FMG_PROD_S
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 120
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Production warehouse for standard workloads';

-- Medium warehouse for analytics and transformations
CREATE WAREHOUSE IF NOT EXISTS FMG_ANALYTICS_M
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 180
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Analytics warehouse for heavier transformations';

-- Large warehouse for ML/AI workloads (used in Lab 4)
CREATE WAREHOUSE IF NOT EXISTS FMG_ML_L
    WAREHOUSE_SIZE = 'LARGE'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'ML/AI workloads including Cortex features';

-- ============================================================================
-- STEP 5: Create Custom Roles (Role Hierarchy)
-- ============================================================================

/*
    FMG Role Hierarchy:
    
    ACCOUNTADMIN
         │
         ├── FMG_ADMIN (Full FMG access)
         │       │
         │       ├── FMG_ANALYST (Read analytics, run queries)
         │       │       │
         │       │       └── FMG_VIEWER (Read-only access)
         │       │
         │       ├── FMG_ENGINEER (ETL, transformations)
         │       │
         │       └── FMG_COMPLIANCE_OFFICER (Audit access, PII viewing)
         │
         └── FMG_DATA_SCIENTIST (ML/AI workloads)
*/

CREATE ROLE IF NOT EXISTS FMG_ADMIN
    COMMENT = 'FMG administrative role with full database access';

CREATE ROLE IF NOT EXISTS FMG_ANALYST
    COMMENT = 'FMG analyst role for reporting and analytics';

CREATE ROLE IF NOT EXISTS FMG_VIEWER
    COMMENT = 'FMG read-only viewer role';

CREATE ROLE IF NOT EXISTS FMG_ENGINEER
    COMMENT = 'FMG data engineer role for ETL and transformations';

CREATE ROLE IF NOT EXISTS FMG_COMPLIANCE_OFFICER
    COMMENT = 'FMG compliance officer with audit and PII access';

CREATE ROLE IF NOT EXISTS FMG_DATA_SCIENTIST
    COMMENT = 'FMG data scientist role for ML/AI workloads';

-- ============================================================================
-- STEP 6: Establish Role Hierarchy
-- ============================================================================

-- Grant child roles to parent roles
GRANT ROLE FMG_VIEWER TO ROLE FMG_ANALYST;
GRANT ROLE FMG_ANALYST TO ROLE FMG_ADMIN;
GRANT ROLE FMG_ENGINEER TO ROLE FMG_ADMIN;
GRANT ROLE FMG_COMPLIANCE_OFFICER TO ROLE FMG_ADMIN;
GRANT ROLE FMG_DATA_SCIENTIST TO ROLE FMG_ADMIN;

-- Grant FMG_ADMIN to SYSADMIN (best practice)
GRANT ROLE FMG_ADMIN TO ROLE SYSADMIN;

-- ============================================================================
-- STEP 7: Grant Database Access
-- ============================================================================

-- FMG_ADMIN gets full access to all FMG databases and schemas
GRANT ALL PRIVILEGES ON DATABASE FMG_PRODUCTION TO ROLE FMG_ADMIN;
GRANT ALL PRIVILEGES ON DATABASE FMG_DEVELOPMENT TO ROLE FMG_ADMIN;
GRANT ALL PRIVILEGES ON DATABASE FMG_ANALYTICS TO ROLE FMG_ADMIN;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE FMG_PRODUCTION TO ROLE FMG_ADMIN;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE FMG_DEVELOPMENT TO ROLE FMG_ADMIN;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE FMG_ANALYTICS TO ROLE FMG_ADMIN;

-- FMG_ANALYST gets read-ONLY access to production and analytics
GRANT USAGE ON DATABASE FMG_PRODUCTION TO ROLE FMG_ANALYST;
GRANT USAGE ON DATABASE FMG_ANALYTICS TO ROLE FMG_ANALYST;
GRANT USAGE ON ALL SCHEMAS IN DATABASE FMG_PRODUCTION TO ROLE FMG_ANALYST;
GRANT USAGE ON ALL SCHEMAS IN DATABASE FMG_ANALYTICS TO ROLE FMG_ANALYST;
GRANT SELECT ON ALL TABLES IN DATABASE FMG_PRODUCTION TO ROLE FMG_ANALYST;
GRANT SELECT ON ALL TABLES IN DATABASE FMG_ANALYTICS TO ROLE FMG_ANALYST;

-- Explicitly REVOKE write permissions from FMG_ANALYST (in case of inheritance issues)
REVOKE INSERT, UPDATE, DELETE ON ALL TABLES IN DATABASE FMG_PRODUCTION FROM ROLE FMG_ANALYST;
REVOKE INSERT, UPDATE, DELETE ON ALL TABLES IN DATABASE FMG_ANALYTICS FROM ROLE FMG_ANALYST;

-- FMG_ENGINEER gets access to development and production
GRANT USAGE ON DATABASE FMG_PRODUCTION TO ROLE FMG_ENGINEER;
GRANT USAGE ON DATABASE FMG_DEVELOPMENT TO ROLE FMG_ENGINEER;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE FMG_DEVELOPMENT TO ROLE FMG_ENGINEER;
GRANT USAGE ON ALL SCHEMAS IN DATABASE FMG_PRODUCTION TO ROLE FMG_ENGINEER;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN DATABASE FMG_PRODUCTION TO ROLE FMG_ENGINEER;

-- FMG_COMPLIANCE_OFFICER gets access to compliance schema
GRANT USAGE ON DATABASE FMG_PRODUCTION TO ROLE FMG_COMPLIANCE_OFFICER;
GRANT USAGE ON SCHEMA FMG_PRODUCTION.COMPLIANCE TO ROLE FMG_COMPLIANCE_OFFICER;
GRANT SELECT ON ALL TABLES IN SCHEMA FMG_PRODUCTION.COMPLIANCE TO ROLE FMG_COMPLIANCE_OFFICER;

-- FMG_DATA_SCIENTIST gets analytics access
GRANT USAGE ON DATABASE FMG_ANALYTICS TO ROLE FMG_DATA_SCIENTIST;
GRANT USAGE ON DATABASE FMG_DEVELOPMENT TO ROLE FMG_DATA_SCIENTIST;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE FMG_ANALYTICS TO ROLE FMG_DATA_SCIENTIST;

-- ============================================================================
-- STEP 8: Grant Warehouse Access
-- ============================================================================

-- Development warehouse for analysts and engineers
GRANT USAGE ON WAREHOUSE FMG_DEV_XS TO ROLE FMG_ANALYST;
GRANT USAGE ON WAREHOUSE FMG_DEV_XS TO ROLE FMG_ENGINEER;
GRANT USAGE ON WAREHOUSE FMG_DEV_XS TO ROLE FMG_VIEWER;

-- Production warehouse for analysts
GRANT USAGE ON WAREHOUSE FMG_PROD_S TO ROLE FMG_ANALYST;
GRANT USAGE ON WAREHOUSE FMG_PROD_S TO ROLE FMG_COMPLIANCE_OFFICER;

-- Analytics warehouse for analysts and engineers
GRANT USAGE ON WAREHOUSE FMG_ANALYTICS_M TO ROLE FMG_ANALYST;
GRANT USAGE ON WAREHOUSE FMG_ANALYTICS_M TO ROLE FMG_ENGINEER;

-- ML warehouse for data scientists
GRANT USAGE ON WAREHOUSE FMG_ML_L TO ROLE FMG_DATA_SCIENTIST;
GRANT USAGE ON WAREHOUSE FMG_ML_L TO ROLE FMG_ADMIN;

-- ============================================================================
-- STEP 9: Set Future Grants (for objects created later)
-- ============================================================================

-- Future grants on FMG_PRODUCTION
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE FMG_PRODUCTION TO ROLE FMG_ANALYST;
GRANT SELECT ON FUTURE TABLES IN DATABASE FMG_PRODUCTION TO ROLE FMG_ANALYST;
GRANT SELECT ON FUTURE VIEWS IN DATABASE FMG_PRODUCTION TO ROLE FMG_ANALYST;

-- Future grants on FMG_ANALYTICS
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE FMG_ANALYTICS TO ROLE FMG_ANALYST;
GRANT SELECT ON FUTURE TABLES IN DATABASE FMG_ANALYTICS TO ROLE FMG_ANALYST;
GRANT SELECT ON FUTURE VIEWS IN DATABASE FMG_ANALYTICS TO ROLE FMG_ANALYST;

-- Future grants on FMG_DEVELOPMENT
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE FMG_DEVELOPMENT TO ROLE FMG_ENGINEER;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN DATABASE FMG_DEVELOPMENT TO ROLE FMG_ENGINEER;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN DATABASE FMG_DEVELOPMENT TO ROLE FMG_ENGINEER;

-- ============================================================================
-- STEP 10: Verify Setup
-- ============================================================================

-- Show created objects
SHOW DATABASES LIKE 'FMG%';
SHOW WAREHOUSES LIKE 'FMG%';
SHOW ROLES LIKE 'FMG%';

-- Verify role grants
SHOW GRANTS TO ROLE FMG_ANALYST;

-- ============================================================================
-- STEP 11: Re-apply Grants After Data Setup (RUN THIS AFTER 01_synthetic_data_setup.sql)
-- ============================================================================
-- If you've already run the synthetic data setup, run this section to ensure
-- proper permissions on the newly created tables:

-- Re-grant SELECT to analyst (for tables created after initial setup)
GRANT SELECT ON ALL TABLES IN DATABASE FMG_PRODUCTION TO ROLE FMG_ANALYST;
GRANT SELECT ON ALL TABLES IN DATABASE FMG_ANALYTICS TO ROLE FMG_ANALYST;

-- Explicitly revoke write permissions
REVOKE INSERT, UPDATE, DELETE ON ALL TABLES IN DATABASE FMG_PRODUCTION FROM ROLE FMG_ANALYST;
REVOKE INSERT, UPDATE, DELETE ON ALL TABLES IN DATABASE FMG_ANALYTICS FROM ROLE FMG_ANALYST;

-- Transfer table ownership to SYSADMIN (so user's personal ownership doesn't grant access)
GRANT OWNERSHIP ON ALL TABLES IN SCHEMA FMG_PRODUCTION.RAW TO ROLE SYSADMIN COPY CURRENT GRANTS;

-- ============================================================================
-- STEP 12: Verify Permissions (Diagnostic)
-- ============================================================================

-- Check what grants FMG_ANALYST has on a specific table
SHOW GRANTS ON TABLE FMG_PRODUCTION.RAW.CUSTOMERS;

-- Check what roles your current user has (replace YOUR_USERNAME)
-- SHOW GRANTS TO USER YOUR_USERNAME;
SELECT CURRENT_USER() AS YOUR_USERNAME;

-- ============================================================================
-- SETUP COMPLETE!
-- Next Step: Run 01_synthetic_data_setup.sql to generate sample data
--            Then re-run STEP 11 above to lock down permissions
-- ============================================================================

SELECT '✅ FMG Environment Setup Complete!' AS STATUS,
       CURRENT_TIMESTAMP() AS COMPLETED_AT;

