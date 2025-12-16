/*=============================================================================
  FMG SUITE - SNOWFLAKE HANDS-ON LABS
  Cleanup Script - Remove All FMG Objects
  
  Description: Drops all FMG databases, warehouses, roles, users, and monitors
               Run this to start fresh or clean up after the labs
  Prerequisites: ACCOUNTADMIN role access
=============================================================================*/

USE ROLE ACCOUNTADMIN;

-- ============================================================================
-- Drop Shares
-- ============================================================================
DROP SHARE IF EXISTS FMG_ANALYTICS_SHARE;

-- ============================================================================
-- Drop Resource Monitors
-- ============================================================================
DROP RESOURCE MONITOR IF EXISTS FMG_ACCOUNT_MONITOR;
DROP RESOURCE MONITOR IF EXISTS FMG_DEV_MONITOR;

-- ============================================================================
-- Drop Databases (this also drops all schemas, tables, views, etc.)
-- ============================================================================
DROP DATABASE IF EXISTS FMG_PRODUCTION;
DROP DATABASE IF EXISTS FMG_DEVELOPMENT;
DROP DATABASE IF EXISTS FMG_ANALYTICS;
DROP DATABASE IF EXISTS FMG_PRODUCTION_DEV_CLONE;

-- ============================================================================
-- Drop Warehouses
-- ============================================================================
DROP WAREHOUSE IF EXISTS FMG_DEV_XS;
DROP WAREHOUSE IF EXISTS FMG_PROD_S;
DROP WAREHOUSE IF EXISTS FMG_ANALYTICS_M;
DROP WAREHOUSE IF EXISTS FMG_ML_L;
DROP WAREHOUSE IF EXISTS FMG_LOAD_M;

-- ============================================================================
-- Drop Users
-- ============================================================================
DROP USER IF EXISTS FMG_DEMO_ENGINEER;
DROP USER IF EXISTS FMG_DEMO_ANALYST;
DROP USER IF EXISTS FMG_DEMO_COMPLIANCE;
DROP USER IF EXISTS FMG_DEMO_DS;
DROP USER IF EXISTS FMG_DEMO_EXEC;

-- ============================================================================
-- Drop Roles (drop child roles first due to hierarchy)
-- ============================================================================
DROP ROLE IF EXISTS FMG_VIEWER;
DROP ROLE IF EXISTS FMG_ANALYST;
DROP ROLE IF EXISTS FMG_ENGINEER;
DROP ROLE IF EXISTS FMG_COMPLIANCE_OFFICER;
DROP ROLE IF EXISTS FMG_DATA_SCIENTIST;
DROP ROLE IF EXISTS FMG_CS_OPS;
DROP ROLE IF EXISTS FMG_SVC_ETL;
DROP ROLE IF EXISTS FMG_SVC_BI;
DROP ROLE IF EXISTS FMG_ADMIN;

-- ============================================================================
-- Verify Cleanup
-- ============================================================================
SHOW DATABASES LIKE 'FMG%';
SHOW WAREHOUSES LIKE 'FMG%';
SHOW ROLES LIKE 'FMG%';

SELECT '✅ FMG Environment Cleaned - Ready to start fresh!' AS STATUS,
       CURRENT_TIMESTAMP() AS COMPLETED_AT;

