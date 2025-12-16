/*=============================================================================
  FMG SUITE - LAB 1: GETTING STARTED WITH SNOWFLAKE
  Script 2: Advanced Compute Configuration
  
  Description: Configure advanced warehouse features (multi-cluster, acceleration, timeouts)
  Prerequisites: Run setup/00_environment_setup.sql first
=============================================================================*/

-- ============================================================================
-- SECTION 1: SET CONTEXT
-- ============================================================================

USE ROLE SYSADMIN;

-- Verify warehouses were created
SHOW WAREHOUSES LIKE 'FMG%';
-- You should see: FMG_DEV_XS, FMG_PROD_S, FMG_ANALYTICS_M, FMG_ML_L

-- ============================================================================
-- SECTION 2: CREATE DATA LOADING WAREHOUSE
-- ============================================================================

-- ETL/Loading Warehouse: For batch data loading
CREATE WAREHOUSE IF NOT EXISTS FMG_LOAD_M
    WAREHOUSE_SIZE = 'MEDIUM'
    WAREHOUSE_TYPE = 'STANDARD'
    AUTO_SUSPEND = 120
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 1
    COMMENT = 'Data loading warehouse for ETL pipelines';

-- Grant access
GRANT USAGE ON WAREHOUSE FMG_LOAD_M TO ROLE FMG_ENGINEER;
GRANT OPERATE ON WAREHOUSE FMG_LOAD_M TO ROLE FMG_ENGINEER;
GRANT ALL PRIVILEGES ON WAREHOUSE FMG_LOAD_M TO ROLE FMG_ADMIN;

-- ============================================================================
-- SECTION 3: CONFIGURE MULTI-CLUSTER SCALING
-- ============================================================================

/*
    Multi-cluster warehouses handle concurrency, NOT query speed.
    Use when many users query simultaneously (e.g., dashboards).
    
    SCALING_POLICY options:
    - STANDARD: Scale out gradually as queries queue
    - ECONOMY: Favor queuing over scaling (saves cost)
*/

-- Production: Scale to 2 clusters for concurrent dashboard users
ALTER WAREHOUSE FMG_PROD_S SET
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 2
    SCALING_POLICY = 'STANDARD';

-- Analytics: Scale to 3 clusters for peak BI usage
ALTER WAREHOUSE FMG_ANALYTICS_M SET
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 3
    SCALING_POLICY = 'STANDARD';

-- ============================================================================
-- SECTION 4: ENABLE QUERY ACCELERATION
-- ============================================================================

/*
    Query Acceleration offloads portions of queries to shared compute.
    Great for:
    - Large scans with selective filters
    - Spiky workloads with occasional complex queries
    - Dashboards with varying query complexity
    
    Billed separately from warehouse compute.
*/

ALTER WAREHOUSE FMG_ANALYTICS_M SET
    ENABLE_QUERY_ACCELERATION = TRUE
    QUERY_ACCELERATION_MAX_SCALE_FACTOR = 4;

-- Check acceleration eligibility (run after using the warehouse)
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.QUERY_ACCELERATION_ELIGIBLE('FMG_ANALYTICS_M'));

-- ============================================================================
-- SECTION 5: SET STATEMENT TIMEOUTS
-- ============================================================================

/*
    Prevent runaway queries from burning credits.
    Set different limits based on expected workload.
*/

-- Production: 30 minute max (dashboards should be fast)
ALTER WAREHOUSE FMG_PROD_S SET 
    STATEMENT_TIMEOUT_IN_SECONDS = 1800;

-- Analytics: 2 hour max (complex analysis)
ALTER WAREHOUSE FMG_ANALYTICS_M SET 
    STATEMENT_TIMEOUT_IN_SECONDS = 7200;

-- ML: 4 hour max (model training)
ALTER WAREHOUSE FMG_ML_L SET 
    STATEMENT_TIMEOUT_IN_SECONDS = 14400;

-- Loading: 1 hour max
ALTER WAREHOUSE FMG_LOAD_M SET 
    STATEMENT_TIMEOUT_IN_SECONDS = 3600;

-- ============================================================================
-- SECTION 6: ADDITIONAL ROLE GRANTS
-- ============================================================================

-- Grant OPERATE privilege (suspend/resume) to specific roles
GRANT OPERATE ON WAREHOUSE FMG_ML_L TO ROLE FMG_DATA_SCIENTIST;
GRANT OPERATE ON WAREHOUSE FMG_ANALYTICS_M TO ROLE FMG_ENGINEER;

-- ============================================================================
-- SECTION 7: VERIFY CONFIGURATION
-- ============================================================================

SHOW WAREHOUSES LIKE 'FMG%';

SELECT 
    name,
    size,
    min_cluster_count,
    max_cluster_count,
    auto_suspend,
    enable_query_acceleration,
    query_acceleration_max_scale_factor
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- ============================================================================
-- SECTION 8: TEST WAREHOUSE
-- ============================================================================

USE WAREHOUSE FMG_DEV_XS;

SELECT 
    'Warehouse Test' AS test_name,
    CURRENT_WAREHOUSE() AS warehouse,
    CURRENT_TIMESTAMP() AS executed_at;

-- ============================================================================
-- BEST PRACTICES SUMMARY
-- ============================================================================

/*
    FMG Warehouse Strategy:
    
    1. SIZE FOR THE WORKLOAD
       - Dev/ad-hoc: X-Small
       - Production: Small
       - Analytics/ETL: Medium
       - ML/AI: Large
    
    2. AUTO-SUSPEND AGGRESSIVELY
       - Dev: 60s | Prod: 120s | Analytics: 180s | ML: 300s
    
    3. MULTI-CLUSTER FOR CONCURRENCY (not speed)
       - Production: 1-2 clusters
       - Analytics: 1-3 clusters
    
    4. QUERY ACCELERATION FOR SPIKY WORKLOADS
       - Enable on analytics warehouse
       - Scale factor 2-8x
    
    5. SET TIMEOUTS TO PREVENT RUNAWAY QUERIES
*/

-- ============================================================================
-- COMPLETE!
-- ============================================================================

SELECT '✅ Advanced Compute Configuration Complete!' AS STATUS;
