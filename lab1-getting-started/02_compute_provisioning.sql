/*=============================================================================
  FMG SUITE - LAB 1: GETTING STARTED WITH SNOWFLAKE
  Script 2: Compute Provisioning
  
  Description: Configure virtual warehouses for different FMG workloads
  Prerequisites: ACCOUNTADMIN or SYSADMIN access
  Duration: ~10 minutes
=============================================================================*/

-- ============================================================================
-- SECTION 1: SET CONTEXT
-- ============================================================================

USE ROLE SYSADMIN;  -- SYSADMIN can create warehouses

-- ============================================================================
-- SECTION 2: UNDERSTAND WAREHOUSE SIZING
-- ============================================================================

/*
    Snowflake Warehouse Sizes and Credits per Hour:
    
    Size        │ Credits/Hour │ Servers │ Best For
    ────────────┼──────────────┼─────────┼─────────────────────────────
    X-Small     │ 1            │ 1       │ Development, simple queries
    Small       │ 2            │ 2       │ Light production workloads
    Medium      │ 4            │ 4       │ Standard analytics, BI
    Large       │ 8            │ 8       │ Complex queries, ML
    X-Large     │ 16           │ 16      │ Heavy ETL, large datasets
    2X-Large    │ 32           │ 32      │ Very large scale processing
    ...         │              │         │
    6X-Large    │ 512          │ 512     │ Maximum scale
    
    Key Insight: Larger warehouses run faster but cost more per hour.
    For long-running queries, a larger warehouse may be MORE cost-effective!
*/

-- ============================================================================
-- SECTION 3: CREATE FMG WAREHOUSES
-- ============================================================================

-- Development Warehouse: For developers and ad-hoc queries
CREATE WAREHOUSE IF NOT EXISTS FMG_DEV_XS
    WAREHOUSE_SIZE = 'X-SMALL'
    WAREHOUSE_TYPE = 'STANDARD'
    AUTO_SUSPEND = 60           -- Suspend after 1 minute idle
    AUTO_RESUME = TRUE          -- Auto-start when query arrives
    INITIALLY_SUSPENDED = TRUE  -- Don't start now
    MIN_CLUSTER_COUNT = 1       -- Single cluster
    MAX_CLUSTER_COUNT = 1
    SCALING_POLICY = 'STANDARD'
    COMMENT = 'Development warehouse for FMG team - auto-suspends quickly';

-- Production Warehouse: For dashboards and standard reporting
CREATE WAREHOUSE IF NOT EXISTS FMG_PROD_S
    WAREHOUSE_SIZE = 'SMALL'
    WAREHOUSE_TYPE = 'STANDARD'
    AUTO_SUSPEND = 120          -- 2 minute suspend
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 2       -- Can scale to 2 clusters for concurrent users
    SCALING_POLICY = 'STANDARD'
    COMMENT = 'Production warehouse for FMG dashboards and reporting';

-- Analytics Warehouse: For BI tools, heavy transformations
CREATE WAREHOUSE IF NOT EXISTS FMG_ANALYTICS_M
    WAREHOUSE_SIZE = 'MEDIUM'
    WAREHOUSE_TYPE = 'STANDARD'
    AUTO_SUSPEND = 180          -- 3 minute suspend
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 3       -- Scale up for peak BI usage
    SCALING_POLICY = 'STANDARD'
    COMMENT = 'Analytics warehouse for BI tools and transformations';

-- ML/AI Warehouse: For Cortex and data science workloads
CREATE WAREHOUSE IF NOT EXISTS FMG_ML_L
    WAREHOUSE_SIZE = 'LARGE'
    WAREHOUSE_TYPE = 'STANDARD'
    AUTO_SUSPEND = 300          -- 5 minute suspend (ML jobs may have pauses)
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 1       -- ML typically single-threaded
    SCALING_POLICY = 'STANDARD'
    COMMENT = 'ML/AI warehouse for Cortex and data science workloads';

-- Data Loading Warehouse: For ETL and batch loading
CREATE WAREHOUSE IF NOT EXISTS FMG_LOAD_M
    WAREHOUSE_SIZE = 'MEDIUM'
    WAREHOUSE_TYPE = 'STANDARD'
    AUTO_SUSPEND = 120
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 1       -- Loading is typically sequential
    SCALING_POLICY = 'STANDARD'
    COMMENT = 'Data loading warehouse for ETL pipelines';

-- ============================================================================
-- SECTION 4: QUERY ACCELERATION (Snowflake Enterprise+)
-- ============================================================================

/*
    Query Acceleration offloads portions of query processing to shared compute
    resources. Great for:
    - Queries with large scans and selective filters
    - Spiky workloads with occasional complex queries
    - Dashboards with varying query complexity
    
    It's billed separately from warehouse compute.
*/

-- Enable Query Acceleration on analytics warehouse
ALTER WAREHOUSE FMG_ANALYTICS_M SET
    ENABLE_QUERY_ACCELERATION = TRUE
    QUERY_ACCELERATION_MAX_SCALE_FACTOR = 4;  -- Max 4x additional compute

-- Check if acceleration is beneficial for recent queries
-- (Run after using the warehouse)
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.QUERY_ACCELERATION_ELIGIBLE('FMG_ANALYTICS_M'));

-- ============================================================================
-- SECTION 5: GRANT WAREHOUSE ACCESS TO ROLES
-- ============================================================================

-- FMG_VIEWER: Only dev warehouse (for minimal access)
GRANT USAGE ON WAREHOUSE FMG_DEV_XS TO ROLE FMG_VIEWER;

-- FMG_ANALYST: Dev and production warehouses
GRANT USAGE ON WAREHOUSE FMG_DEV_XS TO ROLE FMG_ANALYST;
GRANT USAGE ON WAREHOUSE FMG_PROD_S TO ROLE FMG_ANALYST;
GRANT USAGE ON WAREHOUSE FMG_ANALYTICS_M TO ROLE FMG_ANALYST;

-- FMG_ENGINEER: All except ML warehouse
GRANT USAGE ON WAREHOUSE FMG_DEV_XS TO ROLE FMG_ENGINEER;
GRANT USAGE ON WAREHOUSE FMG_PROD_S TO ROLE FMG_ENGINEER;
GRANT USAGE ON WAREHOUSE FMG_ANALYTICS_M TO ROLE FMG_ENGINEER;
GRANT USAGE ON WAREHOUSE FMG_LOAD_M TO ROLE FMG_ENGINEER;
GRANT OPERATE ON WAREHOUSE FMG_LOAD_M TO ROLE FMG_ENGINEER;  -- Can suspend/resume

-- FMG_COMPLIANCE_OFFICER: Dev and production
GRANT USAGE ON WAREHOUSE FMG_DEV_XS TO ROLE FMG_COMPLIANCE_OFFICER;
GRANT USAGE ON WAREHOUSE FMG_PROD_S TO ROLE FMG_COMPLIANCE_OFFICER;

-- FMG_DATA_SCIENTIST: Dev, analytics, and ML warehouses
GRANT USAGE ON WAREHOUSE FMG_DEV_XS TO ROLE FMG_DATA_SCIENTIST;
GRANT USAGE ON WAREHOUSE FMG_ANALYTICS_M TO ROLE FMG_DATA_SCIENTIST;
GRANT USAGE ON WAREHOUSE FMG_ML_L TO ROLE FMG_DATA_SCIENTIST;
GRANT OPERATE ON WAREHOUSE FMG_ML_L TO ROLE FMG_DATA_SCIENTIST;

-- FMG_ADMIN: Full access to all warehouses
GRANT ALL PRIVILEGES ON WAREHOUSE FMG_DEV_XS TO ROLE FMG_ADMIN;
GRANT ALL PRIVILEGES ON WAREHOUSE FMG_PROD_S TO ROLE FMG_ADMIN;
GRANT ALL PRIVILEGES ON WAREHOUSE FMG_ANALYTICS_M TO ROLE FMG_ADMIN;
GRANT ALL PRIVILEGES ON WAREHOUSE FMG_ML_L TO ROLE FMG_ADMIN;
GRANT ALL PRIVILEGES ON WAREHOUSE FMG_LOAD_M TO ROLE FMG_ADMIN;

-- Service accounts
GRANT USAGE ON WAREHOUSE FMG_LOAD_M TO ROLE FMG_SVC_ETL;
GRANT USAGE ON WAREHOUSE FMG_ANALYTICS_M TO ROLE FMG_SVC_BI;

-- ============================================================================
-- SECTION 6: WAREHOUSE PARAMETER TUNING
-- ============================================================================

/*
    Key parameters to consider:
    
    STATEMENT_TIMEOUT_IN_SECONDS - Max query runtime
    STATEMENT_QUEUED_TIMEOUT_IN_SECONDS - Max time in queue
    
    These prevent runaway queries from consuming resources.
*/

-- Set reasonable timeouts on production warehouse
ALTER WAREHOUSE FMG_PROD_S SET 
    STATEMENT_TIMEOUT_IN_SECONDS = 1800;  -- 30 minutes max query time

-- Analytics can run longer queries
ALTER WAREHOUSE FMG_ANALYTICS_M SET 
    STATEMENT_TIMEOUT_IN_SECONDS = 7200;  -- 2 hours max

-- ML warehouse needs even longer for training jobs
ALTER WAREHOUSE FMG_ML_L SET 
    STATEMENT_TIMEOUT_IN_SECONDS = 14400;  -- 4 hours max

-- ============================================================================
-- SECTION 7: VERIFY WAREHOUSE CONFIGURATION
-- ============================================================================

-- Show all FMG warehouses
SHOW WAREHOUSES LIKE 'FMG%';

-- Get detailed information
SELECT 
    name,
    size,
    min_cluster_count,
    max_cluster_count,
    auto_suspend,
    auto_resume,
    enable_query_acceleration,
    query_acceleration_max_scale_factor,
    comment
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- Check warehouse state
SELECT 
    name,
    state,
    running,
    queued,
    is_current
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID(-2)));

-- ============================================================================
-- SECTION 8: TEST WAREHOUSE FUNCTIONALITY
-- ============================================================================

-- Start the dev warehouse and run a test query
USE WAREHOUSE FMG_DEV_XS;

-- Simple test query
SELECT 
    'FMG Warehouse Test' AS test_name,
    CURRENT_WAREHOUSE() AS warehouse,
    CURRENT_TIMESTAMP() AS executed_at;

-- Check that the warehouse is now running
SHOW WAREHOUSES LIKE 'FMG_DEV_XS';

-- View recent warehouse usage (requires some history)
-- SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
-- WHERE WAREHOUSE_NAME LIKE 'FMG%'
-- AND START_TIME > DATEADD('day', -7, CURRENT_TIMESTAMP())
-- ORDER BY START_TIME DESC
-- LIMIT 20;

-- ============================================================================
-- SECTION 9: BEST PRACTICES SUMMARY
-- ============================================================================

/*
    FMG Warehouse Strategy:
    
    1. SIZE FOR THE WORKLOAD
       - Dev/ad-hoc: X-Small (cheap, fast spin-up)
       - Production: Small (balanced cost/performance)
       - Analytics: Medium (handles complex joins)
       - ML/AI: Large (Cortex needs compute power)
    
    2. AUTO-SUSPEND AGGRESSIVELY
       - Dev: 60 seconds (nobody notices)
       - Production: 120 seconds (dashboards may have pauses)
       - Analytics: 180 seconds (complex work may pause briefly)
    
    3. USE MULTI-CLUSTER FOR CONCURRENCY
       - Not for faster queries!
       - For handling more simultaneous users
       - Scaling policy: STANDARD (scale out gradually)
    
    4. QUERY ACCELERATION FOR SPIKY WORKLOADS
       - Enable on dashboards with varied complexity
       - Set reasonable scale factor (2-8x)
    
    5. SET TIMEOUTS
       - Prevent runaway queries from burning credits
       - Different limits for different use cases
    
    6. SEPARATE BY WORKLOAD TYPE
       - Don't share ETL and BI on same warehouse
       - Allows independent scaling and billing tracking
*/

-- ============================================================================
-- SCRIPT COMPLETE!
-- ============================================================================

SELECT '✅ Compute Provisioning Complete!' AS STATUS,
       (SELECT COUNT(*) FROM TABLE(RESULT_SCAN(LAST_QUERY_ID(-5))) WHERE NAME LIKE 'FMG%') AS WAREHOUSES_CONFIGURED,
       CURRENT_TIMESTAMP() AS COMPLETED_AT;

