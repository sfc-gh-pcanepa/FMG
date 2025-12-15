/*=============================================================================
  FMG SUITE - LAB 3: TRANSFORMATIONS IN SNOWFLAKE
  Script 4: Data Lineage
  
  Description: Track data flow and dependencies across the FMG data platform
  Prerequisites: Dynamic tables and views created
  Duration: ~10 minutes
=============================================================================*/

-- ============================================================================
-- SECTION 1: SET CONTEXT
-- ============================================================================

USE ROLE ACCOUNTADMIN;  -- Need for ACCOUNT_USAGE access
USE WAREHOUSE FMG_DEV_XS;

-- ============================================================================
-- SECTION 2: UNDERSTANDING DATA LINEAGE
-- ============================================================================

/*
    Snowflake tracks lineage automatically through:
    
    1. ACCESS_HISTORY - Records what objects were read/written
    2. OBJECT_DEPENDENCIES - Shows view/table dependencies
    3. Dynamic Table dependencies - Built-in for DT chains
    
    Lineage Types:
    - UPSTREAM: What data feeds into this object?
    - DOWNSTREAM: What objects depend on this data?
    
    Column-Level Lineage:
    - Tracks which source columns map to target columns
    - Essential for impact analysis
    - Available via ACCESS_HISTORY.base_objects_accessed
*/

-- ============================================================================
-- SECTION 3: QUERY ACCESS HISTORY FOR LINEAGE
-- ============================================================================

-- Recent data access patterns
SELECT 
    query_start_time,
    user_name,
    query_type,
    -- Objects that were directly accessed
    direct_objects_accessed,
    -- Base tables behind views
    base_objects_accessed,
    -- Objects that were modified
    objects_modified
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
WHERE query_start_time >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
AND (
    ARRAY_SIZE(direct_objects_accessed) > 0 
    OR ARRAY_SIZE(objects_modified) > 0
)
ORDER BY query_start_time DESC
LIMIT 50;

-- ============================================================================
-- SECTION 4: BUILD LINEAGE MAP FOR FMG TABLES
-- ============================================================================

-- Find all reads from FMG_PRODUCTION.RAW tables
SELECT 
    DATE_TRUNC('day', query_start_time) AS access_date,
    obj.value:objectName::STRING AS source_table,
    COUNT(*) AS read_count,
    COUNT(DISTINCT user_name) AS unique_users
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY,
    LATERAL FLATTEN(input => direct_objects_accessed) obj
WHERE query_start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
AND obj.value:objectName::STRING LIKE 'FMG_PRODUCTION.RAW.%'
GROUP BY 1, 2
ORDER BY access_date DESC, read_count DESC;

-- Find where FMG data flows to (downstream objects)
SELECT 
    src.value:objectName::STRING AS source_table,
    tgt.value:objectName::STRING AS target_table,
    COUNT(*) AS write_count,
    MAX(query_start_time) AS last_write
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY,
    LATERAL FLATTEN(input => base_objects_accessed) src,
    LATERAL FLATTEN(input => objects_modified) tgt
WHERE query_start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
AND src.value:objectName::STRING LIKE 'FMG_PRODUCTION.RAW.%'
AND tgt.value:objectName::STRING LIKE 'FMG%'
GROUP BY 1, 2
ORDER BY write_count DESC;

-- ============================================================================
-- SECTION 5: OBJECT DEPENDENCIES
-- ============================================================================

-- View dependencies (what tables do views reference?)
SELECT 
    referencing_object_name AS view_name,
    referencing_object_domain AS view_type,
    referenced_object_name AS depends_on_table,
    referenced_object_domain AS depends_on_type
FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
WHERE referencing_database = 'FMG_PRODUCTION'
OR referenced_database = 'FMG_PRODUCTION'
ORDER BY referencing_object_name;

-- Find all objects that depend on CUSTOMERS table
SELECT 
    referencing_database || '.' || referencing_schema || '.' || referencing_object_name AS dependent_object,
    referencing_object_domain AS object_type
FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
WHERE referenced_database = 'FMG_PRODUCTION'
AND referenced_schema = 'RAW'
AND referenced_object_name = 'CUSTOMERS';

-- ============================================================================
-- SECTION 6: COLUMN-LEVEL LINEAGE
-- ============================================================================

-- Track which columns were accessed
SELECT 
    query_start_time,
    user_name,
    obj.value:objectName::STRING AS table_name,
    col.value:columnName::STRING AS column_name
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY,
    LATERAL FLATTEN(input => base_objects_accessed) obj,
    LATERAL FLATTEN(input => obj.value:columns) col
WHERE query_start_time >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
AND obj.value:objectName::STRING LIKE 'FMG_PRODUCTION%'
ORDER BY query_start_time DESC
LIMIT 100;

-- Most accessed columns (for optimization insights)
SELECT 
    obj.value:objectName::STRING AS table_name,
    col.value:columnName::STRING AS column_name,
    COUNT(*) AS access_count
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY,
    LATERAL FLATTEN(input => base_objects_accessed) obj,
    LATERAL FLATTEN(input => obj.value:columns) col
WHERE query_start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
AND obj.value:objectName::STRING LIKE 'FMG_PRODUCTION.RAW.%'
GROUP BY 1, 2
ORDER BY access_count DESC
LIMIT 30;

-- ============================================================================
-- SECTION 7: CREATE LINEAGE DOCUMENTATION VIEWS
-- ============================================================================

USE DATABASE FMG_PRODUCTION;
USE SCHEMA GOVERNANCE;

-- Lineage summary view
CREATE OR REPLACE VIEW V_DATA_LINEAGE_SUMMARY AS
WITH source_reads AS (
    SELECT 
        obj.value:objectName::STRING AS table_name,
        COUNT(*) AS read_count,
        COUNT(DISTINCT user_name) AS unique_users,
        MAX(query_start_time) AS last_read
    FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY,
        LATERAL FLATTEN(input => direct_objects_accessed) obj
    WHERE query_start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
    AND obj.value:objectName::STRING LIKE 'FMG_%'
    GROUP BY 1
),
table_writes AS (
    SELECT 
        obj.value:objectName::STRING AS table_name,
        COUNT(*) AS write_count,
        MAX(query_start_time) AS last_write
    FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY,
        LATERAL FLATTEN(input => objects_modified) obj
    WHERE query_start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
    AND obj.value:objectName::STRING LIKE 'FMG_%'
    GROUP BY 1
)
SELECT 
    COALESCE(r.table_name, w.table_name) AS table_name,
    COALESCE(r.read_count, 0) AS reads_30d,
    COALESCE(r.unique_users, 0) AS unique_readers,
    COALESCE(w.write_count, 0) AS writes_30d,
    r.last_read,
    w.last_write,
    CASE 
        WHEN w.write_count IS NULL THEN 'SOURCE' 
        WHEN r.read_count IS NULL THEN 'TARGET'
        ELSE 'INTERMEDIATE'
    END AS table_role
FROM source_reads r
FULL OUTER JOIN table_writes w ON r.table_name = w.table_name;

-- Query the lineage summary
SELECT * FROM V_DATA_LINEAGE_SUMMARY ORDER BY reads_30d DESC;

-- ============================================================================
-- SECTION 8: DYNAMIC TABLE LINEAGE
-- ============================================================================

-- Dynamic tables have built-in lineage through their definitions
-- View the dependency chain
SELECT 
    name AS dynamic_table,
    target_lag,
    refresh_mode,
    text AS definition  -- Shows upstream dependencies
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES())
WHERE schema_name = 'DYNAMIC';

-- Check DT refresh dependencies
SELECT 
    name,
    target_lag,
    data_timestamp,
    DATEDIFF('minute', data_timestamp, CURRENT_TIMESTAMP()) AS lag_minutes
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES())
WHERE database_name = 'FMG_ANALYTICS';

-- ============================================================================
-- SECTION 9: IMPACT ANALYSIS
-- ============================================================================

/*
    Impact Analysis: "What breaks if I change this table?"
    
    Before making schema changes:
    1. Find all downstream dependencies
    2. Identify affected queries and users
    3. Plan migration/notification
*/

-- Find everything that depends on CUSTOMERS table
CREATE OR REPLACE VIEW V_CUSTOMERS_IMPACT_ANALYSIS AS
-- Views that reference CUSTOMERS
SELECT 
    'VIEW' AS object_type,
    referencing_database || '.' || referencing_schema || '.' || referencing_object_name AS object_name,
    'Direct reference in view definition' AS impact
FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
WHERE referenced_database = 'FMG_PRODUCTION'
AND referenced_schema = 'RAW'
AND referenced_object_name = 'CUSTOMERS'

UNION ALL

-- Recent queries that used CUSTOMERS
SELECT DISTINCT
    'QUERY' AS object_type,
    user_name || ' queries' AS object_name,
    COUNT(*) || ' queries in last 7 days' AS impact
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY,
    LATERAL FLATTEN(input => direct_objects_accessed) obj
WHERE query_start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
AND obj.value:objectName::STRING = 'FMG_PRODUCTION.RAW.CUSTOMERS'
GROUP BY user_name;

-- Run impact analysis
SELECT * FROM V_CUSTOMERS_IMPACT_ANALYSIS;

-- ============================================================================
-- SECTION 10: DATA FLOW VISUALIZATION DATA
-- ============================================================================

-- Generate data for lineage visualization tools
CREATE OR REPLACE VIEW V_DATA_FLOW_EDGES AS
SELECT DISTINCT
    src.value:objectName::STRING AS source_node,
    tgt.value:objectName::STRING AS target_node,
    'data_flow' AS edge_type
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY,
    LATERAL FLATTEN(input => base_objects_accessed) src,
    LATERAL FLATTEN(input => objects_modified) tgt
WHERE query_start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
AND src.value:objectName::STRING LIKE 'FMG%'
AND tgt.value:objectName::STRING LIKE 'FMG%'
AND src.value:objectName::STRING != tgt.value:objectName::STRING;

-- Generate nodes for visualization
CREATE OR REPLACE VIEW V_DATA_FLOW_NODES AS
SELECT DISTINCT
    table_name AS node_id,
    SPLIT_PART(table_name, '.', 1) AS database_name,
    SPLIT_PART(table_name, '.', 2) AS schema_name,
    SPLIT_PART(table_name, '.', 3) AS object_name,
    table_role AS node_type
FROM V_DATA_LINEAGE_SUMMARY
WHERE table_name IS NOT NULL;

-- View the data flow
SELECT * FROM V_DATA_FLOW_EDGES LIMIT 50;
SELECT * FROM V_DATA_FLOW_NODES;

-- ============================================================================
-- SECTION 11: LINEAGE BEST PRACTICES
-- ============================================================================

/*
    FMG LINEAGE BEST PRACTICES:
    
    1. DOCUMENTATION
       - Add comments to all tables and views
       - Document business ownership
       - Tag objects with data domains
    
    2. NAMING CONVENTIONS
       - RAW_ for raw data
       - STG_ for staging
       - DIM_ for dimensions
       - FACT_ for facts
       - V_ for views
       - DT_ for dynamic tables
    
    3. IMPACT ANALYSIS
       - Run before schema changes
       - Notify affected users
       - Plan migration paths
    
    4. MONITORING
       - Track unused tables (candidates for deprecation)
       - Identify hot spots (frequently accessed tables)
       - Monitor query patterns for optimization
    
    5. GOVERNANCE
       - Classify all PII columns
       - Track data access for compliance
       - Document data retention policies
*/

-- ============================================================================
-- SCRIPT COMPLETE!
-- ============================================================================

SELECT '✅ Data Lineage Setup Complete!' AS STATUS,
       'Query V_DATA_LINEAGE_SUMMARY for lineage insights' AS NEXT_STEP,
       CURRENT_TIMESTAMP() AS COMPLETED_AT;

