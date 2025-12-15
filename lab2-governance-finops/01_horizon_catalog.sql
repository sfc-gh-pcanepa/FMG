/*=============================================================================
  FMG SUITE - LAB 2: GOVERNANCE + FINOPS
  Script 1: Horizon Catalog Overview
  
  Description: Explore Snowflake Horizon for data discovery, classification,
               and governance of FMG's data assets
  Prerequisites: Lab 1 completed, FMG databases created
  Duration: ~15 minutes
=============================================================================*/

-- ============================================================================
-- SECTION 1: SET CONTEXT
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE FMG_DEV_XS;
USE DATABASE FMG_PRODUCTION;

-- ============================================================================
-- SECTION 2: UNDERSTANDING SNOWFLAKE HORIZON
-- ============================================================================

/*
    Snowflake Horizon is the built-in governance solution that includes:
    
    1. DATA DISCOVERY & CATALOG
       - Universal Search across all objects
       - Object tagging and classification
       - Data lineage visualization
    
    2. DATA CLASSIFICATION
       - Automatic PII detection
       - Semantic categories (email, phone, SSN, etc.)
       - Privacy categories (identifier, quasi-identifier)
    
    3. DATA QUALITY
       - Data metric functions
       - Quality monitoring
    
    4. DATA ACCESS
       - Row access policies
       - Column masking policies
       - Tag-based policies
    
    5. DATA LINEAGE
       - Automatic column-level lineage
       - Impact analysis
       - Query history integration
*/

-- ============================================================================
-- SECTION 3: CREATE GOVERNANCE TAGS
-- ============================================================================

/*
    Tags are key-value pairs that help classify and govern data.
    FMG will use tags for:
    - Data sensitivity classification
    - Data domain ownership
    - Compliance requirements
    - Cost allocation
*/

-- Create a schema for governance objects
CREATE SCHEMA IF NOT EXISTS FMG_PRODUCTION.GOVERNANCE
    COMMENT = 'Schema for governance policies, tags, and classifications';

USE SCHEMA FMG_PRODUCTION.GOVERNANCE;

-- Tag: Data Sensitivity Level
CREATE TAG IF NOT EXISTS DATA_SENSITIVITY
    ALLOWED_VALUES = 'PUBLIC', 'INTERNAL', 'CONFIDENTIAL', 'RESTRICTED'
    COMMENT = 'Classification of data sensitivity for access control';

-- Tag: Data Domain (Business Ownership)
CREATE TAG IF NOT EXISTS DATA_DOMAIN
    ALLOWED_VALUES = 'CUSTOMER', 'FINANCIAL', 'OPERATIONS', 'MARKETING', 'PRODUCT', 'HR'
    COMMENT = 'Business domain that owns and governs this data';

-- Tag: PII Category
CREATE TAG IF NOT EXISTS PII_CATEGORY
    ALLOWED_VALUES = 'DIRECT_IDENTIFIER', 'QUASI_IDENTIFIER', 'SENSITIVE', 'NON_PII'
    COMMENT = 'Personal Identifiable Information classification';

-- Tag: Retention Period
CREATE TAG IF NOT EXISTS RETENTION_PERIOD
    ALLOWED_VALUES = '30_DAYS', '90_DAYS', '1_YEAR', '3_YEARS', '7_YEARS', 'INDEFINITE'
    COMMENT = 'Data retention period for compliance';

-- Tag: Cost Center (for FinOps)
CREATE TAG IF NOT EXISTS COST_CENTER
    ALLOWED_VALUES = 'ENGINEERING', 'ANALYTICS', 'DATA_SCIENCE', 'FINANCE', 'OPERATIONS', 'EXECUTIVE'
    COMMENT = 'Cost allocation center for warehouse and storage costs';

-- Show created tags
SHOW TAGS IN SCHEMA FMG_PRODUCTION.GOVERNANCE;

-- ============================================================================
-- SECTION 4: APPLY TAGS TO DATABASE OBJECTS
-- ============================================================================

-- Tag the CUSTOMERS table
ALTER TABLE FMG_PRODUCTION.RAW.CUSTOMERS SET TAG 
    FMG_PRODUCTION.GOVERNANCE.DATA_SENSITIVITY = 'CONFIDENTIAL',
    FMG_PRODUCTION.GOVERNANCE.DATA_DOMAIN = 'CUSTOMER',
    FMG_PRODUCTION.GOVERNANCE.RETENTION_PERIOD = '7_YEARS';

-- Tag the USERS table (contains PII)
ALTER TABLE FMG_PRODUCTION.RAW.USERS SET TAG 
    FMG_PRODUCTION.GOVERNANCE.DATA_SENSITIVITY = 'RESTRICTED',
    FMG_PRODUCTION.GOVERNANCE.DATA_DOMAIN = 'CUSTOMER',
    FMG_PRODUCTION.GOVERNANCE.RETENTION_PERIOD = '7_YEARS';

-- Tag specific PII columns in USERS table
ALTER TABLE FMG_PRODUCTION.RAW.USERS MODIFY COLUMN 
    email SET TAG FMG_PRODUCTION.GOVERNANCE.PII_CATEGORY = 'DIRECT_IDENTIFIER';

ALTER TABLE FMG_PRODUCTION.RAW.USERS MODIFY COLUMN 
    phone SET TAG FMG_PRODUCTION.GOVERNANCE.PII_CATEGORY = 'DIRECT_IDENTIFIER';

ALTER TABLE FMG_PRODUCTION.RAW.USERS MODIFY COLUMN 
    first_name SET TAG FMG_PRODUCTION.GOVERNANCE.PII_CATEGORY = 'QUASI_IDENTIFIER';

ALTER TABLE FMG_PRODUCTION.RAW.USERS MODIFY COLUMN 
    last_name SET TAG FMG_PRODUCTION.GOVERNANCE.PII_CATEGORY = 'QUASI_IDENTIFIER';

-- Tag financial data
ALTER TABLE FMG_PRODUCTION.RAW.SUBSCRIPTIONS SET TAG 
    FMG_PRODUCTION.GOVERNANCE.DATA_SENSITIVITY = 'CONFIDENTIAL',
    FMG_PRODUCTION.GOVERNANCE.DATA_DOMAIN = 'FINANCIAL',
    FMG_PRODUCTION.GOVERNANCE.RETENTION_PERIOD = '7_YEARS';

ALTER TABLE FMG_PRODUCTION.RAW.INVOICES SET TAG 
    FMG_PRODUCTION.GOVERNANCE.DATA_SENSITIVITY = 'RESTRICTED',
    FMG_PRODUCTION.GOVERNANCE.DATA_DOMAIN = 'FINANCIAL',
    FMG_PRODUCTION.GOVERNANCE.RETENTION_PERIOD = '7_YEARS';

-- Tag warehouses for cost allocation
ALTER WAREHOUSE FMG_DEV_XS SET TAG 
    FMG_PRODUCTION.GOVERNANCE.COST_CENTER = 'ENGINEERING';

ALTER WAREHOUSE FMG_ANALYTICS_M SET TAG 
    FMG_PRODUCTION.GOVERNANCE.COST_CENTER = 'ANALYTICS';

ALTER WAREHOUSE FMG_ML_L SET TAG 
    FMG_PRODUCTION.GOVERNANCE.COST_CENTER = 'DATA_SCIENCE';

-- ============================================================================
-- SECTION 5: AUTOMATIC DATA CLASSIFICATION
-- ============================================================================

/*
    Snowflake can automatically detect sensitive data using:
    - SYSTEM$CLASSIFY: Analyzes columns and suggests classifications
    - SYSTEM$GET_TAG: Retrieves tags from objects
    
    Classification detects:
    - Semantic categories: EMAIL, PHONE, NAME, ADDRESS, etc.
    - Privacy categories: IDENTIFIER, QUASI_IDENTIFIER
*/

-- Run automatic classification on USERS table
-- Note: This requires Enterprise+ edition
-- SELECT SYSTEM$CLASSIFY('FMG_PRODUCTION.RAW.USERS', {'auto_tag': true});

-- Manually classify based on column patterns
-- View columns that look like PII
SELECT 
    table_name,
    column_name,
    data_type,
    CASE 
        WHEN LOWER(column_name) LIKE '%email%' THEN 'EMAIL'
        WHEN LOWER(column_name) LIKE '%phone%' THEN 'PHONE'
        WHEN LOWER(column_name) LIKE '%name%' THEN 'NAME'
        WHEN LOWER(column_name) LIKE '%address%' THEN 'ADDRESS'
        WHEN LOWER(column_name) LIKE '%ssn%' OR LOWER(column_name) LIKE '%social%' THEN 'SSN'
        ELSE 'OTHER'
    END AS likely_semantic_category
FROM FMG_PRODUCTION.INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = 'RAW'
AND (
    LOWER(column_name) LIKE '%email%' 
    OR LOWER(column_name) LIKE '%phone%'
    OR LOWER(column_name) LIKE '%name%'
    OR LOWER(column_name) LIKE '%address%'
)
ORDER BY table_name, likely_semantic_category;

-- ============================================================================
-- SECTION 6: QUERY TAGS AND CATALOG
-- ============================================================================

-- Find all objects with a specific sensitivity level
SELECT *
FROM TABLE(
    FMG_PRODUCTION.INFORMATION_SCHEMA.TAG_REFERENCES(
        'FMG_PRODUCTION.GOVERNANCE.DATA_SENSITIVITY', 
        'TABLE'
    )
);

-- Find all PII columns in the database
SELECT 
    object_database,
    object_schema,
    object_name AS table_name,
    column_name,
    tag_value AS pii_category
FROM TABLE(
    FMG_PRODUCTION.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
        'FMG_PRODUCTION.GOVERNANCE.PII_CATEGORY', 
        'TABLE'
    )
)
WHERE tag_value IS NOT NULL
ORDER BY object_name, column_name;

-- Get all tags on a specific table
SELECT 
    tag_database,
    tag_schema,
    tag_name,
    tag_value
FROM TABLE(
    FMG_PRODUCTION.INFORMATION_SCHEMA.TAG_REFERENCES(
        'FMG_PRODUCTION.RAW.CUSTOMERS', 
        'TABLE'
    )
);

-- ============================================================================
-- SECTION 7: DATA LINEAGE
-- ============================================================================

/*
    Snowflake tracks column-level lineage automatically.
    Access lineage via:
    - Snowsight UI (visual lineage graph)
    - ACCOUNT_USAGE.ACCESS_HISTORY (query-based)
    
    Lineage helps answer:
    - Where did this data come from?
    - What reports use this table?
    - If I change this column, what breaks?
*/

-- Query access history to see data lineage patterns
-- Note: ACCESS_HISTORY has ~3 hour latency
SELECT 
    query_start_time,
    user_name,
    direct_objects_accessed,
    base_objects_accessed,
    objects_modified
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
WHERE query_start_time > DATEADD('day', -7, CURRENT_TIMESTAMP())
AND ARRAY_SIZE(direct_objects_accessed) > 0
ORDER BY query_start_time DESC
LIMIT 20;

-- Find which tables are most frequently accessed
SELECT 
    obj.value:objectName::STRING AS object_name,
    obj.value:objectDomain::STRING AS object_type,
    COUNT(*) AS access_count,
    COUNT(DISTINCT user_name) AS unique_users
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY,
    LATERAL FLATTEN(input => direct_objects_accessed) obj
WHERE query_start_time > DATEADD('day', -30, CURRENT_TIMESTAMP())
AND obj.value:objectName::STRING LIKE 'FMG%'
GROUP BY 1, 2
ORDER BY access_count DESC
LIMIT 20;

-- ============================================================================
-- SECTION 8: UNIVERSAL SEARCH
-- ============================================================================

/*
    Snowflake Universal Search (in Snowsight) allows searching for:
    - Tables, views, columns by name
    - Data values within tables
    - Tags and classifications
    - Queries in history
    
    Best Practice: Use descriptive names and comments for discoverability!
*/

-- Add comments to improve searchability
COMMENT ON TABLE FMG_PRODUCTION.RAW.CUSTOMERS IS 
    'Master table of all FMG customer accounts including RIAs, broker-dealers, 
     and insurance companies. Contains company demographics and account status.';

COMMENT ON TABLE FMG_PRODUCTION.RAW.USERS IS 
    'Individual users (financial advisors, staff) associated with customer accounts.
     Contains PII including email and phone. Protected by masking policies.';

COMMENT ON TABLE FMG_PRODUCTION.RAW.SUBSCRIPTIONS IS 
    'Product subscriptions for each customer including MRR, plan tiers, and renewal dates.
     Primary source for revenue analytics.';

COMMENT ON COLUMN FMG_PRODUCTION.RAW.CUSTOMERS.CUSTOMER_ID IS 
    'Unique identifier for customer accounts. Format: CUST-XXXXXX';

COMMENT ON COLUMN FMG_PRODUCTION.RAW.CUSTOMERS.SEGMENT IS 
    'Customer segment: SMB, Mid-Market, or Enterprise. Determines pricing tier.';

-- View comments on tables
SHOW TABLES IN SCHEMA FMG_PRODUCTION.RAW;

-- ============================================================================
-- SECTION 9: DATA QUALITY MONITORING (Preview)
-- ============================================================================

/*
    Snowflake Data Metric Functions allow monitoring data quality:
    - NULL_COUNT: Count of nulls in a column
    - DUPLICATE_COUNT: Count of duplicate values
    - UNIQUE_COUNT: Count of distinct values
    - Custom metrics using UDFs
    
    Note: DMFs are a preview feature and may require enablement
*/

-- Create a simple data quality view (works without DMF feature)
CREATE OR REPLACE VIEW FMG_PRODUCTION.GOVERNANCE.V_DATA_QUALITY_CUSTOMERS AS
SELECT 
    'CUSTOMERS' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT customer_id) AS unique_customers,
    COUNT(*) - COUNT(company_name) AS null_company_names,
    COUNT(*) - COUNT(segment) AS null_segments,
    COUNT(*) - COUNT(account_status) AS null_statuses,
    COUNT(CASE WHEN account_status NOT IN ('Active', 'Churned', 'Paused', 'Trial') THEN 1 END) AS invalid_statuses,
    ROUND(COUNT(CASE WHEN account_status = 'Active' THEN 1 END) * 100.0 / COUNT(*), 2) AS pct_active
FROM FMG_PRODUCTION.RAW.CUSTOMERS;

-- Create a data quality view for users
CREATE OR REPLACE VIEW FMG_PRODUCTION.GOVERNANCE.V_DATA_QUALITY_USERS AS
SELECT 
    'USERS' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT user_id) AS unique_users,
    COUNT(DISTINCT customer_id) AS unique_customers,
    COUNT(*) - COUNT(email) AS null_emails,
    COUNT(CASE WHEN email NOT LIKE '%@%.%' THEN 1 END) AS invalid_emails,
    COUNT(CASE WHEN last_login_date < DATEADD('year', -1, CURRENT_DATE()) THEN 1 END) AS stale_users,
    ROUND(COUNT(CASE WHEN mfa_enabled THEN 1 END) * 100.0 / COUNT(*), 2) AS pct_mfa_enabled
FROM FMG_PRODUCTION.RAW.USERS;

-- Run data quality checks
SELECT * FROM FMG_PRODUCTION.GOVERNANCE.V_DATA_QUALITY_CUSTOMERS;
SELECT * FROM FMG_PRODUCTION.GOVERNANCE.V_DATA_QUALITY_USERS;

-- ============================================================================
-- SECTION 10: CATALOG BEST PRACTICES FOR FMG
-- ============================================================================

/*
    FMG DATA CATALOG BEST PRACTICES:
    
    1. CONSISTENT TAGGING
       - Apply sensitivity tags to ALL tables
       - Tag PII columns at column level
       - Use cost center tags for FinOps
    
    2. RICH DOCUMENTATION
       - Add comments to all tables and important columns
       - Use business-friendly descriptions
       - Include data owners and refresh schedules
    
    3. NAMING CONVENTIONS
       - Prefix schemas: RAW_, STAGING_, CURATED_
       - Views: V_ prefix for standard, MV_ for materialized
       - Tags: Use UPPERCASE with underscores
    
    4. REGULAR CLASSIFICATION
       - Run auto-classification monthly
       - Review and validate PII detection
       - Update tags as data evolves
    
    5. LINEAGE MONITORING
       - Review access patterns quarterly
       - Identify unused objects for cleanup
       - Track data flow for compliance
*/

-- ============================================================================
-- SCRIPT COMPLETE!
-- ============================================================================

SELECT '✅ Horizon Catalog Setup Complete!' AS STATUS,
       (SELECT COUNT(*) FROM TABLE(RESULT_SCAN(LAST_QUERY_ID(-10)))) AS TAGS_CREATED,
       CURRENT_TIMESTAMP() AS COMPLETED_AT;

