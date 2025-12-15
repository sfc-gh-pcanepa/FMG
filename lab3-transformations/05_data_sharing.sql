/*=============================================================================
  FMG SUITE - LAB 3: TRANSFORMATIONS IN SNOWFLAKE
  Script 5: Data Distribution (Sharing)
  
  Description: Share transformed data across FMG teams and external partners
  Prerequisites: Analytics tables and views created
  Duration: ~10 minutes
=============================================================================*/

-- ============================================================================
-- SECTION 1: SET CONTEXT
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE FMG_DEV_XS;

-- ============================================================================
-- SECTION 2: INTERNAL DATA SHARING STRATEGY
-- ============================================================================

/*
    FMG Data Distribution Strategy:
    
    ┌─────────────────────────────────────────────────────────────────────────┐
    │                     FMG DATA DISTRIBUTION                                │
    ├─────────────────────────────────────────────────────────────────────────┤
    │                                                                          │
    │   PRODUCTION             ANALYTICS              CONSUMERS               │
    │   (Source of Truth)      (Transformed)          (End Users)             │
    │                                                                          │
    │   ┌─────────────┐       ┌─────────────┐       ┌─────────────┐          │
    │   │ RAW.        │──────▶│ DT_CUSTOMER │──────▶│ BI Tools    │          │
    │   │ CUSTOMERS   │       │ _360        │       │ (Tableau)   │          │
    │   └─────────────┘       └─────────────┘       └─────────────┘          │
    │                                │                                        │
    │   ┌─────────────┐              │             ┌─────────────┐          │
    │   │ RAW.        │──────────────┼────────────▶│ Data Science│          │
    │   │ SUBSCRIPTIONS│             │             │ (Notebooks) │          │
    │   └─────────────┘              ▼             └─────────────┘          │
    │                         ┌─────────────┐                                 │
    │   ┌─────────────┐       │ EXECUTIVE   │       ┌─────────────┐          │
    │   │ RAW.        │──────▶│ _KPIS       │──────▶│ Exec Dash   │          │
    │   │ HEALTH      │       │             │       │             │          │
    │   └─────────────┘       └─────────────┘       └─────────────┘          │
    │                                                                          │
    │   ───────────────── INTERNAL SHARING ─────────────────                  │
    │                                                                          │
    │   ┌─────────────┐                             ┌─────────────┐          │
    │   │ ANALYTICS   │ ─── EXTERNAL SHARE ───────▶│ Enterprise  │          │
    │   │ BENCHMARKS  │                             │ Customers   │          │
    │   └─────────────┘                             └─────────────┘          │
    │                                                                          │
    └─────────────────────────────────────────────────────────────────────────┘
*/

-- ============================================================================
-- SECTION 3: CREATE SHAREABLE VIEWS
-- ============================================================================

USE DATABASE FMG_ANALYTICS;

-- Create schema for shareable objects
CREATE SCHEMA IF NOT EXISTS FMG_ANALYTICS.SHARED
    COMMENT = 'Curated objects for internal and external sharing';

USE SCHEMA FMG_ANALYTICS.SHARED;

-- Secure view for BI tools (customer summary)
CREATE OR REPLACE SECURE VIEW SV_CUSTOMER_SUMMARY AS
SELECT 
    c.customer_id,
    c.company_name,
    c.segment,
    c.industry,
    c.state,
    c.account_status,
    DATEDIFF('month', c.created_date, CURRENT_DATE()) AS tenure_months,
    COALESCE(s.total_mrr, 0) AS mrr,
    COALESCE(s.product_count, 0) AS products,
    h.overall_health_score,
    h.churn_risk
FROM FMG_PRODUCTION.RAW.CUSTOMERS c
LEFT JOIN (
    SELECT customer_id, SUM(mrr_amount) AS total_mrr, COUNT(*) AS product_count
    FROM FMG_PRODUCTION.RAW.SUBSCRIPTIONS WHERE status = 'Active'
    GROUP BY customer_id
) s ON c.customer_id = s.customer_id
LEFT JOIN (
    SELECT * FROM FMG_PRODUCTION.RAW.CUSTOMER_HEALTH_SCORES
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY snapshot_date DESC) = 1
) h ON c.customer_id = h.customer_id
WHERE c.account_status IN ('Active', 'Paused');

-- Secure view for executive dashboard
CREATE OR REPLACE SECURE VIEW SV_EXECUTIVE_METRICS AS
SELECT 
    CURRENT_DATE() AS report_date,
    COUNT(DISTINCT customer_id) AS total_customers,
    SUM(mrr) AS total_mrr,
    AVG(mrr) AS avg_mrr,
    AVG(overall_health_score) AS avg_health_score,
    COUNT(CASE WHEN churn_risk = 'Critical' THEN 1 END) AS critical_risk_count
FROM SV_CUSTOMER_SUMMARY;

-- Secure view for product usage (for product team)
CREATE OR REPLACE SECURE VIEW SV_PRODUCT_USAGE_SUMMARY AS
SELECT 
    DATE_TRUNC('month', usage_date) AS usage_month,
    COUNT(DISTINCT customer_id) AS active_customers,
    SUM(emails_sent) AS total_emails,
    SUM(social_posts_published) AS total_social_posts,
    SUM(website_leads_generated) AS total_leads,
    SUM(myrepchat_messages_sent) AS total_texts,
    AVG(session_duration_minutes) AS avg_session_minutes
FROM FMG_PRODUCTION.RAW.PLATFORM_USAGE_DAILY
GROUP BY 1
ORDER BY 1 DESC;

-- ============================================================================
-- SECTION 4: CREATE INTERNAL SHARE
-- ============================================================================

-- Create share for analytics team
CREATE SHARE IF NOT EXISTS FMG_ANALYTICS_INTERNAL_SHARE
    COMMENT = 'Internal share of analytics data for BI and reporting teams';

-- Grant access to the database
GRANT USAGE ON DATABASE FMG_ANALYTICS TO SHARE FMG_ANALYTICS_INTERNAL_SHARE;

-- Grant access to the shared schema
GRANT USAGE ON SCHEMA FMG_ANALYTICS.SHARED TO SHARE FMG_ANALYTICS_INTERNAL_SHARE;

-- Grant access to specific views
GRANT SELECT ON VIEW FMG_ANALYTICS.SHARED.SV_CUSTOMER_SUMMARY 
    TO SHARE FMG_ANALYTICS_INTERNAL_SHARE;
GRANT SELECT ON VIEW FMG_ANALYTICS.SHARED.SV_EXECUTIVE_METRICS 
    TO SHARE FMG_ANALYTICS_INTERNAL_SHARE;
GRANT SELECT ON VIEW FMG_ANALYTICS.SHARED.SV_PRODUCT_USAGE_SUMMARY 
    TO SHARE FMG_ANALYTICS_INTERNAL_SHARE;

-- Also share dynamic tables if available
GRANT USAGE ON SCHEMA FMG_ANALYTICS.DYNAMIC TO SHARE FMG_ANALYTICS_INTERNAL_SHARE;
-- GRANT SELECT ON DYNAMIC TABLE FMG_ANALYTICS.DYNAMIC.DT_CUSTOMER_360 
--     TO SHARE FMG_ANALYTICS_INTERNAL_SHARE;

-- View share configuration
SHOW GRANTS TO SHARE FMG_ANALYTICS_INTERNAL_SHARE;

-- ============================================================================
-- SECTION 5: CREATE EXTERNAL SHARE (For Enterprise Customers)
-- ============================================================================

/*
    Scenario: FMG wants to share aggregated industry benchmarks 
    with enterprise customers. Each customer sees only:
    - Their own data (if applicable)
    - Anonymized industry benchmarks
*/

-- Create benchmark data (anonymized aggregates)
CREATE OR REPLACE SECURE VIEW SV_INDUSTRY_BENCHMARKS AS
SELECT 
    industry,
    segment,
    COUNT(*) AS sample_size,
    AVG(overall_health_score) AS avg_health_score,
    AVG(tenure_months) AS avg_tenure_months,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY mrr) AS median_mrr,
    AVG(products) AS avg_products
FROM SV_CUSTOMER_SUMMARY
GROUP BY industry, segment
HAVING COUNT(*) >= 10;  -- Only show if enough data for anonymity

-- Create external share
CREATE SHARE IF NOT EXISTS FMG_ENTERPRISE_CUSTOMER_SHARE
    COMMENT = 'Share for FMG enterprise customers - benchmarks and their own data';

-- Grant access
GRANT USAGE ON DATABASE FMG_ANALYTICS TO SHARE FMG_ENTERPRISE_CUSTOMER_SHARE;
GRANT USAGE ON SCHEMA FMG_ANALYTICS.SHARED TO SHARE FMG_ENTERPRISE_CUSTOMER_SHARE;
GRANT SELECT ON VIEW FMG_ANALYTICS.SHARED.SV_INDUSTRY_BENCHMARKS 
    TO SHARE FMG_ENTERPRISE_CUSTOMER_SHARE;

-- To share with a specific account:
-- ALTER SHARE FMG_ENTERPRISE_CUSTOMER_SHARE ADD ACCOUNTS = <customer_account>;

-- ============================================================================
-- SECTION 6: DATA EXCHANGE (Private Marketplace)
-- ============================================================================

/*
    FMG could create a private Data Exchange for:
    - Partner broker-dealers
    - Technology integration partners
    - Research collaborators
    
    Benefits:
    - Controlled access to approved partners
    - Self-service data discovery
    - Governed data distribution
    
    To create:
    1. Contact Snowflake account team
    2. Define exchange membership and governance
    3. Publish data products as listings
*/

-- ============================================================================
-- SECTION 7: READER ACCOUNTS (For CUSTOMERS WITHOUT SNOWFLAKE)
-- ============================================================================

/*
    If FMG customers don't have Snowflake accounts, FMG can create
    "Reader Accounts" for them:
    
    - Managed by FMG (provider pays for compute)
    - Customer gets read-only access to shared data
    - Can use Snowsight or connect with BI tools
    
    To create a reader account:
    
    CREATE MANAGED ACCOUNT enterprise_customer_123
        ADMIN_NAME = 'admin'
        ADMIN_PASSWORD = 'SecureP@ss123!'
        TYPE = READER
        COMMENT = 'Reader account for Enterprise Customer XYZ';
    
    Then share data with that account:
    ALTER SHARE FMG_ENTERPRISE_CUSTOMER_SHARE 
        ADD ACCOUNTS = enterprise_customer_123;
*/

-- ============================================================================
-- SECTION 8: MONITOR SHARE USAGE
-- ============================================================================

-- View shares we've created
SHOW SHARES LIKE 'FMG%';

-- View who has access to our shares
SELECT 
    share_name,
    consumer_account,
    consumer_name,
    created_on
FROM SNOWFLAKE.ACCOUNT_USAGE.SHARE_USAGE
WHERE share_name LIKE 'FMG%'
ORDER BY created_on DESC;

-- Monitor queries from share consumers
SELECT 
    share_name,
    consumer_account,
    query_date,
    query_count,
    storage_bytes
FROM SNOWFLAKE.DATA_SHARING_USAGE.LISTING_CONSUMPTION_DAILY
WHERE share_name LIKE 'FMG%'
ORDER BY query_date DESC;

-- ============================================================================
-- SECTION 9: SHARE MAINTENANCE
-- ============================================================================

-- Add new objects to existing share
-- GRANT SELECT ON VIEW FMG_ANALYTICS.SHARED.SV_NEW_VIEW 
--     TO SHARE FMG_ANALYTICS_INTERNAL_SHARE;

-- Remove objects from share
-- REVOKE SELECT ON VIEW FMG_ANALYTICS.SHARED.SV_OLD_VIEW 
--     FROM SHARE FMG_ANALYTICS_INTERNAL_SHARE;

-- Add consumer accounts
-- ALTER SHARE FMG_ANALYTICS_INTERNAL_SHARE 
--     ADD ACCOUNTS = account1, account2;

-- Remove consumer accounts
-- ALTER SHARE FMG_ANALYTICS_INTERNAL_SHARE 
--     REMOVE ACCOUNTS = account1;

-- Drop a share (removes access for all consumers!)
-- DROP SHARE FMG_ANALYTICS_INTERNAL_SHARE;

-- ============================================================================
-- SECTION 10: SHARING BEST PRACTICES
-- ============================================================================

/*
    FMG DATA SHARING BEST PRACTICES:
    
    1. SHARE SECURE VIEWS, NOT TABLES
       - Control what columns are exposed
       - Apply row-level filtering
       - Hide implementation details
    
    2. NAMING CONVENTIONS
       - SV_ prefix for Secure Views
       - _SHARE suffix for shares
       - Clear, descriptive names
    
    3. DOCUMENTATION
       - Comment all shared objects
       - Maintain data dictionary
       - Communicate changes to consumers
    
    4. SECURITY
       - Use row access policies for multi-tenant
       - Mask PII before sharing
       - Regular access reviews
    
    5. MONITORING
       - Track consumer usage
       - Monitor for stale consumers
       - Alert on unusual patterns
    
    6. VERSIONING
       - Plan for schema evolution
       - Communicate breaking changes
       - Consider versioned views (V1, V2)
*/

-- ============================================================================
-- SECTION 11: VERIFY SHARE SETUP
-- ============================================================================

-- Summary of shares
SELECT 
    'INTERNAL_SHARE' AS share_type,
    (SELECT COUNT(*) FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))) AS object_count
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
UNION ALL
SELECT 
    'ENTERPRISE_SHARE',
    (SELECT COUNT(*) FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())))
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- List all shareable views
SELECT 
    table_schema,
    table_name,
    is_secure,
    comment
FROM FMG_ANALYTICS.INFORMATION_SCHEMA.VIEWS
WHERE table_schema = 'SHARED'
AND is_secure = 'YES';

-- ============================================================================
-- SCRIPT COMPLETE!
-- ============================================================================

SELECT '✅ Data Distribution Setup Complete!' AS STATUS,
       'Shares created for internal analytics and enterprise customers' AS SUMMARY,
       CURRENT_TIMESTAMP() AS COMPLETED_AT;

