/*=============================================================================
  FMG SUITE - LAB 1: GETTING STARTED WITH SNOWFLAKE
  Script 3: Data Sharing
  
  Description: Configure secure data sharing for FMG internal analytics
               and external customer data access
  Prerequisites: ACCOUNTADMIN access, previous scripts completed
  Duration: ~15 minutes
=============================================================================*/

-- ============================================================================
-- SECTION 1: SET CONTEXT
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE FMG_DEV_XS;

-- ============================================================================
-- SECTION 2: UNDERSTAND DATA SHARING
-- ============================================================================

/*
    Snowflake Data Sharing enables:
    
    1. ZERO-COPY SHARING
       - Data stays in provider account
       - Consumer queries run against live data
       - No storage costs for consumer
       - Always up-to-date
    
    2. SECURE & GOVERNED
       - Provider controls what's shared
       - Consumer can't see underlying queries
       - Row-level security persists
       - Audit trail maintained
    
    3. USE CASES FOR FMG
       - Share production data with analytics team
       - Provide enterprise customers their own data
       - Access third-party data from Marketplace
       - Enable cross-region data access
    
    Share Hierarchy:
    
    ┌─────────────────────────────────────────────────────────────┐
    │                     PROVIDER ACCOUNT                         │
    │  ┌─────────────────────────────────────────────────────┐    │
    │  │  DATABASE: FMG_PRODUCTION                           │    │
    │  │  └── SCHEMA: RAW                                    │    │
    │  │      ├── CUSTOMERS (table)                          │    │
    │  │      ├── SUBSCRIPTIONS (table)                      │    │
    │  │      └── V_CUSTOMER_SUMMARY (secure view)           │    │
    │  └─────────────────────────────────────────────────────┘    │
    │                          │                                   │
    │                    CREATE SHARE                              │
    │                          ▼                                   │
    │  ┌─────────────────────────────────────────────────────┐    │
    │  │  SHARE: FMG_ANALYTICS_SHARE                         │    │
    │  │  ├── Granted: USAGE on DATABASE                     │    │
    │  │  ├── Granted: USAGE on SCHEMA                       │    │
    │  │  └── Granted: SELECT on tables/views                │    │
    │  └─────────────────────────────────────────────────────┘    │
    │                          │                                   │
    └──────────────────────────┼───────────────────────────────────┘
                               │ SHARE TO ACCOUNT
                               ▼
    ┌─────────────────────────────────────────────────────────────┐
    │                     CONSUMER ACCOUNT                         │
    │  ┌─────────────────────────────────────────────────────┐    │
    │  │  DATABASE: SHARED_FMG_DATA (from share)             │    │
    │  │  └── SCHEMA: RAW                                    │    │
    │  │      ├── CUSTOMERS (read-only)                      │    │
    │  │      ├── SUBSCRIPTIONS (read-only)                  │    │
    │  │      └── V_CUSTOMER_SUMMARY (read-only)             │    │
    │  └─────────────────────────────────────────────────────┘    │
    └─────────────────────────────────────────────────────────────┘
*/

-- ============================================================================
-- SECTION 3: CREATE SECURE VIEWS FOR SHARING
-- ============================================================================

/*
    Best Practice: Share SECURE VIEWS, not base tables
    
    Secure Views:
    - Hide underlying query logic from consumers
    - Can include row-level filtering
    - Optimizer can't be exploited to infer hidden data
*/

USE DATABASE FMG_PRODUCTION;
USE SCHEMA RAW;

-- Secure view for customer summary (hides internal fields)
CREATE OR REPLACE SECURE VIEW V_SHARED_CUSTOMER_SUMMARY AS
SELECT 
    customer_id,
    company_name,
    segment,
    industry,
    state,
    account_status,
    created_date,
    -- Derived fields (hide raw data logic)
    DATEDIFF('month', created_date, CURRENT_DATE()) AS tenure_months,
    CASE 
        WHEN account_status = 'Active' THEN TRUE 
        ELSE FALSE 
    END AS is_active
FROM CUSTOMERS
WHERE account_status IN ('Active', 'Paused');  -- Don't share churned customers

-- Secure view for subscription metrics
CREATE OR REPLACE SECURE VIEW V_SHARED_SUBSCRIPTION_METRICS AS
SELECT 
    s.customer_id,
    s.product_name,
    s.plan_tier,
    s.status,
    s.start_date,
    s.mrr_amount,
    s.arr_amount,
    -- Don't expose discount details
    CASE 
        WHEN s.discount_percent > 0 THEN 'Discounted'
        ELSE 'Standard'
    END AS pricing_type
FROM SUBSCRIPTIONS s
WHERE s.status IN ('Active', 'Pending');

-- Secure view for aggregated platform usage (no user-level details)
CREATE OR REPLACE SECURE VIEW V_SHARED_USAGE_SUMMARY AS
SELECT 
    p.customer_id,
    DATE_TRUNC('month', p.usage_date) AS usage_month,
    SUM(p.emails_sent) AS total_emails_sent,
    SUM(p.social_posts_published) AS total_social_posts,
    SUM(p.website_leads_generated) AS total_leads,
    SUM(p.myrepchat_messages_sent) AS total_texts,
    SUM(p.total_logins) AS total_logins,
    AVG(p.session_duration_minutes) AS avg_session_duration
FROM PLATFORM_USAGE_DAILY p
GROUP BY p.customer_id, DATE_TRUNC('month', p.usage_date);

-- ============================================================================
-- SECTION 4: CREATE INTERNAL ANALYTICS SHARE
-- ============================================================================

/*
    Scenario: Share production data with FMG's analytics team
    This allows the analytics team to query production data without
    having direct access to production schemas.
*/

-- Create the share
CREATE SHARE IF NOT EXISTS FMG_INTERNAL_ANALYTICS_SHARE
    COMMENT = 'Internal share of FMG production data for analytics team';

-- Grant access to the database
GRANT USAGE ON DATABASE FMG_PRODUCTION TO SHARE FMG_INTERNAL_ANALYTICS_SHARE;

-- Grant access to the schema
GRANT USAGE ON SCHEMA FMG_PRODUCTION.RAW TO SHARE FMG_INTERNAL_ANALYTICS_SHARE;

-- Grant access to specific secure views (not base tables!)
GRANT SELECT ON VIEW FMG_PRODUCTION.RAW.V_SHARED_CUSTOMER_SUMMARY 
    TO SHARE FMG_INTERNAL_ANALYTICS_SHARE;
GRANT SELECT ON VIEW FMG_PRODUCTION.RAW.V_SHARED_SUBSCRIPTION_METRICS 
    TO SHARE FMG_INTERNAL_ANALYTICS_SHARE;
GRANT SELECT ON VIEW FMG_PRODUCTION.RAW.V_SHARED_USAGE_SUMMARY 
    TO SHARE FMG_INTERNAL_ANALYTICS_SHARE;

-- Also share the curated customer 360 view
GRANT USAGE ON SCHEMA FMG_PRODUCTION.CURATED TO SHARE FMG_INTERNAL_ANALYTICS_SHARE;
GRANT SELECT ON VIEW FMG_PRODUCTION.CURATED.V_CUSTOMER_360 
    TO SHARE FMG_INTERNAL_ANALYTICS_SHARE;

-- View share details
SHOW GRANTS TO SHARE FMG_INTERNAL_ANALYTICS_SHARE;

-- ============================================================================
-- SECTION 5: CREATE ENTERPRISE CUSTOMER SHARE (Template)
-- ============================================================================

/*
    Scenario: Share data with FMG's enterprise customers
    Each enterprise customer should only see THEIR data.
    This requires a row-access policy or customer-specific views.
*/

-- Create a template secure view for customer-specific sharing
-- This view would be customized per customer
CREATE OR REPLACE SECURE VIEW V_CUSTOMER_SPECIFIC_DATA AS
SELECT 
    c.customer_id,
    c.company_name,
    u.user_id,
    u.first_name,
    u.last_name,
    u.role,
    u.last_login_date,
    s.product_name,
    s.plan_tier,
    s.status AS subscription_status
FROM CUSTOMERS c
JOIN USERS u ON c.customer_id = u.customer_id
JOIN SUBSCRIPTIONS s ON c.customer_id = s.customer_id
WHERE c.customer_id = 'CUST-001000';  -- Would be parameterized per customer share

-- For multi-tenant sharing, consider using:
-- 1. Separate shares per customer (most isolated)
-- 2. Reader accounts for customers (they get their own account)
-- 3. Row-access policies (single share, data filtered by consumer)

-- ============================================================================
-- SECTION 6: SIMULATE CONSUMING A SHARE (Same Account Demo)
-- ============================================================================

/*
    In a real scenario, the consumer would be a different Snowflake account.
    For this demo, we'll simulate by creating a database from our own share.
    
    Note: In production, you would run:
    ALTER SHARE FMG_INTERNAL_ANALYTICS_SHARE ADD ACCOUNTS = <consumer_account>;
*/

-- Create a "simulated consumer" database from the share
-- (This only works within the same account for demo purposes)
-- In real scenarios, the CONSUMER account would run:
-- CREATE DATABASE SHARED_FMG_DATA FROM SHARE <provider_account>.FMG_INTERNAL_ANALYTICS_SHARE;

-- ============================================================================
-- SECTION 7: ACCESS SNOWFLAKE MARKETPLACE
-- ============================================================================

/*
    The Snowflake Marketplace offers:
    - Free and paid datasets
    - Third-party data providers
    - Financial data, weather, demographics, etc.
    
    FMG could benefit from:
    - Financial market data (for advisor insights)
    - Economic indicators
    - Demographic data for marketing
    
    To access:
    1. Go to Snowsight → Data → Marketplace
    2. Search for relevant datasets
    3. Click "Get" to add to your account
    
    Example providers:
    - Refinitiv (market data)
    - S&P Global (financial data)
    - Experian (marketing data)
    - Weather Source (weather data)
*/

-- Check what shares you currently have access to
SHOW SHARES;

-- List available databases from shares
SHOW DATABASES LIKE '%SHARE%';

-- ============================================================================
-- SECTION 8: DATA EXCHANGE (Private Marketplace)
-- ============================================================================

/*
    Snowflake Data Exchange is a private marketplace where FMG could:
    
    1. Share data with SPECIFIC partners only
    2. Create listings for approved consumers
    3. Control who can access what
    
    Use cases for FMG:
    - Share benchmarking data with broker-dealer partners
    - Provide aggregated industry trends to research partners
    - Enable technology integrations with approved vendors
    
    To create a Data Exchange:
    1. Contact Snowflake account team
    2. Define exchange members and governance
    3. Publish listings for approved consumers
*/

-- ============================================================================
-- SECTION 9: LISTING SHARES AND VERIFYING ACCESS
-- ============================================================================

-- Show all shares we've created
SHOW SHARES LIKE 'FMG%';

-- Show detailed grants for a share
SHOW GRANTS TO SHARE FMG_INTERNAL_ANALYTICS_SHARE;

-- Show shares we're consuming (from other accounts)
SHOW SHARES INBOUND;

-- List all shared databases
SHOW DATABASES WHERE ORIGIN != '';

-- ============================================================================
-- SECTION 10: BEST PRACTICES FOR FMG DATA SHARING
-- ============================================================================

/*
    DATA SHARING BEST PRACTICES:
    
    1. SHARE SECURE VIEWS, NOT TABLES
       - Control what columns are exposed
       - Apply business logic in the view
       - Hide implementation details
    
    2. USE ROW-LEVEL SECURITY FOR MULTI-TENANT
       - Row Access Policies filter data automatically
       - Single share can serve multiple consumers
       - Consumer identity determines what they see
    
    3. MONITOR SHARE USAGE
       - Track who's querying shared data
       - Monitor query patterns for optimization
       - Review access logs for compliance
    
    4. VERSION YOUR SHARED OBJECTS
       - Communicate changes to consumers
       - Consider backward compatibility
       - Use schema versioning if needed
    
    5. DOCUMENT YOUR SHARES
       - Clear naming conventions
       - Detailed comments on objects
       - Data dictionaries for consumers
    
    6. CONSIDER READER ACCOUNTS FOR EXTERNAL
       - Gives customers their own account
       - Billed back to FMG (provider pays)
       - Customers can use Snowsight/tools
*/

-- ============================================================================
-- SECTION 11: CLEANUP (Optional)
-- ============================================================================

-- If you want to remove the share (don't run during lab!)
-- DROP SHARE IF EXISTS FMG_INTERNAL_ANALYTICS_SHARE;

-- ============================================================================
-- SCRIPT COMPLETE!
-- ============================================================================

-- Verify what we've created
SELECT 'Secure Views Created' AS object_type, COUNT(*) AS count
FROM FMG_PRODUCTION.INFORMATION_SCHEMA.VIEWS 
WHERE IS_SECURE = 'YES' AND TABLE_SCHEMA = 'RAW'
UNION ALL
SELECT 'Shares Created', COUNT(*)
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID(-4)));

SELECT '✅ Data Sharing Setup Complete!' AS STATUS,
       CURRENT_TIMESTAMP() AS COMPLETED_AT;

