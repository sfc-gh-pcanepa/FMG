/*=============================================================================
  FMG SUITE - LAB 3: TRANSFORMATIONS IN SNOWFLAKE
  Script 2: Dynamic Tables
  
  Description: Create declarative, auto-maintained data transformations
  Prerequisites: FMG databases and sample data created
  Duration: ~15 minutes
=============================================================================*/

-- ============================================================================
-- SECTION 1: SET CONTEXT
-- ============================================================================

USE ROLE FMG_ADMIN;
USE WAREHOUSE FMG_ANALYTICS_M;
USE DATABASE FMG_ANALYTICS;

-- Create schema for dynamic tables
CREATE SCHEMA IF NOT EXISTS FMG_ANALYTICS.DYNAMIC
    COMMENT = 'Dynamic tables for automated transformations';

USE SCHEMA FMG_ANALYTICS.DYNAMIC;

-- ============================================================================
-- SECTION 2: UNDERSTANDING DYNAMIC TABLES
-- ============================================================================

/*
    Dynamic Tables are declarative transformations:
    
    KEY PROPERTIES:
    - TARGET_LAG: How fresh the data should be
      - 'DOWNSTREAM': Match the lag of downstream consumers
      - '1 minute' to '7 days': Specific freshness target
    
    - WAREHOUSE: Compute for refreshes
    
    - REFRESH_MODE: 
      - AUTO: Snowflake decides (incremental if possible)
      - FULL: Always full refresh
      - INCREMENTAL: Force incremental (may fail if not possible)
    
    BENEFITS:
    - No stream/task management
    - Automatic incremental refresh when possible
    - Chaining is simple (DT can read from other DTs)
    - Built-in monitoring and freshness SLAs
*/

-- ============================================================================
-- SECTION 3: CREATE CUSTOMER 360 DYNAMIC TABLE
-- ============================================================================

-- Customer 360 view - comprehensive customer profile
CREATE OR REPLACE DYNAMIC TABLE DT_CUSTOMER_360
    TARGET_LAG = '1 hour'
    WAREHOUSE = FMG_ANALYTICS_M
    COMMENT = 'Comprehensive customer profile combining multiple data sources'
AS
SELECT 
    -- Customer Core
    c.customer_id,
    c.company_name,
    c.segment,
    c.industry,
    c.sub_industry,
    c.state,
    c.account_status,
    c.csm_owner,
    c.created_date AS customer_since,
    DATEDIFF('month', c.created_date, CURRENT_DATE()) AS tenure_months,
    
    -- Subscription Metrics
    COALESCE(s.total_mrr, 0) AS total_mrr,
    COALESCE(s.total_arr, 0) AS total_arr,
    COALESCE(s.product_count, 0) AS active_products,
    s.products_list,
    
    -- User Metrics
    COALESCE(u.total_users, 0) AS total_users,
    COALESCE(u.active_users, 0) AS active_users,
    u.last_login,
    DATEDIFF('day', u.last_login, CURRENT_TIMESTAMP()) AS days_since_last_login,
    
    -- Health Score
    h.overall_health_score,
    h.churn_risk,
    h.health_trend,
    
    -- Support Metrics (last 90 days)
    COALESCE(t.ticket_count, 0) AS tickets_90d,
    t.avg_csat,
    
    -- NPS
    n.latest_nps_score,
    n.nps_category,
    
    -- Calculated Fields
    CASE 
        WHEN c.account_status = 'Churned' THEN 'Churned'
        WHEN h.churn_risk = 'Critical' THEN 'At Risk'
        WHEN h.overall_health_score >= 80 THEN 'Healthy'
        WHEN h.overall_health_score >= 60 THEN 'Needs Attention'
        ELSE 'At Risk'
    END AS customer_health_status,
    
    CURRENT_TIMESTAMP() AS _refreshed_at

FROM FMG_PRODUCTION.RAW.CUSTOMERS c

-- Subscription aggregates
LEFT JOIN (
    SELECT 
        customer_id,
        SUM(mrr_amount) AS total_mrr,
        SUM(arr_amount) AS total_arr,
        COUNT(DISTINCT subscription_id) AS product_count,
        LISTAGG(DISTINCT product_name, ', ') AS products_list
    FROM FMG_PRODUCTION.RAW.SUBSCRIPTIONS
    WHERE status = 'Active'
    GROUP BY customer_id
) s ON c.customer_id = s.customer_id

-- User aggregates
LEFT JOIN (
    SELECT 
        customer_id,
        COUNT(DISTINCT user_id) AS total_users,
        COUNT(DISTINCT CASE WHEN user_status = 'Active' THEN user_id END) AS active_users,
        MAX(last_login_date) AS last_login
    FROM FMG_PRODUCTION.RAW.USERS
    GROUP BY customer_id
) u ON c.customer_id = u.customer_id

-- Latest health score
LEFT JOIN (
    SELECT *
    FROM FMG_PRODUCTION.RAW.CUSTOMER_HEALTH_SCORES
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY snapshot_date DESC) = 1
) h ON c.customer_id = h.customer_id

-- Support tickets (last 90 days)
LEFT JOIN (
    SELECT 
        customer_id,
        COUNT(*) AS ticket_count,
        AVG(csat_score) AS avg_csat
    FROM FMG_PRODUCTION.RAW.SUPPORT_TICKETS
    WHERE created_date >= DATEADD('day', -90, CURRENT_TIMESTAMP())
    GROUP BY customer_id
) t ON c.customer_id = t.customer_id

-- Latest NPS
LEFT JOIN (
    SELECT 
        customer_id,
        nps_score AS latest_nps_score,
        nps_category
    FROM FMG_PRODUCTION.RAW.NPS_RESPONSES
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY survey_date DESC) = 1
) n ON c.customer_id = n.customer_id;

-- Verify the dynamic table
SELECT * FROM DT_CUSTOMER_360 LIMIT 10;

-- ============================================================================
-- SECTION 4: CREATE REVENUE ANALYTICS DYNAMIC TABLE
-- ============================================================================

-- Monthly revenue by segment and product
CREATE OR REPLACE DYNAMIC TABLE DT_REVENUE_BY_SEGMENT
    TARGET_LAG = '1 hour'
    WAREHOUSE = FMG_ANALYTICS_M
AS
SELECT 
    DATE_TRUNC('month', s.start_date) AS revenue_month,
    c.segment,
    c.industry,
    s.product_name,
    s.plan_tier,
    COUNT(DISTINCT s.subscription_id) AS subscription_count,
    COUNT(DISTINCT s.customer_id) AS customer_count,
    SUM(s.mrr_amount) AS total_mrr,
    SUM(s.arr_amount) AS total_arr,
    AVG(s.mrr_amount) AS avg_mrr,
    -- Revenue metrics
    SUM(CASE WHEN s.status = 'Active' THEN s.mrr_amount ELSE 0 END) AS active_mrr,
    SUM(CASE WHEN s.status = 'Cancelled' THEN s.mrr_amount ELSE 0 END) AS churned_mrr,
    -- Churn rate (by count)
    ROUND(
        COUNT(CASE WHEN s.status = 'Cancelled' THEN 1 END) * 100.0 / 
        NULLIF(COUNT(*), 0), 2
    ) AS churn_rate_pct,
    CURRENT_TIMESTAMP() AS _refreshed_at
FROM FMG_PRODUCTION.RAW.SUBSCRIPTIONS s
JOIN FMG_PRODUCTION.RAW.CUSTOMERS c ON s.customer_id = c.customer_id
GROUP BY 1, 2, 3, 4, 5;

-- ============================================================================
-- SECTION 5: CREATE CHAINED DYNAMIC TABLES
-- ============================================================================

/*
    Dynamic Tables can read from other Dynamic Tables!
    This creates a transformation chain that Snowflake manages.
*/

-- Executive KPI dashboard (reads from DT_CUSTOMER_360)
CREATE OR REPLACE DYNAMIC TABLE DT_EXECUTIVE_KPIS
    TARGET_LAG = '2 hours'  -- Can be less fresh since it's aggregated
    WAREHOUSE = FMG_ANALYTICS_M
AS
SELECT 
    -- Time dimension
    CURRENT_DATE() AS report_date,
    
    -- Customer Metrics
    COUNT(*) AS total_customers,
    COUNT(CASE WHEN account_status = 'Active' THEN 1 END) AS active_customers,
    COUNT(CASE WHEN account_status = 'Churned' THEN 1 END) AS churned_customers,
    ROUND(COUNT(CASE WHEN account_status = 'Churned' THEN 1 END) * 100.0 / 
        NULLIF(COUNT(*), 0), 2) AS churn_rate_pct,
    
    -- Revenue Metrics
    SUM(total_mrr) AS total_mrr,
    SUM(total_arr) AS total_arr,
    AVG(total_mrr) AS avg_mrr_per_customer,
    
    -- Segment Breakdown
    COUNT(CASE WHEN segment = 'Enterprise' THEN 1 END) AS enterprise_customers,
    COUNT(CASE WHEN segment = 'Mid-Market' THEN 1 END) AS midmarket_customers,
    COUNT(CASE WHEN segment = 'SMB' THEN 1 END) AS smb_customers,
    
    -- Health Metrics
    AVG(overall_health_score) AS avg_health_score,
    COUNT(CASE WHEN churn_risk = 'Critical' THEN 1 END) AS critical_risk_customers,
    COUNT(CASE WHEN churn_risk = 'High' THEN 1 END) AS high_risk_customers,
    
    -- Engagement
    AVG(active_users) AS avg_active_users,
    AVG(days_since_last_login) AS avg_days_since_login,
    
    -- NPS
    AVG(latest_nps_score) AS avg_nps_score,
    COUNT(CASE WHEN nps_category = 'Promoter' THEN 1 END) AS promoter_count,
    COUNT(CASE WHEN nps_category = 'Detractor' THEN 1 END) AS detractor_count,
    
    CURRENT_TIMESTAMP() AS _refreshed_at
FROM DT_CUSTOMER_360  -- Reading from another dynamic table!
WHERE account_status != 'Trial';

-- ============================================================================
-- SECTION 6: CUSTOMER HEALTH DASHBOARD DYNAMIC TABLE
-- ============================================================================

CREATE OR REPLACE DYNAMIC TABLE DT_CUSTOMER_HEALTH_DASHBOARD
    TARGET_LAG = '1 hour'
    WAREHOUSE = FMG_ANALYTICS_M
AS
SELECT 
    c.customer_id,
    c.company_name,
    c.segment,
    c.industry,
    c.csm_owner,
    c.account_status,
    
    -- Latest health metrics
    h.overall_health_score,
    h.usage_score,
    h.engagement_score,
    h.support_score,
    h.payment_score,
    h.churn_risk,
    h.health_trend,
    h.snapshot_date AS health_score_date,
    
    -- Revenue at risk
    COALESCE(s.total_mrr, 0) AS mrr_at_risk,
    
    -- Recent engagement
    COALESCE(u.last_7d_logins, 0) AS logins_last_7d,
    COALESCE(u.last_30d_emails, 0) AS emails_last_30d,
    
    -- Support sentiment
    COALESCE(t.open_tickets, 0) AS open_tickets,
    COALESCE(t.avg_recent_csat, 0) AS recent_csat,
    
    -- Action recommendation
    CASE 
        WHEN h.churn_risk = 'Critical' AND s.total_mrr > 500 THEN 'URGENT: Executive outreach needed'
        WHEN h.churn_risk = 'Critical' THEN 'Schedule CSM call immediately'
        WHEN h.churn_risk = 'High' AND h.health_trend = 'Declining' THEN 'Proactive check-in recommended'
        WHEN h.usage_score < 40 THEN 'Low usage - offer training'
        WHEN u.last_7d_logins = 0 THEN 'No recent activity - re-engagement needed'
        WHEN t.open_tickets > 3 THEN 'Multiple open tickets - prioritize resolution'
        ELSE 'Healthy - maintain relationship'
    END AS recommended_action,
    
    CURRENT_TIMESTAMP() AS _refreshed_at

FROM FMG_PRODUCTION.RAW.CUSTOMERS c

-- Latest health score
LEFT JOIN (
    SELECT *
    FROM FMG_PRODUCTION.RAW.CUSTOMER_HEALTH_SCORES
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY snapshot_date DESC) = 1
) h ON c.customer_id = h.customer_id

-- MRR
LEFT JOIN (
    SELECT customer_id, SUM(mrr_amount) AS total_mrr
    FROM FMG_PRODUCTION.RAW.SUBSCRIPTIONS
    WHERE status = 'Active'
    GROUP BY customer_id
) s ON c.customer_id = s.customer_id

-- Recent usage
LEFT JOIN (
    SELECT 
        customer_id,
        SUM(CASE WHEN usage_date >= DATEADD('day', -7, CURRENT_DATE()) THEN total_logins ELSE 0 END) AS last_7d_logins,
        SUM(CASE WHEN usage_date >= DATEADD('day', -30, CURRENT_DATE()) THEN emails_sent ELSE 0 END) AS last_30d_emails
    FROM FMG_PRODUCTION.RAW.PLATFORM_USAGE_DAILY
    GROUP BY customer_id
) u ON c.customer_id = u.customer_id

-- Support tickets
LEFT JOIN (
    SELECT 
        customer_id,
        COUNT(CASE WHEN status IN ('Open', 'In Progress') THEN 1 END) AS open_tickets,
        AVG(CASE WHEN created_date >= DATEADD('day', -30, CURRENT_DATE()) THEN csat_score END) AS avg_recent_csat
    FROM FMG_PRODUCTION.RAW.SUPPORT_TICKETS
    GROUP BY customer_id
) t ON c.customer_id = t.customer_id

WHERE c.account_status = 'Active'
AND h.churn_risk IN ('Critical', 'High', 'Medium');  -- Focus on at-risk customers

-- ============================================================================
-- SECTION 7: MONITOR DYNAMIC TABLE REFRESH
-- ============================================================================

-- View dynamic table metadata
SHOW DYNAMIC TABLES IN SCHEMA FMG_ANALYTICS.DYNAMIC;

-- Check refresh history
SELECT 
    name,
    state,
    refresh_version,
    refresh_start_time,
    refresh_end_time,
    DATEDIFF('second', refresh_start_time, refresh_end_time) AS refresh_duration_sec,
    rows_inserted,
    rows_deleted,
    rows_updated
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
WHERE name LIKE 'DT_%'
ORDER BY refresh_start_time DESC
LIMIT 20;

-- Check current lag (data freshness)
SELECT 
    name,
    target_lag,
    data_timestamp,
    DATEDIFF('minute', data_timestamp, CURRENT_TIMESTAMP()) AS current_lag_minutes,
    refresh_mode
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES())
WHERE schema_name = 'DYNAMIC';

-- ============================================================================
-- SECTION 8: ALTER DYNAMIC TABLE PROPERTIES
-- ============================================================================

-- Change target lag
-- ALTER DYNAMIC TABLE DT_CUSTOMER_360 SET TARGET_LAG = '30 minutes';

-- Change warehouse
-- ALTER DYNAMIC TABLE DT_CUSTOMER_360 SET WAREHOUSE = FMG_ML_L;

-- Suspend automatic refresh (for maintenance)
-- ALTER DYNAMIC TABLE DT_CUSTOMER_360 SUSPEND;

-- Resume automatic refresh
-- ALTER DYNAMIC TABLE DT_CUSTOMER_360 RESUME;

-- Force immediate refresh
ALTER DYNAMIC TABLE DT_CUSTOMER_360 REFRESH;

-- ============================================================================
-- SECTION 9: COMPARE WITH STREAM/TASK APPROACH
-- ============================================================================

/*
    DYNAMIC TABLE                          STREAM + TASK
    ─────────────────                      ─────────────────
    ✅ Declarative (just SQL)              ❌ Procedural (MERGE logic)
    ✅ Auto-incremental refresh            ❌ Manual stream consumption
    ✅ Built-in freshness SLA              ❌ Schedule-based
    ✅ Simple chaining                     ❌ Complex DAG management
    ✅ Built-in monitoring                 ❌ Custom monitoring needed
    
    ❌ Min 1-minute lag                    ✅ Near real-time possible
    ❌ Limited to SQL transformations      ✅ Can call procedures, APIs
    ❌ Less control over timing            ✅ Full control over execution
*/

-- ============================================================================
-- SECTION 10: SAMPLE QUERIES ON DYNAMIC TABLES
-- ============================================================================

-- Executive KPIs
SELECT * FROM DT_EXECUTIVE_KPIS;

-- Top 10 at-risk customers by MRR
SELECT 
    company_name,
    segment,
    csm_owner,
    overall_health_score,
    churn_risk,
    mrr_at_risk,
    recommended_action
FROM DT_CUSTOMER_HEALTH_DASHBOARD
ORDER BY mrr_at_risk DESC
LIMIT 10;

-- Revenue by segment and product
SELECT 
    segment,
    product_name,
    SUM(total_mrr) AS total_mrr,
    SUM(customer_count) AS customers
FROM DT_REVENUE_BY_SEGMENT
WHERE revenue_month >= DATEADD('month', -6, CURRENT_DATE())
GROUP BY 1, 2
ORDER BY total_mrr DESC;

-- ============================================================================
-- SCRIPT COMPLETE!
-- ============================================================================

SELECT '✅ Dynamic Tables Created!' AS STATUS,
       'Tables will auto-refresh based on TARGET_LAG' AS NOTE,
       CURRENT_TIMESTAMP() AS COMPLETED_AT;

