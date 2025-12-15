/*=============================================================================
  FMG SUITE - LAB 2: GOVERNANCE + FINOPS
  Script 2: Resource Monitoring
  
  Description: Set up credit monitoring and alerts for FMG warehouses
  Prerequisites: Lab 1 completed, warehouses created
  Duration: ~10 minutes
=============================================================================*/

-- ============================================================================
-- SECTION 1: SET CONTEXT
-- ============================================================================

USE ROLE ACCOUNTADMIN;  -- Resource monitors require ACCOUNTADMIN
USE WAREHOUSE FMG_DEV_XS;

-- ============================================================================
-- SECTION 2: UNDERSTANDING RESOURCE MONITORS
-- ============================================================================

/*
    Resource Monitors track and control credit usage:
    
    SCOPE:
    - Account level: Monitors ALL warehouses
    - Warehouse level: Monitors specific warehouse(s)
    
    TRIGGERS (at percentage of quota):
    - NOTIFY: Send alert email to admins
    - SUSPEND: Stop warehouse, queued queries wait
    - SUSPEND_IMMEDIATE: Stop warehouse, kill running queries
    
    SCHEDULE:
    - MONTHLY (default): Resets on 1st of each month
    - WEEKLY: Resets on specified day
    - DAILY: Resets daily (for testing)
    - NEVER: Never resets (fixed quota)
    
    Credit Costs (approximate):
    - Standard: ~$2-3 per credit
    - Enterprise: ~$4-5 per credit  
    - Business Critical: ~$6+ per credit
*/

-- ============================================================================
-- SECTION 3: CREATE ACCOUNT-LEVEL RESOURCE MONITOR
-- ============================================================================

/*
    FMG Account Budget Strategy:
    
    Total Monthly Budget: ~$10,000
    At $3/credit = ~3,333 credits/month
    
    We'll set account monitor slightly higher than sum of warehouse 
    monitors to catch any unexpected usage.
*/

CREATE RESOURCE MONITOR IF NOT EXISTS FMG_ACCOUNT_MONITOR
    WITH 
        CREDIT_QUOTA = 3500  -- Total account credits per month
        FREQUENCY = MONTHLY
        START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 50 PERCENT DO NOTIFY
        ON 75 PERCENT DO NOTIFY
        ON 90 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND;  -- Suspend non-essential warehouses

-- Assign to account level
ALTER ACCOUNT SET RESOURCE_MONITOR = FMG_ACCOUNT_MONITOR;

-- Verify account-level monitor
SHOW RESOURCE MONITORS;

-- ============================================================================
-- SECTION 4: CREATE WAREHOUSE-LEVEL RESOURCE MONITORS
-- ============================================================================

/*
    FMG Warehouse Budgets:
    
    Warehouse          | Monthly Credits | ~Monthly Cost | Purpose
    -------------------|-----------------|---------------|------------------
    FMG_DEV_XS         | 200             | $600          | Development
    FMG_PROD_S         | 500             | $1,500        | Production dashboards
    FMG_ANALYTICS_M    | 1000            | $3,000        | BI and analytics
    FMG_ML_L           | 800             | $2,400        | ML/AI workloads
    FMG_LOAD_M         | 500             | $1,500        | ETL pipelines
*/

-- Development Warehouse Monitor (low budget, tight controls)
CREATE RESOURCE MONITOR IF NOT EXISTS FMG_DEV_MONITOR
    WITH 
        CREDIT_QUOTA = 200
        FREQUENCY = MONTHLY
        START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 75 PERCENT DO NOTIFY
        ON 90 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND;

-- Apply to development warehouse
ALTER WAREHOUSE FMG_DEV_XS SET RESOURCE_MONITOR = FMG_DEV_MONITOR;

-- Production Warehouse Monitor (more critical, warn early)
CREATE RESOURCE MONITOR IF NOT EXISTS FMG_PROD_MONITOR
    WITH 
        CREDIT_QUOTA = 500
        FREQUENCY = MONTHLY
        START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 50 PERCENT DO NOTIFY  -- Early warning for production
        ON 75 PERCENT DO NOTIFY
        ON 90 PERCENT DO NOTIFY
        ON 100 PERCENT DO NOTIFY;  -- Don't suspend production!

-- Apply to production warehouse
ALTER WAREHOUSE FMG_PROD_S SET RESOURCE_MONITOR = FMG_PROD_MONITOR;

-- Analytics Warehouse Monitor (largest budget)
CREATE RESOURCE MONITOR IF NOT EXISTS FMG_ANALYTICS_MONITOR
    WITH 
        CREDIT_QUOTA = 1000
        FREQUENCY = MONTHLY
        START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 50 PERCENT DO NOTIFY
        ON 75 PERCENT DO NOTIFY
        ON 90 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND;  -- Can suspend analytics

-- Apply to analytics warehouse
ALTER WAREHOUSE FMG_ANALYTICS_M SET RESOURCE_MONITOR = FMG_ANALYTICS_MONITOR;

-- ML Warehouse Monitor (expensive, needs oversight)
CREATE RESOURCE MONITOR IF NOT EXISTS FMG_ML_MONITOR
    WITH 
        CREDIT_QUOTA = 800
        FREQUENCY = MONTHLY
        START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 50 PERCENT DO NOTIFY
        ON 75 PERCENT DO NOTIFY
        ON 90 PERCENT DO NOTIFY
        ON 95 PERCENT DO SUSPEND;  -- Suspend before hitting 100%

-- Apply to ML warehouse
ALTER WAREHOUSE FMG_ML_L SET RESOURCE_MONITOR = FMG_ML_MONITOR;

-- ETL/Loading Warehouse Monitor
CREATE RESOURCE MONITOR IF NOT EXISTS FMG_LOAD_MONITOR
    WITH 
        CREDIT_QUOTA = 500
        FREQUENCY = MONTHLY
        START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 75 PERCENT DO NOTIFY
        ON 90 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND_IMMEDIATE;  -- Critical to stop runaway loads

-- Apply to loading warehouse
ALTER WAREHOUSE FMG_LOAD_M SET RESOURCE_MONITOR = FMG_LOAD_MONITOR;

-- ============================================================================
-- SECTION 5: VIEW RESOURCE MONITOR STATUS
-- ============================================================================

-- Show all resource monitors
SHOW RESOURCE MONITORS;

-- Get detailed resource monitor information
SELECT 
    name,
    credit_quota,
    frequency,
    start_time,
    end_time,
    used_credits,
    remaining_credits,
    ROUND(used_credits / credit_quota * 100, 2) AS pct_used,
    suspend_at,
    suspend_immediately_at
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- ============================================================================
-- SECTION 6: CHECK CURRENT CREDIT USAGE
-- ============================================================================

-- View warehouse credit usage for current month
SELECT 
    warehouse_name,
    DATE_TRUNC('day', start_time) AS usage_date,
    SUM(credits_used) AS daily_credits,
    SUM(SUM(credits_used)) OVER (
        PARTITION BY warehouse_name 
        ORDER BY DATE_TRUNC('day', start_time)
    ) AS cumulative_credits
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATE_TRUNC('month', CURRENT_DATE())
AND warehouse_name LIKE 'FMG%'
GROUP BY 1, 2
ORDER BY warehouse_name, usage_date;

-- Summarize by warehouse for current month
SELECT 
    warehouse_name,
    SUM(credits_used) AS total_credits,
    ROUND(SUM(credits_used) * 3, 2) AS estimated_cost_usd,
    MAX(end_time) AS last_usage
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATE_TRUNC('month', CURRENT_DATE())
AND warehouse_name LIKE 'FMG%'
GROUP BY 1
ORDER BY total_credits DESC;

-- ============================================================================
-- SECTION 7: CONFIGURE NOTIFICATION EMAILS
-- ============================================================================

/*
    Resource monitor notifications are sent to users with:
    - ACCOUNTADMIN role
    - Or users explicitly configured via notification integrations
    
    For custom notifications, you can:
    1. Create an email notification integration
    2. Set up webhooks for Slack/Teams
    3. Use tasks to poll and alert
*/

-- Example: Check which users will receive notifications
SHOW USERS;

-- View users with ACCOUNTADMIN access (they receive alerts)
SHOW GRANTS OF ROLE ACCOUNTADMIN;

-- ============================================================================
-- SECTION 8: ADJUST MONITORS (Examples)
-- ============================================================================

-- Increase quota mid-month if needed (won't reset usage)
-- ALTER RESOURCE MONITOR FMG_ANALYTICS_MONITOR SET CREDIT_QUOTA = 1200;

-- Add a new trigger
-- ALTER RESOURCE MONITOR FMG_PROD_MONITOR 
--     SET TRIGGERS ON 60 PERCENT DO NOTIFY;

-- Change schedule to weekly (for testing)
-- ALTER RESOURCE MONITOR FMG_DEV_MONITOR 
--     SET FREQUENCY = WEEKLY
--     START_TIMESTAMP = IMMEDIATELY;

-- Remove a resource monitor from warehouse
-- ALTER WAREHOUSE FMG_DEV_XS UNSET RESOURCE_MONITOR;

-- Drop a resource monitor (must unassign first)
-- DROP RESOURCE MONITOR FMG_DEV_MONITOR;

-- ============================================================================
-- SECTION 9: CREATE A CREDIT USAGE ALERT VIEW
-- ============================================================================

-- Create a view for monitoring credit usage across all warehouses
CREATE OR REPLACE VIEW FMG_PRODUCTION.GOVERNANCE.V_WAREHOUSE_CREDIT_STATUS AS
WITH monthly_usage AS (
    SELECT 
        warehouse_name,
        SUM(credits_used) AS month_to_date_credits
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE start_time >= DATE_TRUNC('month', CURRENT_DATE())
    GROUP BY 1
),
monitor_config AS (
    SELECT 
        'FMG_DEV_XS' AS warehouse_name, 200 AS credit_quota UNION ALL
        SELECT 'FMG_PROD_S', 500 UNION ALL
        SELECT 'FMG_ANALYTICS_M', 1000 UNION ALL
        SELECT 'FMG_ML_L', 800 UNION ALL
        SELECT 'FMG_LOAD_M', 500
)
SELECT 
    m.warehouse_name,
    c.credit_quota,
    COALESCE(u.month_to_date_credits, 0) AS used_credits,
    c.credit_quota - COALESCE(u.month_to_date_credits, 0) AS remaining_credits,
    ROUND(COALESCE(u.month_to_date_credits, 0) / c.credit_quota * 100, 2) AS pct_used,
    CASE 
        WHEN COALESCE(u.month_to_date_credits, 0) / c.credit_quota >= 0.9 THEN '🔴 CRITICAL'
        WHEN COALESCE(u.month_to_date_credits, 0) / c.credit_quota >= 0.75 THEN '🟡 WARNING'
        WHEN COALESCE(u.month_to_date_credits, 0) / c.credit_quota >= 0.5 THEN '🟢 ON TRACK'
        ELSE '✅ HEALTHY'
    END AS status,
    -- Projected end-of-month usage
    ROUND(COALESCE(u.month_to_date_credits, 0) / GREATEST(DAYOFMONTH(CURRENT_DATE()), 1) * 
        DAYOFMONTH(LAST_DAY(CURRENT_DATE())), 0) AS projected_monthly_credits,
    CASE 
        WHEN ROUND(COALESCE(u.month_to_date_credits, 0) / GREATEST(DAYOFMONTH(CURRENT_DATE()), 1) * 
            DAYOFMONTH(LAST_DAY(CURRENT_DATE())), 0) > c.credit_quota 
        THEN '⚠️ OVER BUDGET'
        ELSE '✅ WITHIN BUDGET'
    END AS projection_status
FROM monitor_config m
LEFT JOIN monthly_usage u ON m.warehouse_name = u.warehouse_name;

-- Run the credit status check
SELECT * FROM FMG_PRODUCTION.GOVERNANCE.V_WAREHOUSE_CREDIT_STATUS;

-- ============================================================================
-- SECTION 10: BEST PRACTICES SUMMARY
-- ============================================================================

/*
    FMG RESOURCE MONITORING BEST PRACTICES:
    
    1. LAYERED MONITORING
       - Account-level monitor as a safety net
       - Warehouse-level monitors for granular control
       - Set account quota > sum of warehouse quotas
    
    2. APPROPRIATE TRIGGERS
       - Production: NOTIFY only, never suspend
       - Development: Can suspend at 100%
       - ETL: SUSPEND_IMMEDIATE to stop runaway jobs
       - ML: Suspend at 95% to preserve buffer
    
    3. EARLY WARNINGS
       - Start notifications at 50% for visibility
       - 75% trigger means investigate
       - 90% trigger means take action
    
    4. REGULAR REVIEW
       - Check usage weekly during month
       - Adjust quotas based on actual usage
       - Review suspended warehouses immediately
    
    5. DOCUMENTATION
       - Comment on why quotas are set
       - Document escalation procedures
       - Track quota changes in version control
*/

-- ============================================================================
-- SCRIPT COMPLETE!
-- ============================================================================

SELECT '✅ Resource Monitoring Setup Complete!' AS STATUS,
       (SELECT COUNT(*) FROM TABLE(RESULT_SCAN(LAST_QUERY_ID(-8)))) AS MONITORS_CREATED,
       CURRENT_TIMESTAMP() AS COMPLETED_AT;

