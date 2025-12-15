/*=============================================================================
  FMG SUITE - LAB 2: GOVERNANCE + FINOPS
  Script 3: Budgets
  
  Description: Implement cost controls and budget tracking for FMG
  Prerequisites: Resource monitors created
  Duration: ~10 minutes
=============================================================================*/

-- ============================================================================
-- SECTION 1: SET CONTEXT
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE FMG_DEV_XS;

-- ============================================================================
-- SECTION 2: UNDERSTANDING SNOWFLAKE COSTS
-- ============================================================================

/*
    Snowflake Billing Components:
    
    1. COMPUTE (Credits)
       - Virtual Warehouse usage
       - Billed per-second (1-minute minimum)
       - Size determines credits/hour
    
    2. STORAGE ($/TB/month)
       - Active storage
       - Time Travel storage
       - Fail-safe storage
    
    3. SERVERLESS (Credits)
       - Snowpipe (continuous loading)
       - Tasks (scheduled SQL)
       - Materialized Views maintenance
       - Search Optimization
       - Query Acceleration
    
    4. DATA TRANSFER ($/TB)
       - Cross-region replication
       - Egress to other clouds
       - Data sharing (if provider pays)
    
    5. CORTEX AI (Credits)
       - LLM function calls
       - ML model inference
       - Cortex Search
*/

-- ============================================================================
-- SECTION 3: CREATE BUDGET TRACKING VIEWS
-- ============================================================================

/*
    While Snowflake Budgets feature may require specific enablement,
    we can build our own budget tracking using ACCOUNT_USAGE views.
*/

USE DATABASE FMG_PRODUCTION;
USE SCHEMA GOVERNANCE;

-- FMG Monthly Budget Configuration Table
CREATE OR REPLACE TABLE BUDGET_CONFIG (
    budget_category VARCHAR(50) PRIMARY KEY,
    monthly_budget_credits DECIMAL(10,2),
    monthly_budget_usd DECIMAL(10,2),
    cost_per_credit DECIMAL(5,2) DEFAULT 3.00,
    alert_threshold_pct DECIMAL(5,2) DEFAULT 80,
    owner_email VARCHAR(200),
    notes VARCHAR(500),
    last_updated TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Insert FMG budget allocations
INSERT INTO BUDGET_CONFIG (budget_category, monthly_budget_credits, monthly_budget_usd, owner_email, notes) VALUES
    ('COMPUTE_DEV', 200, 600, 'engineering@fmgsuite.com', 'Development warehouse budget'),
    ('COMPUTE_PROD', 500, 1500, 'operations@fmgsuite.com', 'Production dashboard warehouse'),
    ('COMPUTE_ANALYTICS', 1000, 3000, 'analytics@fmgsuite.com', 'BI and analytics workloads'),
    ('COMPUTE_ML', 800, 2400, 'datascience@fmgsuite.com', 'ML/AI and Cortex workloads'),
    ('COMPUTE_ETL', 500, 1500, 'engineering@fmgsuite.com', 'Data loading and ETL'),
    ('STORAGE', 500, 1500, 'engineering@fmgsuite.com', 'Data storage (estimated)'),
    ('SERVERLESS', 200, 600, 'engineering@fmgsuite.com', 'Tasks, pipes, and serverless features'),
    ('CORTEX_AI', 300, 900, 'datascience@fmgsuite.com', 'Cortex LLM and AI features');

-- View budget configuration
SELECT * FROM BUDGET_CONFIG ORDER BY monthly_budget_usd DESC;

-- ============================================================================
-- SECTION 4: CREATE BUDGET vs ACTUAL DASHBOARD
-- ============================================================================

-- Comprehensive budget tracking view
CREATE OR REPLACE VIEW V_BUDGET_DASHBOARD AS
WITH 
-- Warehouse compute usage
warehouse_usage AS (
    SELECT 
        CASE 
            WHEN warehouse_name = 'FMG_DEV_XS' THEN 'COMPUTE_DEV'
            WHEN warehouse_name = 'FMG_PROD_S' THEN 'COMPUTE_PROD'
            WHEN warehouse_name = 'FMG_ANALYTICS_M' THEN 'COMPUTE_ANALYTICS'
            WHEN warehouse_name = 'FMG_ML_L' THEN 'COMPUTE_ML'
            WHEN warehouse_name = 'FMG_LOAD_M' THEN 'COMPUTE_ETL'
            ELSE 'OTHER_COMPUTE'
        END AS budget_category,
        SUM(credits_used) AS credits_used
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE start_time >= DATE_TRUNC('month', CURRENT_DATE())
    AND warehouse_name LIKE 'FMG%'
    GROUP BY 1
),
-- Storage usage (approximate - actual billing may vary)
storage_usage AS (
    SELECT 
        'STORAGE' AS budget_category,
        (AVG(STORAGE_BYTES) + AVG(STAGE_BYTES) + AVG(FAILSAFE_BYTES)) / POWER(1024, 4) * 23 AS credits_used
    FROM SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE
    WHERE usage_date >= DATE_TRUNC('month', CURRENT_DATE())
),
-- Serverless usage
serverless_usage AS (
    SELECT 
        'SERVERLESS' AS budget_category,
        SUM(credits_used) AS credits_used
    FROM SNOWFLAKE.ACCOUNT_USAGE.SERVERLESS_TASK_HISTORY
    WHERE start_time >= DATE_TRUNC('month', CURRENT_DATE())
),
-- Combine all usage
all_usage AS (
    SELECT * FROM warehouse_usage
    UNION ALL SELECT * FROM storage_usage
    UNION ALL SELECT * FROM serverless_usage
),
-- Calculate days in month for projection
month_info AS (
    SELECT 
        DAYOFMONTH(CURRENT_DATE()) AS days_elapsed,
        DAYOFMONTH(LAST_DAY(CURRENT_DATE())) AS days_in_month
)
SELECT 
    b.budget_category,
    b.monthly_budget_credits,
    b.monthly_budget_usd,
    COALESCE(u.credits_used, 0) AS mtd_credits_used,
    ROUND(COALESCE(u.credits_used, 0) * b.cost_per_credit, 2) AS mtd_cost_usd,
    b.monthly_budget_credits - COALESCE(u.credits_used, 0) AS remaining_credits,
    ROUND((b.monthly_budget_usd - COALESCE(u.credits_used, 0) * b.cost_per_credit), 2) AS remaining_budget_usd,
    ROUND(COALESCE(u.credits_used, 0) / b.monthly_budget_credits * 100, 2) AS pct_budget_used,
    -- Projection
    ROUND(COALESCE(u.credits_used, 0) / GREATEST(m.days_elapsed, 1) * m.days_in_month, 2) AS projected_credits,
    ROUND(COALESCE(u.credits_used, 0) / GREATEST(m.days_elapsed, 1) * m.days_in_month * b.cost_per_credit, 2) AS projected_cost_usd,
    -- Status
    CASE 
        WHEN COALESCE(u.credits_used, 0) / b.monthly_budget_credits >= 1.0 THEN '🔴 OVER BUDGET'
        WHEN COALESCE(u.credits_used, 0) / b.monthly_budget_credits >= b.alert_threshold_pct/100 THEN '🟡 AT RISK'
        WHEN COALESCE(u.credits_used, 0) / GREATEST(m.days_elapsed, 1) * m.days_in_month > b.monthly_budget_credits THEN '⚠️ PROJECTED OVER'
        ELSE '🟢 ON TRACK'
    END AS budget_status,
    b.owner_email
FROM BUDGET_CONFIG b
LEFT JOIN all_usage u ON b.budget_category = u.budget_category
CROSS JOIN month_info m
ORDER BY b.monthly_budget_usd DESC;

-- Run budget dashboard
SELECT * FROM V_BUDGET_DASHBOARD;

-- ============================================================================
-- SECTION 5: HISTORICAL SPEND TRACKING
-- ============================================================================

-- Monthly spend history by category
CREATE OR REPLACE VIEW V_MONTHLY_SPEND_HISTORY AS
WITH monthly_compute AS (
    SELECT 
        DATE_TRUNC('month', start_time) AS month,
        CASE 
            WHEN warehouse_name = 'FMG_DEV_XS' THEN 'COMPUTE_DEV'
            WHEN warehouse_name = 'FMG_PROD_S' THEN 'COMPUTE_PROD'
            WHEN warehouse_name = 'FMG_ANALYTICS_M' THEN 'COMPUTE_ANALYTICS'
            WHEN warehouse_name = 'FMG_ML_L' THEN 'COMPUTE_ML'
            WHEN warehouse_name = 'FMG_LOAD_M' THEN 'COMPUTE_ETL'
            ELSE 'OTHER_COMPUTE'
        END AS category,
        SUM(credits_used) AS credits_used
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE warehouse_name LIKE 'FMG%'
    GROUP BY 1, 2
)
SELECT 
    month,
    category,
    credits_used,
    ROUND(credits_used * 3, 2) AS estimated_cost_usd,
    LAG(credits_used) OVER (PARTITION BY category ORDER BY month) AS prev_month_credits,
    ROUND((credits_used - LAG(credits_used) OVER (PARTITION BY category ORDER BY month)) / 
        NULLIF(LAG(credits_used) OVER (PARTITION BY category ORDER BY month), 0) * 100, 2) AS pct_change
FROM monthly_compute
ORDER BY month DESC, credits_used DESC;

-- View spend history
SELECT * FROM V_MONTHLY_SPEND_HISTORY LIMIT 50;

-- ============================================================================
-- SECTION 6: COST ANOMALY DETECTION
-- ============================================================================

-- Detect unusual spending patterns
CREATE OR REPLACE VIEW V_COST_ANOMALIES AS
WITH daily_usage AS (
    SELECT 
        DATE_TRUNC('day', start_time) AS usage_date,
        warehouse_name,
        SUM(credits_used) AS daily_credits
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE start_time >= DATEADD('day', -30, CURRENT_DATE())
    AND warehouse_name LIKE 'FMG%'
    GROUP BY 1, 2
),
stats AS (
    SELECT 
        warehouse_name,
        AVG(daily_credits) AS avg_daily_credits,
        STDDEV(daily_credits) AS stddev_daily_credits
    FROM daily_usage
    WHERE usage_date < CURRENT_DATE()  -- Exclude today (incomplete)
    GROUP BY 1
)
SELECT 
    d.usage_date,
    d.warehouse_name,
    ROUND(d.daily_credits, 2) AS daily_credits,
    ROUND(s.avg_daily_credits, 2) AS avg_daily_credits,
    ROUND(s.stddev_daily_credits, 2) AS stddev_credits,
    ROUND((d.daily_credits - s.avg_daily_credits) / NULLIF(s.stddev_daily_credits, 0), 2) AS z_score,
    CASE 
        WHEN ABS((d.daily_credits - s.avg_daily_credits) / NULLIF(s.stddev_daily_credits, 0)) > 3 THEN '🔴 EXTREME'
        WHEN ABS((d.daily_credits - s.avg_daily_credits) / NULLIF(s.stddev_daily_credits, 0)) > 2 THEN '🟡 HIGH'
        ELSE '🟢 NORMAL'
    END AS anomaly_level
FROM daily_usage d
JOIN stats s ON d.warehouse_name = s.warehouse_name
WHERE d.daily_credits > s.avg_daily_credits + s.stddev_daily_credits  -- Only flag over-usage
ORDER BY d.usage_date DESC, z_score DESC;

-- View anomalies
SELECT * FROM V_COST_ANOMALIES WHERE anomaly_level != '🟢 NORMAL';

-- ============================================================================
-- SECTION 7: TEAM-BASED COST ALLOCATION
-- ============================================================================

-- Cost allocation by team/department
CREATE OR REPLACE VIEW V_COST_BY_TEAM AS
WITH team_mapping AS (
    SELECT 'FMG_DEV_XS' AS warehouse_name, 'Engineering' AS team UNION ALL
    SELECT 'FMG_PROD_S', 'Operations' UNION ALL
    SELECT 'FMG_ANALYTICS_M', 'Analytics' UNION ALL
    SELECT 'FMG_ML_L', 'Data Science' UNION ALL
    SELECT 'FMG_LOAD_M', 'Engineering'
)
SELECT 
    DATE_TRUNC('month', h.start_time) AS month,
    t.team,
    SUM(h.credits_used) AS total_credits,
    ROUND(SUM(h.credits_used) * 3, 2) AS total_cost_usd,
    ROUND(SUM(h.credits_used) / SUM(SUM(h.credits_used)) OVER (PARTITION BY DATE_TRUNC('month', h.start_time)) * 100, 2) AS pct_of_total
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY h
JOIN team_mapping t ON h.warehouse_name = t.warehouse_name
GROUP BY 1, 2
ORDER BY month DESC, total_credits DESC;

-- View team costs
SELECT * FROM V_COST_BY_TEAM;

-- ============================================================================
-- SECTION 8: SNOWFLAKE BUDGETS FEATURE (if available)
-- ============================================================================

/*
    Snowflake Budgets is a native feature for cost management.
    If enabled in your account, you can use:
    
    -- Create a budget
    CREATE BUDGET FMG_MONTHLY_BUDGET
        WITH 
            SPEND_LIMIT = 10000  -- USD
            FREQUENCY = MONTHLY
        NOTIFY_USERS = ('user1', 'user2');
    
    -- View budgets
    SHOW BUDGETS;
    
    -- Check budget status
    SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.BUDGETS;
    
    Note: Budgets feature availability depends on your Snowflake edition
    and account configuration. Contact your Snowflake representative.
*/

-- ============================================================================
-- SECTION 9: CREATE BUDGET ALERTS TASK (Automated)
-- ============================================================================

-- Create a task that checks budgets daily and logs alerts
CREATE OR REPLACE TASK FMG_PRODUCTION.GOVERNANCE.BUDGET_ALERT_CHECK
    WAREHOUSE = FMG_DEV_XS
    SCHEDULE = 'USING CRON 0 8 * * * America/Los_Angeles'  -- Daily at 8 AM PT
AS
INSERT INTO FMG_PRODUCTION.GOVERNANCE.BUDGET_ALERTS (alert_date, budget_category, pct_used, status, alert_message)
SELECT 
    CURRENT_DATE(),
    budget_category,
    pct_budget_used,
    budget_status,
    budget_category || ' is at ' || pct_budget_used || '% of monthly budget. Status: ' || budget_status
FROM FMG_PRODUCTION.GOVERNANCE.V_BUDGET_DASHBOARD
WHERE pct_budget_used >= 75  -- Only alert when over 75%
OR budget_status LIKE '%OVER%';

-- Create the alerts table
CREATE TABLE IF NOT EXISTS FMG_PRODUCTION.GOVERNANCE.BUDGET_ALERTS (
    alert_id INTEGER AUTOINCREMENT,
    alert_date DATE,
    budget_category VARCHAR(50),
    pct_used DECIMAL(5,2),
    status VARCHAR(50),
    alert_message VARCHAR(500),
    acknowledged BOOLEAN DEFAULT FALSE,
    acknowledged_by VARCHAR(100),
    acknowledged_at TIMESTAMP_NTZ
);

-- Note: Task needs to be resumed to run
-- ALTER TASK FMG_PRODUCTION.GOVERNANCE.BUDGET_ALERT_CHECK RESUME;

-- ============================================================================
-- SECTION 10: BUDGET BEST PRACTICES
-- ============================================================================

/*
    FMG BUDGET MANAGEMENT BEST PRACTICES:
    
    1. SET REALISTIC BUDGETS
       - Base on historical usage + growth
       - Include buffer for unexpected spikes
       - Review and adjust quarterly
    
    2. ALLOCATE BY TEAM
       - Each team owns their warehouse costs
       - Creates accountability and optimization incentive
       - Enables showback/chargeback if needed
    
    3. MONITOR PROACTIVELY
       - Daily checks on budget status
       - Anomaly detection for early warning
       - Weekly team reports
    
    4. AUTOMATE ALERTS
       - Email notifications at thresholds
       - Slack/Teams integration for real-time alerts
       - Escalation procedures for critical overage
    
    5. OPTIMIZE CONTINUOUSLY
       - Review expensive queries monthly
       - Right-size warehouses based on usage
       - Use auto-suspend aggressively
       - Consider Query Acceleration for spiky workloads
*/

-- ============================================================================
-- SCRIPT COMPLETE!
-- ============================================================================

SELECT '✅ Budget Setup Complete!' AS STATUS,
       'Review V_BUDGET_DASHBOARD for current status' AS NEXT_STEP,
       CURRENT_TIMESTAMP() AS COMPLETED_AT;

