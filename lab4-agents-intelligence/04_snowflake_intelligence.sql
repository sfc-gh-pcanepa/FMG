/*=============================================================================
  FMG SUITE - LAB 4: SNOWFLAKE AGENTS AND INTELLIGENCE
  Script 4: Snowflake Intelligence
  
  Description: AI-powered insights, forecasting, and anomaly detection
  Prerequisites: FMG data loaded, Cortex enabled
  Duration: ~10 minutes
=============================================================================*/

-- ============================================================================
-- SECTION 1: SET CONTEXT
-- ============================================================================

USE ROLE FMG_ADMIN;
USE WAREHOUSE FMG_ML_L;
USE DATABASE FMG_ANALYTICS;
USE SCHEMA AI;

-- ============================================================================
-- SECTION 2: UNDERSTANDING SNOWFLAKE INTELLIGENCE
-- ============================================================================

/*
    Snowflake Intelligence includes:
    
    1. ML FUNCTIONS
       - FORECAST: Time series forecasting
       - ANOMALY_DETECTION: Find outliers in data
       - CONTRIBUTION_EXPLORER: Understand drivers of change
       - TOP_INSIGHTS: Automatically find interesting patterns
    
    2. UNIVERSAL SEARCH
       - Find data across your account
       - Search by name or content
       - Understand data relationships
    
    3. DATA QUALITY MONITORING
       - Track data freshness
       - Monitor schema changes
       - Detect data drift
    
    4. TRUST CENTER
       - Unified governance dashboard
       - Policy compliance monitoring
       - Access audit visualization
*/

-- ============================================================================
-- SECTION 3: TIME SERIES FORECASTING
-- ============================================================================

-- Prepare MRR data for forecasting
CREATE OR REPLACE TABLE MRR_HISTORY AS
WITH monthly_mrr AS (
    SELECT 
        DATE_TRUNC('month', s.start_date) AS month,
        SUM(s.mrr_amount) AS total_mrr,
        COUNT(DISTINCT s.customer_id) AS customer_count
    FROM FMG_PRODUCTION.RAW.SUBSCRIPTIONS s
    WHERE s.status = 'Active'
    AND s.start_date >= DATEADD('month', -24, CURRENT_DATE())
    GROUP BY 1
)
SELECT 
    month,
    total_mrr,
    customer_count,
    total_mrr / NULLIF(customer_count, 0) AS arpu
FROM monthly_mrr
ORDER BY month;

-- View the historical data
SELECT * FROM MRR_HISTORY ORDER BY month;

-- Create a forecasting model
CREATE OR REPLACE SNOWFLAKE.ML.FORECAST FMG_MRR_FORECAST(
    INPUT_DATA => SYSTEM$REFERENCE('TABLE', 'MRR_HISTORY'),
    TIMESTAMP_COLNAME => 'MONTH',
    TARGET_COLNAME => 'TOTAL_MRR'
);

-- Generate forecast for next 6 months
CALL FMG_MRR_FORECAST!FORECAST(FORECASTING_PERIODS => 6);

-- View the forecast results
SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- Combine historical and forecast data
CREATE OR REPLACE VIEW V_MRR_FORECAST AS
SELECT 
    month,
    total_mrr AS actual_mrr,
    NULL AS forecast_mrr,
    NULL AS lower_bound,
    NULL AS upper_bound
FROM MRR_HISTORY

UNION ALL

SELECT 
    ts AS month,
    NULL AS actual_mrr,
    forecast AS forecast_mrr,
    lower_bound,
    upper_bound
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
ORDER BY month;

-- ============================================================================
-- SECTION 4: ANOMALY DETECTION
-- ============================================================================

-- Prepare daily usage data for anomaly detection
CREATE OR REPLACE TABLE DAILY_USAGE_METRICS AS
SELECT 
    usage_date,
    COUNT(DISTINCT customer_id) AS active_customers,
    SUM(emails_sent) AS total_emails,
    SUM(total_logins) AS total_logins,
    AVG(session_duration_minutes) AS avg_session_minutes
FROM FMG_PRODUCTION.RAW.PLATFORM_USAGE_DAILY
WHERE usage_date >= DATEADD('day', -90, CURRENT_DATE())
GROUP BY usage_date
ORDER BY usage_date;

-- Create anomaly detection model for logins
CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION FMG_LOGIN_ANOMALIES(
    INPUT_DATA => SYSTEM$REFERENCE('TABLE', 'DAILY_USAGE_METRICS'),
    TIMESTAMP_COLNAME => 'USAGE_DATE',
    TARGET_COLNAME => 'TOTAL_LOGINS',
    LABEL_COLNAME => ''  -- Unsupervised
);

-- Detect anomalies
CALL FMG_LOGIN_ANOMALIES!DETECT_ANOMALIES(
    INPUT_DATA => SYSTEM$REFERENCE('TABLE', 'DAILY_USAGE_METRICS'),
    TIMESTAMP_COLNAME => 'USAGE_DATE',
    TARGET_COLNAME => 'TOTAL_LOGINS'
);

-- View anomalies
SELECT 
    ts AS usage_date,
    y AS total_logins,
    forecast,
    is_anomaly,
    percentile,
    distance
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE is_anomaly = TRUE
ORDER BY ts DESC;

-- ============================================================================
-- SECTION 5: CONTRIBUTION EXPLORER
-- ============================================================================

/*
    Contribution Explorer helps answer:
    "Why did metric X change?"
    
    It identifies which dimensions contributed most to a change.
*/

-- Compare this month vs last month MRR by dimensions
CREATE OR REPLACE VIEW V_MRR_CHANGE_ANALYSIS AS
WITH current_month AS (
    SELECT 
        c.segment,
        c.industry,
        s.product_name,
        SUM(s.mrr_amount) AS mrr
    FROM FMG_PRODUCTION.RAW.SUBSCRIPTIONS s
    JOIN FMG_PRODUCTION.RAW.CUSTOMERS c ON s.customer_id = c.customer_id
    WHERE s.status = 'Active'
    AND s.start_date <= CURRENT_DATE()
    GROUP BY 1, 2, 3
),
prior_month AS (
    SELECT 
        c.segment,
        c.industry,
        s.product_name,
        SUM(s.mrr_amount) AS mrr
    FROM FMG_PRODUCTION.RAW.SUBSCRIPTIONS s
    JOIN FMG_PRODUCTION.RAW.CUSTOMERS c ON s.customer_id = c.customer_id
    WHERE s.status = 'Active'
    AND s.start_date <= DATEADD('month', -1, CURRENT_DATE())
    GROUP BY 1, 2, 3
)
SELECT 
    COALESCE(c.segment, p.segment) AS segment,
    COALESCE(c.industry, p.industry) AS industry,
    COALESCE(c.product_name, p.product_name) AS product_name,
    COALESCE(p.mrr, 0) AS prior_mrr,
    COALESCE(c.mrr, 0) AS current_mrr,
    COALESCE(c.mrr, 0) - COALESCE(p.mrr, 0) AS mrr_change,
    ROUND((COALESCE(c.mrr, 0) - COALESCE(p.mrr, 0)) / NULLIF(COALESCE(p.mrr, 0), 0) * 100, 2) AS pct_change
FROM current_month c
FULL OUTER JOIN prior_month p 
    ON c.segment = p.segment 
    AND c.industry = p.industry 
    AND c.product_name = p.product_name
ORDER BY ABS(mrr_change) DESC;

-- View the biggest contributors to MRR change
SELECT * FROM V_MRR_CHANGE_ANALYSIS 
WHERE ABS(mrr_change) > 0
ORDER BY ABS(mrr_change) DESC
LIMIT 10;

-- ============================================================================
-- SECTION 6: AUTOMATED INSIGHTS
-- ============================================================================

-- Create a view that generates AI-powered insights on demand
CREATE OR REPLACE VIEW V_AI_INSIGHTS_DASHBOARD AS
WITH key_metrics AS (
    SELECT 
        (SELECT COUNT(*) FROM FMG_PRODUCTION.RAW.CUSTOMERS WHERE account_status = 'Active') AS active_customers,
        (SELECT COUNT(*) FROM FMG_PRODUCTION.RAW.CUSTOMERS WHERE account_status = 'Churned' 
            AND _loaded_at >= DATEADD('day', -30, CURRENT_TIMESTAMP())) AS recent_churns,
        (SELECT SUM(mrr_amount) FROM FMG_PRODUCTION.RAW.SUBSCRIPTIONS WHERE status = 'Active') AS total_mrr,
        (SELECT AVG(overall_health_score) FROM FMG_PRODUCTION.RAW.CUSTOMER_HEALTH_SCORES
            WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM FMG_PRODUCTION.RAW.CUSTOMER_HEALTH_SCORES)) AS avg_health_score,
        (SELECT AVG(nps_score) FROM FMG_PRODUCTION.RAW.NPS_RESPONSES 
            WHERE survey_date >= DATEADD('day', -30, CURRENT_DATE())) AS avg_nps,
        (SELECT COUNT(*) FROM FMG_PRODUCTION.RAW.SUPPORT_TICKETS 
            WHERE status = 'Open') AS open_tickets
)
SELECT 
    active_customers,
    recent_churns,
    total_mrr,
    avg_health_score,
    avg_nps,
    open_tickets,
    SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-7b',
        'Based on these FMG metrics, provide 3 brief bullet-point insights:
        - Active customers: ' || active_customers || '
        - Recent churns (30 days): ' || recent_churns || '
        - Total MRR: $' || ROUND(total_mrr, 0) || '
        - Avg health score: ' || ROUND(avg_health_score, 1) || '/100
        - Avg NPS (30 days): ' || ROUND(avg_nps, 1) || '
        - Open support tickets: ' || open_tickets || '
        
        Focus on actionable observations. Be very concise.'
    ) AS ai_insights
FROM key_metrics;

-- View AI insights
SELECT * FROM V_AI_INSIGHTS_DASHBOARD;

-- ============================================================================
-- SECTION 7: DOCUMENT AI (Preview)
-- ============================================================================

/*
    Document AI extracts structured data from documents:
    - Invoices
    - Contracts
    - Forms
    - Images with text
    
    FMG Use Cases:
    - Process customer contracts
    - Extract data from compliance documents
    - Automate invoice processing
    
    Note: Requires specific setup and file staging
*/

-- Example structure (actual implementation requires staged files)
-- CREATE OR REPLACE TABLE EXTRACTED_DOCUMENTS (
--     document_id VARCHAR,
--     document_type VARCHAR,
--     uploaded_at TIMESTAMP_NTZ,
--     extracted_data VARIANT,
--     confidence_score FLOAT
-- );

-- ============================================================================
-- SECTION 8: CREATE INTELLIGENCE DASHBOARD
-- ============================================================================

-- Unified intelligence dashboard
CREATE OR REPLACE VIEW V_INTELLIGENCE_DASHBOARD AS
SELECT 
    'Key Metrics' AS section,
    OBJECT_CONSTRUCT(
        'active_customers', (SELECT COUNT(*) FROM FMG_PRODUCTION.RAW.CUSTOMERS WHERE account_status = 'Active'),
        'total_mrr', (SELECT SUM(mrr_amount) FROM FMG_PRODUCTION.RAW.SUBSCRIPTIONS WHERE status = 'Active'),
        'avg_health_score', (SELECT AVG(overall_health_score) FROM FMG_PRODUCTION.RAW.CUSTOMER_HEALTH_SCORES 
            WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM FMG_PRODUCTION.RAW.CUSTOMER_HEALTH_SCORES)),
        'at_risk_customers', (SELECT COUNT(DISTINCT customer_id) FROM FMG_PRODUCTION.RAW.CUSTOMER_HEALTH_SCORES 
            WHERE churn_risk IN ('High', 'Critical') 
            AND snapshot_date = (SELECT MAX(snapshot_date) FROM FMG_PRODUCTION.RAW.CUSTOMER_HEALTH_SCORES))
    ) AS data

UNION ALL

SELECT 
    'Segment Distribution',
    (SELECT OBJECT_AGG(segment, customer_count) FROM (
        SELECT segment, COUNT(*) AS customer_count 
        FROM FMG_PRODUCTION.RAW.CUSTOMERS 
        WHERE account_status = 'Active'
        GROUP BY segment
    ))

UNION ALL

SELECT 
    'Product Revenue',
    (SELECT OBJECT_AGG(product_name, mrr) FROM (
        SELECT product_name, SUM(mrr_amount) AS mrr 
        FROM FMG_PRODUCTION.RAW.SUBSCRIPTIONS 
        WHERE status = 'Active'
        GROUP BY product_name
    ))

UNION ALL

SELECT 
    'Health Distribution',
    (SELECT OBJECT_AGG(churn_risk, customer_count) FROM (
        SELECT churn_risk, COUNT(DISTINCT customer_id) AS customer_count 
        FROM FMG_PRODUCTION.RAW.CUSTOMER_HEALTH_SCORES
        WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM FMG_PRODUCTION.RAW.CUSTOMER_HEALTH_SCORES)
        GROUP BY churn_risk
    ));

-- View the dashboard
SELECT * FROM V_INTELLIGENCE_DASHBOARD;

-- ============================================================================
-- SECTION 9: SCHEDULED INTELLIGENCE REPORTS
-- ============================================================================

-- Create a task to generate weekly intelligence reports
CREATE OR REPLACE TASK WEEKLY_INTELLIGENCE_REPORT
    WAREHOUSE = FMG_ML_L
    SCHEDULE = 'USING CRON 0 8 * * 1 America/Los_Angeles'  -- Monday 8 AM
AS
INSERT INTO AI_INTELLIGENCE_REPORTS (report_date, report_type, report_content)
SELECT 
    CURRENT_DATE(),
    'Weekly Intelligence Report',
    SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large',
        'Generate a weekly business intelligence report for FMG leadership based on these metrics:

' || (SELECT LISTAGG(section || ': ' || data::VARCHAR, '\n') FROM V_INTELLIGENCE_DASHBOARD) || '

Include:
1. Executive summary (2-3 sentences)
2. Key wins this week
3. Areas of concern
4. Recommended focus areas
5. Week-over-week trends to watch

Keep it concise and actionable.'
    );

-- Create the reports table
CREATE OR REPLACE TABLE AI_INTELLIGENCE_REPORTS (
    report_id INTEGER AUTOINCREMENT,
    report_date DATE,
    report_type VARCHAR,
    report_content VARCHAR,
    generated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Note: Resume task to activate
-- ALTER TASK WEEKLY_INTELLIGENCE_REPORT RESUME;

-- ============================================================================
-- SECTION 10: WRAP UP - FMG AI ROADMAP
-- ============================================================================

/*
    FMG SNOWFLAKE AI ROADMAP:
    
    PHASE 1: Foundation (Weeks 1-4)
    ✅ Set up Cortex access
    ✅ Create sentiment analysis on feedback
    ✅ Build semantic search for support
    ✅ Implement basic agents for CS
    
    PHASE 2: Enhancement (Weeks 5-8)
    - Deploy Cortex Analyst for self-service
    - Add forecasting for revenue planning
    - Create anomaly detection for usage
    - Build content recommendation engine
    
    PHASE 3: Advanced (Weeks 9-12)
    - Custom fine-tuned models
    - Multi-agent orchestration
    - Real-time intelligence dashboards
    - Automated action workflows
    
    KEY SUCCESS METRICS:
    - Reduction in support ticket volume (target: 20%)
    - Time saved on customer research (target: 50%)
    - Improvement in churn prediction accuracy (target: 15%)
    - Self-service analytics adoption (target: 30% of queries)
*/

-- ============================================================================
-- CONGRATULATIONS! LAB 4 COMPLETE!
-- ============================================================================

SELECT '🎉 Lab 4 Complete! Snowflake AI/ML Workshop Finished!' AS STATUS,
       'You have learned Cortex LLMs, Search, Analyst, Agents, and Intelligence' AS SUMMARY,
       'Next: Plan your FMG AI implementation roadmap' AS NEXT_STEP,
       CURRENT_TIMESTAMP() AS COMPLETED_AT;

