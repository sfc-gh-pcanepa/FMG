/*=============================================================================
  FMG SUITE - LAB 3: TRANSFORMATIONS IN SNOWFLAKE
  Script 1: Streams & Tasks
  
  Description: Build incremental data pipelines using Change Data Capture
  Prerequisites: FMG databases and sample data created
  Duration: ~20 minutes
=============================================================================*/

-- ============================================================================
-- SECTION 1: SET CONTEXT
-- ============================================================================

USE ROLE FMG_ADMIN;
USE WAREHOUSE FMG_ANALYTICS_M;
USE DATABASE FMG_PRODUCTION;

-- ============================================================================
-- SECTION 2: UNDERSTANDING STREAMS
-- ============================================================================

/*
    Streams are CDC (Change Data Capture) objects that track:
    
    METADATA$ACTION     - 'INSERT' or 'DELETE' (UPDATE = DELETE + INSERT)
    METADATA$ISUPDATE   - TRUE if this row is part of an UPDATE
    METADATA$ROW_ID     - Unique identifier for the changed row
    
    Stream Types:
    - Standard: Tracks all DML (default)
    - Append-only: Only tracks INSERTs (for logs, events)
    - Insert-only: Similar to append-only
    
    Key Concepts:
    - Streams have a "position" (offset) that moves forward when consumed
    - DML within a transaction consumes the stream
    - Stream shows changes since last consumption
    - Zero storage cost - just a pointer
*/

-- ============================================================================
-- SECTION 3: CREATE STREAMS ON FMG TABLES
-- ============================================================================

-- Create staging schema for pipeline work
CREATE SCHEMA IF NOT EXISTS FMG_PRODUCTION.STAGING
    COMMENT = 'Staging area for data transformations';

CREATE SCHEMA IF NOT EXISTS FMG_ANALYTICS.PIPELINES
    COMMENT = 'Pipeline transformation tables';

-- Stream on CUSTOMERS table (standard - tracks all changes)
CREATE OR REPLACE STREAM FMG_PRODUCTION.STAGING.CUSTOMERS_STREAM 
    ON TABLE FMG_PRODUCTION.RAW.CUSTOMERS
    COMMENT = 'CDC stream tracking customer changes for incremental processing';

-- Stream on SUBSCRIPTIONS table
CREATE OR REPLACE STREAM FMG_PRODUCTION.STAGING.SUBSCRIPTIONS_STREAM 
    ON TABLE FMG_PRODUCTION.RAW.SUBSCRIPTIONS
    COMMENT = 'CDC stream tracking subscription changes for MRR calculations';

-- Stream on PLATFORM_USAGE_DAILY (append-only - we only add new rows)
CREATE OR REPLACE STREAM FMG_PRODUCTION.STAGING.USAGE_STREAM 
    ON TABLE FMG_PRODUCTION.RAW.PLATFORM_USAGE_DAILY
    APPEND_ONLY = TRUE
    COMMENT = 'Append-only stream for daily usage data';

-- Stream on SUPPORT_TICKETS
CREATE OR REPLACE STREAM FMG_PRODUCTION.STAGING.TICKETS_STREAM 
    ON TABLE FMG_PRODUCTION.RAW.SUPPORT_TICKETS
    COMMENT = 'CDC stream for support ticket updates';

-- View created streams
SHOW STREAMS IN SCHEMA FMG_PRODUCTION.STAGING;

-- ============================================================================
-- SECTION 4: SIMULATE CHANGES AND VIEW STREAM DATA
-- ============================================================================

-- Check if stream has any data (initially empty)
SELECT * FROM FMG_PRODUCTION.STAGING.CUSTOMERS_STREAM LIMIT 10;

-- Make a change to see stream in action
UPDATE FMG_PRODUCTION.RAW.CUSTOMERS 
SET account_status = 'Active'
WHERE customer_id = (SELECT customer_id FROM FMG_PRODUCTION.RAW.CUSTOMERS LIMIT 1);

-- Now view the stream - should show the change
SELECT 
    customer_id,
    company_name,
    account_status,
    METADATA$ACTION,
    METADATA$ISUPDATE,
    METADATA$ROW_ID
FROM FMG_PRODUCTION.STAGING.CUSTOMERS_STREAM;

-- ============================================================================
-- SECTION 5: CREATE TARGET TABLES FOR PIPELINES
-- ============================================================================

-- Customer dimension table (curated layer)
CREATE OR REPLACE TABLE FMG_PRODUCTION.CURATED.DIM_CUSTOMERS (
    customer_sk INTEGER AUTOINCREMENT,  -- Surrogate key
    customer_id VARCHAR(20),             -- Natural key
    company_name VARCHAR(200),
    segment VARCHAR(50),
    industry VARCHAR(100),
    sub_industry VARCHAR(100),
    state VARCHAR(2),
    account_status VARCHAR(20),
    csm_owner VARCHAR(100),
    tenure_months INTEGER,
    is_current BOOLEAN DEFAULT TRUE,
    effective_from TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    effective_to TIMESTAMP_NTZ,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- MRR movements fact table
CREATE OR REPLACE TABLE FMG_ANALYTICS.PIPELINES.FACT_MRR_MOVEMENTS (
    movement_id INTEGER AUTOINCREMENT,
    movement_date DATE,
    customer_id VARCHAR(20),
    subscription_id VARCHAR(20),
    product_name VARCHAR(100),
    movement_type VARCHAR(20),  -- NEW, CHURN, EXPANSION, CONTRACTION, REACTIVATION
    previous_mrr DECIMAL(10,2),
    new_mrr DECIMAL(10,2),
    mrr_change DECIMAL(10,2),
    _processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Monthly usage aggregation
CREATE OR REPLACE TABLE FMG_ANALYTICS.PIPELINES.FACT_USAGE_MONTHLY (
    customer_id VARCHAR(20),
    usage_month DATE,
    total_emails_sent INTEGER,
    total_social_posts INTEGER,
    total_website_leads INTEGER,
    total_myrepchat_messages INTEGER,
    total_logins INTEGER,
    total_session_minutes INTEGER,
    active_users INTEGER,
    _processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (customer_id, usage_month)
);

-- ============================================================================
-- SECTION 6: CREATE STORED PROCEDURES FOR TRANSFORMATIONS
-- ============================================================================

-- Procedure to process customer changes
CREATE OR REPLACE PROCEDURE FMG_PRODUCTION.STAGING.PROCESS_CUSTOMER_CHANGES()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    rows_processed INTEGER;
BEGIN
    -- Merge changes from stream into dimension table
    MERGE INTO FMG_PRODUCTION.CURATED.DIM_CUSTOMERS tgt
    USING (
        SELECT 
            customer_id,
            company_name,
            segment,
            industry,
            sub_industry,
            state,
            account_status,
            csm_owner,
            DATEDIFF('month', created_date, CURRENT_DATE()) AS tenure_months
        FROM FMG_PRODUCTION.STAGING.CUSTOMERS_STREAM
        WHERE METADATA$ACTION = 'INSERT'
    ) src
    ON tgt.customer_id = src.customer_id AND tgt.is_current = TRUE
    
    -- Update existing records
    WHEN MATCHED AND (
        tgt.company_name != src.company_name OR
        tgt.segment != src.segment OR
        tgt.account_status != src.account_status
    ) THEN UPDATE SET
        is_current = FALSE,
        effective_to = CURRENT_TIMESTAMP(),
        _updated_at = CURRENT_TIMESTAMP()
    
    -- Insert new records
    WHEN NOT MATCHED THEN INSERT (
        customer_id, company_name, segment, industry, sub_industry,
        state, account_status, csm_owner, tenure_months
    ) VALUES (
        src.customer_id, src.company_name, src.segment, src.industry, 
        src.sub_industry, src.state, src.account_status, src.csm_owner, 
        src.tenure_months
    );
    
    -- Get row count
    rows_processed := SQLROWCOUNT;
    
    RETURN 'Processed ' || rows_processed || ' customer changes';
END;
$$;

-- Procedure to aggregate monthly usage
CREATE OR REPLACE PROCEDURE FMG_ANALYTICS.PIPELINES.AGGREGATE_MONTHLY_USAGE()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    rows_processed INTEGER;
BEGIN
    -- Merge new usage data into monthly aggregation
    MERGE INTO FMG_ANALYTICS.PIPELINES.FACT_USAGE_MONTHLY tgt
    USING (
        SELECT 
            customer_id,
            DATE_TRUNC('month', usage_date) AS usage_month,
            SUM(emails_sent) AS total_emails_sent,
            SUM(social_posts_published) AS total_social_posts,
            SUM(website_leads_generated) AS total_website_leads,
            SUM(myrepchat_messages_sent) AS total_myrepchat_messages,
            SUM(total_logins) AS total_logins,
            SUM(session_duration_minutes) AS total_session_minutes,
            COUNT(DISTINCT user_id) AS active_users
        FROM FMG_PRODUCTION.STAGING.USAGE_STREAM
        GROUP BY customer_id, DATE_TRUNC('month', usage_date)
    ) src
    ON tgt.customer_id = src.customer_id AND tgt.usage_month = src.usage_month
    
    WHEN MATCHED THEN UPDATE SET
        total_emails_sent = tgt.total_emails_sent + src.total_emails_sent,
        total_social_posts = tgt.total_social_posts + src.total_social_posts,
        total_website_leads = tgt.total_website_leads + src.total_website_leads,
        total_myrepchat_messages = tgt.total_myrepchat_messages + src.total_myrepchat_messages,
        total_logins = tgt.total_logins + src.total_logins,
        total_session_minutes = tgt.total_session_minutes + src.total_session_minutes,
        active_users = src.active_users,
        _processed_at = CURRENT_TIMESTAMP()
    
    WHEN NOT MATCHED THEN INSERT (
        customer_id, usage_month, total_emails_sent, total_social_posts,
        total_website_leads, total_myrepchat_messages, total_logins,
        total_session_minutes, active_users
    ) VALUES (
        src.customer_id, src.usage_month, src.total_emails_sent, src.total_social_posts,
        src.total_website_leads, src.total_myrepchat_messages, src.total_logins,
        src.total_session_minutes, src.active_users
    );
    
    rows_processed := SQLROWCOUNT;
    RETURN 'Processed ' || rows_processed || ' monthly usage records';
END;
$$;

-- ============================================================================
-- SECTION 7: CREATE TASKS
-- ============================================================================

/*
    Task Scheduling Options:
    - SCHEDULE = 'USING CRON 0 * * * * America/Los_Angeles'  (hourly at :00)
    - SCHEDULE = '5 MINUTE'  (every 5 minutes)
    - AFTER <other_task>  (dependency chain)
    
    Task Requirements:
    - Warehouse for execution
    - Must be RESUMED to run
    - Can check SYSTEM$STREAM_HAS_DATA() to skip empty runs
*/

-- Task to process customer changes (hourly)
CREATE OR REPLACE TASK FMG_PRODUCTION.STAGING.TASK_PROCESS_CUSTOMERS
    WAREHOUSE = FMG_ANALYTICS_M
    SCHEDULE = 'USING CRON 0 * * * * America/Los_Angeles'
    COMMENT = 'Hourly task to process customer changes from stream'
WHEN
    SYSTEM$STREAM_HAS_DATA('FMG_PRODUCTION.STAGING.CUSTOMERS_STREAM')
AS
    CALL FMG_PRODUCTION.STAGING.PROCESS_CUSTOMER_CHANGES();

-- Task to aggregate usage (runs after customer task)
CREATE OR REPLACE TASK FMG_PRODUCTION.STAGING.TASK_AGGREGATE_USAGE
    WAREHOUSE = FMG_ANALYTICS_M
    AFTER FMG_PRODUCTION.STAGING.TASK_PROCESS_CUSTOMERS
    COMMENT = 'Aggregate daily usage into monthly totals'
WHEN
    SYSTEM$STREAM_HAS_DATA('FMG_PRODUCTION.STAGING.USAGE_STREAM')
AS
    CALL FMG_ANALYTICS.PIPELINES.AGGREGATE_MONTHLY_USAGE();

-- Standalone task for MRR movements (every 6 hours)
CREATE OR REPLACE TASK FMG_PRODUCTION.STAGING.TASK_MRR_MOVEMENTS
    WAREHOUSE = FMG_ANALYTICS_M
    SCHEDULE = 'USING CRON 0 */6 * * * America/Los_Angeles'
    COMMENT = 'Calculate MRR movements from subscription changes'
WHEN
    SYSTEM$STREAM_HAS_DATA('FMG_PRODUCTION.STAGING.SUBSCRIPTIONS_STREAM')
AS
    INSERT INTO FMG_ANALYTICS.PIPELINES.FACT_MRR_MOVEMENTS (
        movement_date, customer_id, subscription_id, product_name,
        movement_type, previous_mrr, new_mrr, mrr_change
    )
    SELECT 
        CURRENT_DATE() AS movement_date,
        s.customer_id,
        s.subscription_id,
        s.product_name,
        CASE 
            WHEN METADATA$ACTION = 'INSERT' AND NOT METADATA$ISUPDATE THEN 'NEW'
            WHEN METADATA$ACTION = 'DELETE' AND NOT METADATA$ISUPDATE THEN 'CHURN'
            WHEN METADATA$ISUPDATE THEN 
                CASE 
                    WHEN s.mrr_amount > 0 THEN 'EXPANSION'
                    ELSE 'CONTRACTION'
                END
            ELSE 'UNKNOWN'
        END AS movement_type,
        0 AS previous_mrr,  -- Would need to look up from history
        s.mrr_amount AS new_mrr,
        s.mrr_amount AS mrr_change
    FROM FMG_PRODUCTION.STAGING.SUBSCRIPTIONS_STREAM s;

-- ============================================================================
-- SECTION 8: VIEW AND MANAGE TASKS
-- ============================================================================

-- View all tasks
SHOW TASKS IN SCHEMA FMG_PRODUCTION.STAGING;

-- Check task dependencies (DAG structure)
SELECT 
    name,
    schedule,
    state,
    predecessors,
    condition,
    definition
FROM TABLE(INFORMATION_SCHEMA.TASK_DEPENDENTS(
    TASK_NAME => 'FMG_PRODUCTION.STAGING.TASK_PROCESS_CUSTOMERS',
    RECURSIVE => TRUE
));

-- Resume tasks (they start suspended by default)
-- Must resume child tasks first, then parent
ALTER TASK FMG_PRODUCTION.STAGING.TASK_AGGREGATE_USAGE RESUME;
ALTER TASK FMG_PRODUCTION.STAGING.TASK_PROCESS_CUSTOMERS RESUME;
ALTER TASK FMG_PRODUCTION.STAGING.TASK_MRR_MOVEMENTS RESUME;

-- View task run history
SELECT 
    name,
    state,
    scheduled_time,
    completed_time,
    error_code,
    error_message
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE name LIKE 'TASK_%'
ORDER BY scheduled_time DESC
LIMIT 20;

-- ============================================================================
-- SECTION 9: MANUALLY TRIGGER TASKS (For Testing)
-- ============================================================================

-- Execute a task immediately (for testing)
EXECUTE TASK FMG_PRODUCTION.STAGING.TASK_PROCESS_CUSTOMERS;

-- Check the results
SELECT * FROM FMG_PRODUCTION.CURATED.DIM_CUSTOMERS LIMIT 10;

-- ============================================================================
-- SECTION 10: MONITORING STREAMS AND TASKS
-- ============================================================================

-- Check stream status (stale time indicates last consumption)
SELECT 
    name,
    stale_after,
    CASE 
        WHEN stale_after IS NULL THEN 'NEVER CONSUMED'
        WHEN stale_after < CURRENT_TIMESTAMP() THEN 'STALE'
        ELSE 'CURRENT'
    END AS status
FROM TABLE(INFORMATION_SCHEMA.STREAMS())
WHERE schema_name = 'STAGING';

-- Create a monitoring view
CREATE OR REPLACE VIEW FMG_PRODUCTION.STAGING.V_PIPELINE_STATUS AS
SELECT 
    'CUSTOMERS_STREAM' AS stream_name,
    (SELECT COUNT(*) FROM FMG_PRODUCTION.STAGING.CUSTOMERS_STREAM) AS pending_rows,
    (SELECT MAX(_updated_at) FROM FMG_PRODUCTION.CURATED.DIM_CUSTOMERS) AS last_processed
UNION ALL
SELECT 
    'USAGE_STREAM',
    (SELECT COUNT(*) FROM FMG_PRODUCTION.STAGING.USAGE_STREAM),
    (SELECT MAX(_processed_at) FROM FMG_ANALYTICS.PIPELINES.FACT_USAGE_MONTHLY)
UNION ALL
SELECT 
    'SUBSCRIPTIONS_STREAM',
    (SELECT COUNT(*) FROM FMG_PRODUCTION.STAGING.SUBSCRIPTIONS_STREAM),
    (SELECT MAX(_processed_at) FROM FMG_ANALYTICS.PIPELINES.FACT_MRR_MOVEMENTS);

-- Check pipeline status
SELECT * FROM FMG_PRODUCTION.STAGING.V_PIPELINE_STATUS;

-- ============================================================================
-- SECTION 11: CLEANUP (Optional)
-- ============================================================================

/*
    To stop and clean up tasks:
    
    -- Suspend tasks (parent first, then children)
    ALTER TASK FMG_PRODUCTION.STAGING.TASK_PROCESS_CUSTOMERS SUSPEND;
    ALTER TASK FMG_PRODUCTION.STAGING.TASK_AGGREGATE_USAGE SUSPEND;
    ALTER TASK FMG_PRODUCTION.STAGING.TASK_MRR_MOVEMENTS SUSPEND;
    
    -- Drop tasks
    DROP TASK FMG_PRODUCTION.STAGING.TASK_AGGREGATE_USAGE;
    DROP TASK FMG_PRODUCTION.STAGING.TASK_PROCESS_CUSTOMERS;
    
    -- Drop streams
    DROP STREAM FMG_PRODUCTION.STAGING.CUSTOMERS_STREAM;
*/

-- ============================================================================
-- SCRIPT COMPLETE!
-- ============================================================================

SELECT '✅ Streams & Tasks Setup Complete!' AS STATUS,
       'Tasks are RESUMED and will run on schedule' AS NOTE,
       CURRENT_TIMESTAMP() AS COMPLETED_AT;

