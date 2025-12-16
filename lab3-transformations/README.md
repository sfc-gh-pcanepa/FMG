# Lab 3: Transformations in Snowflake

---

## Overview

**Level**: Intermediate to Advanced

In this lab, you'll build data transformation pipelines using Snowflake's native features including Streams, Tasks, Dynamic Tables, and zero-copy cloning.

### What You'll Learn

- ✅ Build incremental pipelines with Streams and Tasks
- ✅ Create auto-refreshing Dynamic Tables
- ✅ Use zero-copy cloning and Time Travel
- ✅ Track data lineage
- ✅ Share transformed data

### Prerequisites

- [ ] Completed Labs 1-2
- [ ] `FMG_ADMIN` or `FMG_ENGINEER` role access

---

## Step 1: Understand Transformation Options

### Comparison: Streams+Tasks vs Dynamic Tables

| Feature | Streams + Tasks | Dynamic Tables |
|---------|-----------------|----------------|
| **Approach** | Procedural (you write MERGE) | Declarative (just SQL) |
| **Maintenance** | Manage streams, tasks, DAGs | Automatic |
| **Latency** | Can be near real-time | Minimum 1 minute |
| **Complexity** | More control, more code | Simpler, less control |
| **Best For** | Complex logic, external calls | Aggregations, marts |

---

## Step 2: Create Streams for CDC

### 2.1 What are Streams?

Streams capture change data (INSERT, UPDATE, DELETE) on tables:

```
┌─────────────┐         ┌─────────────┐         ┌─────────────┐
│  SOURCE     │────────▶│   STREAM    │────────▶│   TARGET    │
│  TABLE      │  DML    │   (CDC)     │  Task   │   TABLE     │
└─────────────┘         └─────────────┘         └─────────────┘
```

### 2.2 Create Streams

```sql
USE ROLE FMG_ADMIN;
USE DATABASE FMG_PRODUCTION;

-- Create staging schema
CREATE SCHEMA IF NOT EXISTS STAGING;
USE SCHEMA STAGING;

-- Stream on CUSTOMERS
CREATE OR REPLACE STREAM CUSTOMERS_STREAM 
    ON TABLE FMG_PRODUCTION.RAW.CUSTOMERS
    COMMENT = 'CDC stream for customer changes';

-- Stream on SUBSCRIPTIONS
CREATE OR REPLACE STREAM SUBSCRIPTIONS_STREAM 
    ON TABLE FMG_PRODUCTION.RAW.SUBSCRIPTIONS
    COMMENT = 'CDC stream for subscription changes';

-- Verify
SHOW STREAMS;
```

### 2.3 Test the Stream

```sql
-- Check stream (should be empty initially)
SELECT * FROM CUSTOMERS_STREAM LIMIT 10;

-- Make a change
UPDATE FMG_PRODUCTION.RAW.CUSTOMERS 
SET csm_owner = 'Updated CSM'
WHERE customer_id = (SELECT customer_id FROM FMG_PRODUCTION.RAW.CUSTOMERS LIMIT 1);

-- Check stream again (should show the change)
SELECT 
    customer_id,
    company_name,
    METADATA$ACTION,
    METADATA$ISUPDATE
FROM CUSTOMERS_STREAM;
```

**✅ Success Check**: Stream shows the UPDATE as DELETE + INSERT pair.

---

## Step 3: Create Tasks for Automation

### 3.1 Create Target Table

```sql
USE SCHEMA FMG_PRODUCTION.CURATED;

-- Create dimension table
CREATE OR REPLACE TABLE DIM_CUSTOMERS (
    customer_sk INTEGER AUTOINCREMENT,
    customer_id VARCHAR(20),
    company_name VARCHAR(200),
    segment VARCHAR(50),
    industry VARCHAR(100),
    account_status VARCHAR(20),
    tenure_months INTEGER,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### 3.2 Create the Task

```sql
USE SCHEMA FMG_PRODUCTION.STAGING;

-- Create task that processes stream
CREATE OR REPLACE TASK PROCESS_CUSTOMERS_TASK
    WAREHOUSE = FMG_ANALYTICS_M
    SCHEDULE = 'USING CRON 0 * * * * America/Los_Angeles'  -- Hourly
    COMMENT = 'Process customer changes from stream'
WHEN
    SYSTEM$STREAM_HAS_DATA('FMG_PRODUCTION.STAGING.CUSTOMERS_STREAM')
AS
    MERGE INTO FMG_PRODUCTION.CURATED.DIM_CUSTOMERS tgt
    USING (
        SELECT 
            customer_id,
            company_name,
            segment,
            industry,
            account_status,
            DATEDIFF('month', created_date, CURRENT_DATE()) AS tenure_months
        FROM FMG_PRODUCTION.STAGING.CUSTOMERS_STREAM
        WHERE METADATA$ACTION = 'INSERT'
    ) src
    ON tgt.customer_id = src.customer_id
    WHEN MATCHED THEN UPDATE SET
        company_name = src.company_name,
        segment = src.segment,
        account_status = src.account_status,
        tenure_months = src.tenure_months,
        _updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        customer_id, company_name, segment, industry, account_status, tenure_months
    ) VALUES (
        src.customer_id, src.company_name, src.segment, src.industry, 
        src.account_status, src.tenure_months
    );

-- Resume the task
ALTER TASK PROCESS_CUSTOMERS_TASK RESUME;

-- Verify
SHOW TASKS;
```

### 3.3 Test the Task

```sql
-- Execute immediately (don't wait for schedule)
EXECUTE TASK PROCESS_CUSTOMERS_TASK;

-- Check results
SELECT * FROM FMG_PRODUCTION.CURATED.DIM_CUSTOMERS LIMIT 10;

-- Check task history
SELECT name, state, scheduled_time, completed_time, error_message
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE name = 'PROCESS_CUSTOMERS_TASK'
ORDER BY scheduled_time DESC
LIMIT 5;
```

**✅ Success Check**: Task shows as SUCCEEDED in history.

---

## Step 4: Create Dynamic Tables

### 4.1 What are Dynamic Tables?

Dynamic Tables are declarative - you define the result, Snowflake maintains it:

```sql
CREATE DYNAMIC TABLE my_table
    TARGET_LAG = '1 hour'    -- How fresh
    WAREHOUSE = my_wh        -- Compute for refresh
AS
    SELECT ...               -- Your transformation
```

### 4.2 Create Customer 360 Dynamic Table

```sql
USE DATABASE FMG_ANALYTICS;
CREATE SCHEMA IF NOT EXISTS DYNAMIC;
USE SCHEMA DYNAMIC;

-- Customer 360 view
CREATE OR REPLACE DYNAMIC TABLE DT_CUSTOMER_360
    TARGET_LAG = '1 hour'
    WAREHOUSE = FMG_ANALYTICS_M
AS
SELECT 
    c.customer_id,
    c.company_name,
    c.segment,
    c.industry,
    c.account_status,
    c.csm_owner,
    DATEDIFF('month', c.created_date, CURRENT_DATE()) AS tenure_months,
    
    -- Revenue
    COALESCE(s.total_mrr, 0) AS mrr,
    COALESCE(s.product_count, 0) AS products,
    
    -- Health
    h.overall_health_score,
    h.churn_risk,
    
    CURRENT_TIMESTAMP() AS _refreshed_at
FROM FMG_PRODUCTION.RAW.CUSTOMERS c
LEFT JOIN (
    SELECT customer_id, SUM(mrr_amount) AS total_mrr, COUNT(*) AS product_count
    FROM FMG_PRODUCTION.RAW.SUBSCRIPTIONS WHERE status = 'Active'
    GROUP BY customer_id
) s ON c.customer_id = s.customer_id
LEFT JOIN (
    SELECT * FROM FMG_PRODUCTION.RAW.CUSTOMER_HEALTH_SCORES
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY snapshot_date DESC) = 1
) h ON c.customer_id = h.customer_id;

-- Query the dynamic table
SELECT * FROM DT_CUSTOMER_360 LIMIT 10;
```

### 4.3 Create Revenue Summary Dynamic Table

```sql
CREATE OR REPLACE DYNAMIC TABLE DT_REVENUE_SUMMARY
    TARGET_LAG = '1 hour'
    WAREHOUSE = FMG_ANALYTICS_M
AS
SELECT 
    c.segment,
    c.industry,
    s.product_name,
    COUNT(DISTINCT s.customer_id) AS customers,
    SUM(s.mrr_amount) AS total_mrr,
    AVG(s.mrr_amount) AS avg_mrr
FROM FMG_PRODUCTION.RAW.SUBSCRIPTIONS s
JOIN FMG_PRODUCTION.RAW.CUSTOMERS c ON s.customer_id = c.customer_id
WHERE s.status = 'Active'
GROUP BY 1, 2, 3;

SELECT * FROM DT_REVENUE_SUMMARY ORDER BY total_mrr DESC;
```

### 4.4 Check Dynamic Table Status

```sql
-- View refresh history
SELECT name, refresh_start_time, refresh_end_time, 
       DATEDIFF('second', refresh_start_time, refresh_end_time) AS duration_sec
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
WHERE name LIKE 'DT_%'
ORDER BY refresh_start_time DESC
LIMIT 10;
```

**✅ Success Check**: Dynamic tables are created and contain data.

---

## Step 5: Zero-Copy Cloning

### 5.1 Clone a Database

```sql
USE ROLE FMG_ADMIN;

-- Create instant dev copy (no additional storage!)
CREATE DATABASE FMG_PRODUCTION_DEV_CLONE
    CLONE FMG_PRODUCTION
    COMMENT = 'Dev clone created ' || CURRENT_DATE();

-- Verify
SHOW DATABASES LIKE 'FMG_PRODUCTION%';
```

### 5.2 Clone a Table

```sql
-- Clone a single table
CREATE TABLE FMG_PRODUCTION.RAW.CUSTOMERS_BACKUP
    CLONE FMG_PRODUCTION.RAW.CUSTOMERS;

-- Make changes to clone (original unaffected)
DELETE FROM FMG_PRODUCTION.RAW.CUSTOMERS_BACKUP 
WHERE account_status = 'Churned';

-- Compare
SELECT 'ORIGINAL' AS source, COUNT(*) AS rows FROM FMG_PRODUCTION.RAW.CUSTOMERS
UNION ALL
SELECT 'CLONE', COUNT(*) FROM FMG_PRODUCTION.RAW.CUSTOMERS_BACKUP;
```

**✅ Success Check**: Clone has fewer rows, original unchanged.

---

## Step 6: Time Travel & Undrop

### 6.1 Query Historical Data

```sql
-- Query table as it was 5 minutes ago
SELECT COUNT(*) FROM FMG_PRODUCTION.RAW.CUSTOMERS
AT(OFFSET => -300);  -- 300 seconds ago
```

### 6.2 Recover Dropped Table

```sql
-- Drop a table
DROP TABLE FMG_PRODUCTION.RAW.CUSTOMERS_BACKUP;

-- Verify it's gone
SHOW TABLES LIKE 'CUSTOMERS_BACKUP' IN SCHEMA FMG_PRODUCTION.RAW;

-- Undrop!
UNDROP TABLE FMG_PRODUCTION.RAW.CUSTOMERS_BACKUP;

-- Verify it's back
SHOW TABLES LIKE 'CUSTOMERS_BACKUP' IN SCHEMA FMG_PRODUCTION.RAW;
```

**✅ Success Check**: Dropped table is recovered.

---

## Step 7: Hands-On Exercise

### Challenge: Create a Usage Summary Dynamic Table

Create a dynamic table that shows monthly usage metrics per customer:

<details>
<summary>💡 Click for Solution</summary>

```sql
CREATE OR REPLACE DYNAMIC TABLE FMG_ANALYTICS.DYNAMIC.DT_USAGE_MONTHLY
    TARGET_LAG = '1 hour'
    WAREHOUSE = FMG_ANALYTICS_M
AS
SELECT 
    DATE_TRUNC('month', usage_date) AS month,
    customer_id,
    SUM(emails_sent) AS emails,
    SUM(social_posts_published) AS social_posts,
    SUM(website_leads_generated) AS leads,
    SUM(total_logins) AS logins,
    AVG(session_duration_minutes) AS avg_session_mins
FROM FMG_PRODUCTION.RAW.PLATFORM_USAGE_DAILY
GROUP BY 1, 2;
```

</details>

---

## Summary

In this lab, you learned:

| Topic | What You Did |
|-------|--------------|
| **Streams** | Created CDC streams on tables |
| **Tasks** | Built scheduled transformation jobs |
| **Dynamic Tables** | Created auto-maintained aggregations |
| **Cloning** | Made instant zero-copy clones |
| **Time Travel** | Queried and recovered historical data |

---

## Next Steps

👉 **[Continue to Lab 4: Agents & Intelligence →](../lab4-agents-intelligence/README.md)**

In Lab 4, you'll learn to:
- Use Cortex LLM functions for AI
- Create Semantic Views for natural language queries
- Build Cortex Agents that combine analytics and search
