# Lab 2: Governance + FinOps

---

## Overview

**Level**: Intermediate

In this lab, you'll implement governance and cost management for FMG's data platform including data classification, resource monitoring, auditing, and access policies.

### What You'll Learn

- ✅ Navigate Snowflake Horizon for data discovery
- ✅ Create tags for data classification
- ✅ Set up resource monitors and budgets
- ✅ Query ACCOUNT_USAGE for auditing
- ✅ Implement row-level security and masking

### Prerequisites

- [ ] Completed Lab 1
- [ ] `ACCOUNTADMIN` role access

---

## Step 1: Explore Horizon Catalog

### 1.1 Understand Horizon

Snowflake Horizon provides built-in governance:

```
┌─────────────────────────────────────────────────────────────────┐
│                    SNOWFLAKE HORIZON                             │
├─────────────────────────────────────────────────────────────────┤
│  DISCOVER          │  GOVERN           │  SECURE                │
│  • Data Catalog    │  • Data Quality   │  • Access Control      │
│  • Search          │  • Data Lineage   │  • Masking             │
│  • Classification  │  • Tags & Policies│  • Row Access          │
└─────────────────────────────────────────────────────────────────┘
```

### 1.2 Create Governance Tags

```sql
USE ROLE ACCOUNTADMIN;
USE DATABASE FMG_PRODUCTION;

-- Create governance schema
CREATE SCHEMA IF NOT EXISTS GOVERNANCE;
USE SCHEMA GOVERNANCE;

-- Create sensitivity tag
CREATE TAG IF NOT EXISTS DATA_SENSITIVITY
    ALLOWED_VALUES = 'PUBLIC', 'INTERNAL', 'CONFIDENTIAL', 'RESTRICTED'
    COMMENT = 'Data sensitivity classification';

-- Create PII tag
CREATE TAG IF NOT EXISTS PII_CATEGORY
    ALLOWED_VALUES = 'DIRECT_IDENTIFIER', 'QUASI_IDENTIFIER', 'SENSITIVE', 'NON_PII'
    COMMENT = 'Personal identifiable information category';

-- Create domain tag
CREATE TAG IF NOT EXISTS DATA_DOMAIN
    ALLOWED_VALUES = 'CUSTOMER', 'FINANCIAL', 'OPERATIONS', 'MARKETING'
    COMMENT = 'Business domain ownership';

-- Verify
SHOW TAGS IN SCHEMA GOVERNANCE;
```

**✅ Success Check**: You should see 3 tags listed.

### 1.3 Apply Tags to Tables

```sql
-- Tag the CUSTOMERS table
ALTER TABLE FMG_PRODUCTION.RAW.CUSTOMERS SET TAG 
    FMG_PRODUCTION.GOVERNANCE.DATA_SENSITIVITY = 'CONFIDENTIAL',
    FMG_PRODUCTION.GOVERNANCE.DATA_DOMAIN = 'CUSTOMER';

-- Tag the USERS table (contains PII)
ALTER TABLE FMG_PRODUCTION.RAW.USERS SET TAG 
    FMG_PRODUCTION.GOVERNANCE.DATA_SENSITIVITY = 'RESTRICTED',
    FMG_PRODUCTION.GOVERNANCE.DATA_DOMAIN = 'CUSTOMER';

-- Tag PII columns
ALTER TABLE FMG_PRODUCTION.RAW.USERS MODIFY COLUMN 
    email SET TAG FMG_PRODUCTION.GOVERNANCE.PII_CATEGORY = 'DIRECT_IDENTIFIER';

ALTER TABLE FMG_PRODUCTION.RAW.USERS MODIFY COLUMN 
    phone SET TAG FMG_PRODUCTION.GOVERNANCE.PII_CATEGORY = 'DIRECT_IDENTIFIER';

-- Verify tags on table
SELECT * FROM TABLE(FMG_PRODUCTION.INFORMATION_SCHEMA.TAG_REFERENCES(
    'FMG_PRODUCTION.RAW.USERS', 'TABLE'
));
```

---

## Step 2: Set Up Resource Monitoring

### 2.1 Understand Credit Usage

| Warehouse Size | Credits/Hour | ~Cost/Hour |
|---------------|--------------|------------|
| X-Small | 1 | $3 |
| Small | 2 | $6 |
| Medium | 4 | $12 |
| Large | 8 | $24 |

### 2.2 Create Resource Monitors

```sql
USE ROLE ACCOUNTADMIN;

-- Account-level monitor (safety net)
CREATE RESOURCE MONITOR IF NOT EXISTS FMG_ACCOUNT_MONITOR
    WITH CREDIT_QUOTA = 3000
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 75 PERCENT DO NOTIFY
        ON 90 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND;

-- Development warehouse monitor
CREATE RESOURCE MONITOR IF NOT EXISTS FMG_DEV_MONITOR
    WITH CREDIT_QUOTA = 200
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 75 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND;

-- Apply to warehouses
ALTER WAREHOUSE FMG_DEV_XS SET RESOURCE_MONITOR = FMG_DEV_MONITOR;

-- Verify
SHOW RESOURCE MONITORS;
```

**✅ Success Check**: Resource monitors show in the list with credit quotas.

### 2.3 Create Budget Tracking

```sql
USE SCHEMA FMG_PRODUCTION.GOVERNANCE;

-- Create budget configuration table
CREATE OR REPLACE TABLE BUDGET_CONFIG (
    category VARCHAR PRIMARY KEY,
    monthly_credits DECIMAL(10,2),
    monthly_usd DECIMAL(10,2),
    owner_email VARCHAR
);

INSERT INTO BUDGET_CONFIG VALUES
    ('COMPUTE_DEV', 200, 600, 'engineering@fmgsuite.com'),
    ('COMPUTE_PROD', 500, 1500, 'operations@fmgsuite.com'),
    ('COMPUTE_ANALYTICS', 1000, 3000, 'analytics@fmgsuite.com'),
    ('COMPUTE_ML', 800, 2400, 'datascience@fmgsuite.com');

SELECT * FROM BUDGET_CONFIG;
```

---

## Step 3: Query ACCOUNT_USAGE for Auditing

### 3.1 Understand ACCOUNT_USAGE

The `SNOWFLAKE.ACCOUNT_USAGE` schema contains 365 days of audit data:

| View | Purpose |
|------|---------|
| `QUERY_HISTORY` | All queries with timing and credits |
| `LOGIN_HISTORY` | User authentication events |
| `ACCESS_HISTORY` | Data access patterns |
| `WAREHOUSE_METERING_HISTORY` | Credit consumption |

### 3.2 Query Expensive Queries

```sql
-- Top 10 most expensive queries (last 7 days)
SELECT 
    query_id,
    user_name,
    warehouse_name,
    ROUND(total_elapsed_time/1000, 2) AS seconds,
    ROUND(bytes_scanned/1e9, 2) AS gb_scanned,
    LEFT(query_text, 100) AS query_preview
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
AND warehouse_name LIKE 'FMG%'
ORDER BY total_elapsed_time DESC
LIMIT 10;
```

### 3.3 Check Login Activity

```sql
-- Recent login failures
SELECT 
    event_timestamp,
    user_name,
    client_ip,
    error_message
FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
WHERE is_success = 'NO'
AND event_timestamp >= DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY event_timestamp DESC
LIMIT 20;
```

### 3.4 Track Data Access

```sql
-- Who accessed customer data?
SELECT 
    DATE(query_start_time) AS access_date,
    user_name,
    obj.value:objectName::STRING AS table_accessed,
    COUNT(*) AS access_count
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY,
    LATERAL FLATTEN(input => direct_objects_accessed) obj
WHERE query_start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
AND obj.value:objectName::STRING LIKE '%CUSTOMER%'
GROUP BY 1, 2, 3
ORDER BY access_date DESC, access_count DESC;
```

**✅ Success Check**: Queries return results from your account activity.

---

## Step 4: Implement Data Masking

### 4.1 Create Masking Policies

```sql
USE ROLE ACCOUNTADMIN;
USE SCHEMA FMG_PRODUCTION.GOVERNANCE;

-- Email masking policy
CREATE OR REPLACE MASKING POLICY EMAIL_MASK AS (val STRING) 
RETURNS STRING ->
    CASE 
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'FMG_ADMIN', 'FMG_COMPLIANCE_OFFICER') 
            THEN val
        WHEN CURRENT_ROLE() IN ('FMG_ANALYST', 'FMG_ENGINEER') 
            THEN REGEXP_REPLACE(val, '^[^@]+', '****')
        ELSE '****@****.***'
    END;

-- Phone masking policy
CREATE OR REPLACE MASKING POLICY PHONE_MASK AS (val STRING) 
RETURNS STRING ->
    CASE 
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'FMG_ADMIN', 'FMG_COMPLIANCE_OFFICER') 
            THEN val
        WHEN CURRENT_ROLE() IN ('FMG_ANALYST') 
            THEN '(***) ***-' || RIGHT(REGEXP_REPLACE(val, '[^0-9]', ''), 4)
        ELSE '(***) ***-****'
    END;

-- Revenue masking policy
CREATE OR REPLACE MASKING POLICY REVENUE_MASK AS (val NUMBER) 
RETURNS NUMBER ->
    CASE 
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'FMG_ADMIN', 'FMG_ANALYST') 
            THEN val
        ELSE NULL
    END;
```

### 4.2 Apply Masking Policies

```sql
-- Apply to USERS table
ALTER TABLE FMG_PRODUCTION.RAW.USERS 
    MODIFY COLUMN email SET MASKING POLICY FMG_PRODUCTION.GOVERNANCE.EMAIL_MASK;

ALTER TABLE FMG_PRODUCTION.RAW.USERS 
    MODIFY COLUMN phone SET MASKING POLICY FMG_PRODUCTION.GOVERNANCE.PHONE_MASK;

-- Apply to SUBSCRIPTIONS table
ALTER TABLE FMG_PRODUCTION.RAW.SUBSCRIPTIONS 
    MODIFY COLUMN mrr_amount SET MASKING POLICY FMG_PRODUCTION.GOVERNANCE.REVENUE_MASK;
```

### 4.3 Test Masking

```sql
-- Test as ADMIN (should see full data)
USE ROLE FMG_ADMIN;
SELECT user_id, email, phone FROM FMG_PRODUCTION.RAW.USERS LIMIT 5;

-- Test as ANALYST (should see partial masking)
USE ROLE FMG_ANALYST;
SELECT user_id, email, phone FROM FMG_PRODUCTION.RAW.USERS LIMIT 5;

-- Test as VIEWER (should see full masking)
USE ROLE FMG_VIEWER;
SELECT user_id, email, phone FROM FMG_PRODUCTION.RAW.USERS LIMIT 5;
```

**✅ Success Check**: Different roles see different levels of masked data.

---

## Step 5: Implement Row-Level Security

### 5.1 Create Row Access Policy

```sql
USE ROLE ACCOUNTADMIN;
USE SCHEMA FMG_PRODUCTION.GOVERNANCE;

-- Only show active customers to analysts
CREATE OR REPLACE ROW ACCESS POLICY ACTIVE_CUSTOMER_ONLY AS (status VARCHAR) 
RETURNS BOOLEAN ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'FMG_ADMIN', 'FMG_ENGINEER') 
            THEN TRUE
        ELSE status IN ('Active', 'Paused', 'Trial')
    END;

-- Apply to CUSTOMERS table
ALTER TABLE FMG_PRODUCTION.RAW.CUSTOMERS 
    ADD ROW ACCESS POLICY ACTIVE_CUSTOMER_ONLY ON (account_status);
```

### 5.2 Test Row Access

```sql
-- As ADMIN (sees all statuses)
USE ROLE FMG_ADMIN;
SELECT account_status, COUNT(*) 
FROM FMG_PRODUCTION.RAW.CUSTOMERS 
GROUP BY 1;

-- As ANALYST (should not see 'Churned')
USE ROLE FMG_ANALYST;
SELECT account_status, COUNT(*) 
FROM FMG_PRODUCTION.RAW.CUSTOMERS 
GROUP BY 1;
```

**✅ Success Check**: Analysts cannot see churned customers.

---

## Step 6: Hands-On Exercise

### Challenge: Create a Cost Dashboard Query

Write a query that shows daily credit usage by warehouse for the current month:

<details>
<summary>💡 Click for Solution</summary>

```sql
SELECT 
    DATE(start_time) AS usage_date,
    warehouse_name,
    ROUND(SUM(credits_used), 2) AS daily_credits,
    ROUND(SUM(credits_used) * 3, 2) AS estimated_cost_usd
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATE_TRUNC('month', CURRENT_DATE())
AND warehouse_name LIKE 'FMG%'
GROUP BY 1, 2
ORDER BY usage_date DESC, daily_credits DESC;
```

</details>

---

## Summary

In this lab, you learned how to:

| Topic | What You Did |
|-------|--------------|
| **Tags** | Created and applied data classification tags |
| **Monitors** | Set up resource monitors for cost control |
| **Auditing** | Queried ACCOUNT_USAGE for compliance |
| **Masking** | Created dynamic data masking policies |
| **RLS** | Implemented row-level security |

---

## Next Steps

👉 **[Continue to Lab 3: Transformations →](../lab3-transformations/README.md)**

In Lab 3, you'll learn to:
- Build incremental pipelines with Streams and Tasks
- Create auto-refreshing Dynamic Tables
- Use zero-copy cloning for dev environments
- Track data lineage
