# Lab 1: Getting Started with Snowflake

---

## Overview

**Level**: Beginner

In this lab, you'll set up the foundational components of FMG's Snowflake environment including users, roles, warehouses, and data sharing.

### What You'll Learn

- ✅ Create custom roles with proper hierarchy
- ✅ Provision virtual warehouses for different workloads
- ✅ Configure secure data sharing
- ✅ Apply Snowflake security best practices

### Prerequisites

- [ ] Completed the setup scripts from the main README
- [ ] `ACCOUNTADMIN` role access
- [ ] Snowsight open in your browser

---

## Step 1: Understand Snowflake Architecture

Before we start, let's review Snowflake's three-layer architecture:

```
┌─────────────────────────────────────────────────────────────────┐
│                    CLOUD SERVICES LAYER                          │
│   Authentication • Query Optimization • Metadata • Access Control│
├─────────────────────────────────────────────────────────────────┤
│                    COMPUTE LAYER                                 │
│   ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐       │
│   │ FMG_DEV  │  │ FMG_PROD │  │FMG_ANLYTC│  │  FMG_ML  │       │
│   │   _XS    │  │    _S    │  │    _M    │  │    _L    │       │
│   └──────────┘  └──────────┘  └──────────┘  └──────────┘       │
│   Independent • Per-second billing • Auto-suspend/resume        │
├─────────────────────────────────────────────────────────────────┤
│                    STORAGE LAYER                                 │
│   FMG_PRODUCTION • FMG_ANALYTICS • FMG_DEVELOPMENT              │
│   Centralized • Compressed • Encrypted • Zero-copy cloning      │
└─────────────────────────────────────────────────────────────────┘
```

**Key Insight**: Compute and storage are completely separated. You can scale compute without affecting storage, and vice versa.

---

## Step 2: Create Users and Roles

### 2.1 Open a New Worksheet

1. In Snowsight, click **+ Worksheet**
2. Name it "Lab 1 - Users and Roles"
3. Set role to `ACCOUNTADMIN`

### 2.2 Understand the Role Hierarchy

FMG's role hierarchy follows the principle of least privilege:

```
ACCOUNTADMIN
    │
    └── SYSADMIN
            │
            └── FMG_ADMIN
                    │
                    ├── FMG_ENGINEER
                    │
                    ├── FMG_ANALYST ───► FMG_VIEWER
                    │
                    ├── FMG_COMPLIANCE_OFFICER
                    │
                    └── FMG_DATA_SCIENTIST
```

### 2.3 Run the Role Setup Script

Copy and run this code:

```sql
-- Set context
USE ROLE ACCOUNTADMIN;

-- Create custom roles
CREATE ROLE IF NOT EXISTS FMG_ADMIN COMMENT = 'FMG admin with full database access';
CREATE ROLE IF NOT EXISTS FMG_ENGINEER COMMENT = 'Data engineering role';
CREATE ROLE IF NOT EXISTS FMG_ANALYST COMMENT = 'Analytics and reporting role';
CREATE ROLE IF NOT EXISTS FMG_VIEWER COMMENT = 'Read-only access';
CREATE ROLE IF NOT EXISTS FMG_COMPLIANCE_OFFICER COMMENT = 'Audit and compliance access';
CREATE ROLE IF NOT EXISTS FMG_DATA_SCIENTIST COMMENT = 'ML/AI workloads';

-- Establish hierarchy
GRANT ROLE FMG_VIEWER TO ROLE FMG_ANALYST;
GRANT ROLE FMG_ANALYST TO ROLE FMG_ADMIN;
GRANT ROLE FMG_ENGINEER TO ROLE FMG_ADMIN;
GRANT ROLE FMG_COMPLIANCE_OFFICER TO ROLE FMG_ADMIN;
GRANT ROLE FMG_DATA_SCIENTIST TO ROLE FMG_ADMIN;
GRANT ROLE FMG_ADMIN TO ROLE SYSADMIN;

-- Verify
SHOW ROLES LIKE 'FMG%';
```

**✅ Success Check**: You should see 6 FMG roles listed.

### 2.4 Grant Database Permissions

```sql
-- FMG_ADMIN: Full access
GRANT ALL PRIVILEGES ON DATABASE FMG_PRODUCTION TO ROLE FMG_ADMIN;
GRANT ALL PRIVILEGES ON DATABASE FMG_ANALYTICS TO ROLE FMG_ADMIN;
GRANT ALL PRIVILEGES ON DATABASE FMG_DEVELOPMENT TO ROLE FMG_ADMIN;

-- FMG_ANALYST: Read access to production and analytics
GRANT USAGE ON DATABASE FMG_PRODUCTION TO ROLE FMG_ANALYST;
GRANT USAGE ON DATABASE FMG_ANALYTICS TO ROLE FMG_ANALYST;
GRANT USAGE ON ALL SCHEMAS IN DATABASE FMG_PRODUCTION TO ROLE FMG_ANALYST;
GRANT USAGE ON ALL SCHEMAS IN DATABASE FMG_ANALYTICS TO ROLE FMG_ANALYST;
GRANT SELECT ON ALL TABLES IN DATABASE FMG_PRODUCTION TO ROLE FMG_ANALYST;
GRANT SELECT ON ALL TABLES IN DATABASE FMG_ANALYTICS TO ROLE FMG_ANALYST;

-- Future grants
GRANT SELECT ON FUTURE TABLES IN DATABASE FMG_PRODUCTION TO ROLE FMG_ANALYST;
GRANT SELECT ON FUTURE TABLES IN DATABASE FMG_ANALYTICS TO ROLE FMG_ANALYST;
```

**✅ Success Check**: Run `SHOW GRANTS TO ROLE FMG_ANALYST` to verify.

---

## Step 3: Provision Warehouses

### 3.1 Understand Warehouse Sizing

| Size | Credits/Hour | Best For |
|------|-------------|----------|
| X-Small | 1 | Development, simple queries |
| Small | 2 | Production dashboards |
| Medium | 4 | Analytics, BI tools |
| Large | 8 | ML/AI, complex queries |

### 3.2 Create Warehouses

```sql
USE ROLE SYSADMIN;

-- Development warehouse (cheap, fast suspend)
CREATE WAREHOUSE IF NOT EXISTS FMG_DEV_XS
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Development - suspends after 1 minute';

-- Production warehouse
CREATE WAREHOUSE IF NOT EXISTS FMG_PROD_S
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 120
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Production dashboards';

-- Analytics warehouse
CREATE WAREHOUSE IF NOT EXISTS FMG_ANALYTICS_M
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 180
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    ENABLE_QUERY_ACCELERATION = TRUE
    COMMENT = 'BI and analytics';

-- ML warehouse
CREATE WAREHOUSE IF NOT EXISTS FMG_ML_L
    WAREHOUSE_SIZE = 'LARGE'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'ML/AI workloads';

-- Verify
SHOW WAREHOUSES LIKE 'FMG%';
```

**✅ Success Check**: You should see 4+ FMG warehouses.

### 3.3 Grant Warehouse Access

```sql
-- Grant warehouse access to roles
GRANT USAGE ON WAREHOUSE FMG_DEV_XS TO ROLE FMG_ANALYST;
GRANT USAGE ON WAREHOUSE FMG_DEV_XS TO ROLE FMG_ENGINEER;
GRANT USAGE ON WAREHOUSE FMG_PROD_S TO ROLE FMG_ANALYST;
GRANT USAGE ON WAREHOUSE FMG_ANALYTICS_M TO ROLE FMG_ANALYST;
GRANT USAGE ON WAREHOUSE FMG_ML_L TO ROLE FMG_DATA_SCIENTIST;
```

---

## Step 4: Test Role Switching

### 4.1 Test as Analyst

```sql
-- Switch to analyst role
USE ROLE FMG_ANALYST;
USE WAREHOUSE FMG_DEV_XS;
USE SECONDARY ROLES NONE;  -- Only use FMG_ANALYST privileges

-- This should work (read access)
SELECT COUNT(*) AS customer_count 
FROM FMG_PRODUCTION.RAW.CUSTOMERS;

-- This should FAIL (no write access)
-- INSERT INTO FMG_PRODUCTION.RAW.CUSTOMERS (customer_id) VALUES ('TEST');
```

### 4.2 Test as Admin

```sql
-- Switch to admin role
USE ROLE FMG_ADMIN;

-- This should work (full access)
SELECT segment, COUNT(*) AS customers
FROM FMG_PRODUCTION.RAW.CUSTOMERS
GROUP BY segment;
```

**✅ Success Check**: Analyst can read but not write. Admin can do both.

---

## Step 5: Configure Data Sharing

### 5.1 Create Secure Views

Secure views hide the underlying query logic from consumers:

```sql
USE ROLE FMG_ADMIN;
USE DATABASE FMG_PRODUCTION;
USE SCHEMA RAW;

-- Create a secure view for sharing
CREATE OR REPLACE SECURE VIEW V_SHARED_CUSTOMER_SUMMARY AS
SELECT 
    customer_id,
    company_name,
    segment,
    industry,
    state,
    account_status,
    DATEDIFF('month', created_date, CURRENT_DATE()) AS tenure_months
FROM CUSTOMERS
WHERE account_status IN ('Active', 'Paused');

-- Test the view
SELECT * FROM V_SHARED_CUSTOMER_SUMMARY LIMIT 10;
```

### 5.2 Create a Share

```sql
USE ROLE ACCOUNTADMIN;

-- Create the share
CREATE SHARE IF NOT EXISTS FMG_ANALYTICS_SHARE
    COMMENT = 'FMG analytics data for internal sharing';

-- Grant access to objects
GRANT USAGE ON DATABASE FMG_PRODUCTION TO SHARE FMG_ANALYTICS_SHARE;
GRANT USAGE ON SCHEMA FMG_PRODUCTION.RAW TO SHARE FMG_ANALYTICS_SHARE;
GRANT SELECT ON VIEW FMG_PRODUCTION.RAW.V_SHARED_CUSTOMER_SUMMARY 
    TO SHARE FMG_ANALYTICS_SHARE;

-- Verify
SHOW GRANTS TO SHARE FMG_ANALYTICS_SHARE;
```

**✅ Success Check**: The share shows grants on database, schema, and view.

---

## Step 6: Hands-On Exercise

### Challenge: Create a CS Team Role

Create a new role called `FMG_CS_OPS` for the Customer Success Operations team with:
- Read access to CUSTOMERS and SUBSCRIPTIONS
- Read/write access to SUPPORT_TICKETS
- Access to FMG_PROD_S warehouse

<details>
<summary>💡 Click for Solution</summary>

```sql
USE ROLE ACCOUNTADMIN;

-- Create role
CREATE ROLE IF NOT EXISTS FMG_CS_OPS 
    COMMENT = 'Customer Success Operations';

-- Role hierarchy
GRANT ROLE FMG_VIEWER TO ROLE FMG_CS_OPS;
GRANT ROLE FMG_CS_OPS TO ROLE FMG_ADMIN;

-- Database access
GRANT USAGE ON DATABASE FMG_PRODUCTION TO ROLE FMG_CS_OPS;
GRANT USAGE ON SCHEMA FMG_PRODUCTION.RAW TO ROLE FMG_CS_OPS;

-- Table access
GRANT SELECT ON TABLE FMG_PRODUCTION.RAW.CUSTOMERS TO ROLE FMG_CS_OPS;
GRANT SELECT ON TABLE FMG_PRODUCTION.RAW.SUBSCRIPTIONS TO ROLE FMG_CS_OPS;
GRANT SELECT, INSERT, UPDATE ON TABLE FMG_PRODUCTION.RAW.SUPPORT_TICKETS TO ROLE FMG_CS_OPS;

-- Warehouse access
GRANT USAGE ON WAREHOUSE FMG_PROD_S TO ROLE FMG_CS_OPS;
```

</details>

---

## Summary

In this lab, you learned how to:

| Topic | What You Did |
|-------|--------------|
| **Roles** | Created 6 custom roles with hierarchy |
| **Warehouses** | Provisioned 4 warehouses for different workloads |
| **Permissions** | Granted database and warehouse access |
| **Sharing** | Created secure views and a data share |

---

## Next Steps

👉 **[Continue to Lab 2: Governance + FinOps →](../lab2-governance-finops/README.md)**

In Lab 2, you'll learn to:
- Tag and classify sensitive data
- Set up resource monitors and budgets
- Query audit logs for compliance
- Implement row-level security and masking
