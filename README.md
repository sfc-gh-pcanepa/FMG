# FMG Suite - Snowflake Hands-On Labs
## Internal Reporting & Analytics Platform

---

## Overview

**Level**: Beginner to Advanced  
**Snowflake Edition**: Enterprise or higher recommended

Welcome to the **FMG Suite Snowflake Workshop Series**. This quickstart guide walks you through building an enterprise data platform using Snowflake, with data modeled after FMG's internal business operations.

### What You'll Build

By the end of this workshop, you'll have:

- ✅ A complete Snowflake environment with roles, warehouses, and databases
- ✅ Governance policies including cost controls and data masking
- ✅ Real-time data pipelines using Streams, Tasks, and Dynamic Tables
- ✅ AI-powered analytics with Cortex Analyst and Agents

### Prerequisites

Before starting, ensure you have:

- [ ] A Snowflake account (trial accounts work fine)
- [ ] `ACCOUNTADMIN` access for initial setup
- [ ] A modern web browser for Snowsight
- [ ] Basic SQL knowledge

---

## Workshop Sessions

| Session | Lab | Topics |
|---------|-----|--------|
| **1** | [Getting Started](lab1-getting-started/README.md) | Users, Roles, Warehouses, Data Sharing |
| **2** | [Governance + FinOps](lab2-governance-finops/README.md) | Horizon Catalog, Budgets, RBAC, Masking |
| **3** | [Transformations](lab3-transformations/README.md) | Streams, Tasks, Dynamic Tables, Cloning |
| **4** | [Agents & Intelligence](lab4-agents-intelligence/README.md) | Cortex AI, Semantic Views, Agents |

---

## Quick Setup

Before starting Lab 1, run the setup scripts to create the FMG environment.

### Step 1: Open Snowsight

Navigate to your Snowflake account and open **Snowsight** (the web UI).

### Step 2: Create a New Worksheet

Click **+ Worksheet** and name it "FMG Setup".

### Step 3: Run Environment Setup

Copy and paste the contents of `setup/00_environment_setup.sql` and execute.

```sql
-- Verify setup completed
SHOW DATABASES LIKE 'FMG%';
SHOW WAREHOUSES LIKE 'FMG%';
SHOW ROLES LIKE 'FMG%';
```

**Expected Output**: You should see 3 databases, 5 warehouses, and 6 custom roles.

### Step 4: Generate Sample Data

Copy and paste the contents of `setup/01_synthetic_data_setup.sql` and execute.

```sql
-- Verify data loaded
SELECT 'CUSTOMERS' AS table_name, COUNT(*) AS rows FROM FMG_PRODUCTION.RAW.CUSTOMERS
UNION ALL SELECT 'USERS', COUNT(*) FROM FMG_PRODUCTION.RAW.USERS
UNION ALL SELECT 'SUBSCRIPTIONS', COUNT(*) FROM FMG_PRODUCTION.RAW.SUBSCRIPTIONS;
```

**Expected Output**: ~500 customers, ~2500 users, ~1500 subscriptions.

---

## FMG Data Model

The sample data represents FMG's internal business operations:

```
┌─────────────────────────────────────────────────────────────────┐
│                    FMG INTERNAL DATA MODEL                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  CUSTOMERS ─────► SUBSCRIPTIONS ─────► REVENUE                  │
│      │                 │                                         │
│      ▼                 ▼                                         │
│   USERS           PLATFORM_USAGE                                 │
│      │                 │                                         │
│      ▼                 ▼                                         │
│  SUPPORT_TICKETS   CUSTOMER_HEALTH_SCORES                       │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

| Table | Records | Description |
|-------|---------|-------------|
| `CUSTOMERS` | 500 | FMG customer accounts (RIAs, broker-dealers) |
| `USERS` | 2,500 | Individual advisors within accounts |
| `SUBSCRIPTIONS` | 1,500 | Product subscriptions with MRR |
| `PLATFORM_USAGE_DAILY` | 100K+ | Daily feature usage metrics |
| `CUSTOMER_HEALTH_SCORES` | 6,000 | Weekly health score snapshots |
| `SUPPORT_TICKETS` | 5,000+ | Support interactions |
| `NPS_RESPONSES` | 500+ | Customer feedback |
| `SALES_LEADS` | 1,000 | Sales pipeline |

---

## Repository Structure

```
FMG/
├── README.md                    ← You are here
├── setup/
│   ├── 00_environment_setup.sql
│   └── 01_synthetic_data_setup.sql
│
├── lab1-getting-started/
│   ├── README.md               ← Start here for Lab 1
│   └── *.sql
│
├── lab2-governance-finops/
│   ├── README.md
│   └── *.sql
│
├── lab3-transformations/
│   ├── README.md
│   └── *.sql
│
└── lab4-agents-intelligence/
    ├── README.md
    └── *.sql
```

---

## Getting Help

- **Snowflake Documentation**: [docs.snowflake.com](https://docs.snowflake.com)
- **Quickstart Tutorials**: [quickstarts.snowflake.com](https://quickstarts.snowflake.com)
- **Community**: [community.snowflake.com](https://community.snowflake.com)

---

## Let's Get Started!

👉 **[Begin Lab 1: Getting Started with Snowflake →](lab1-getting-started/README.md)**
