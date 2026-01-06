# Lab 1: Getting Started with Snowflake

**Time**: ~20 minutes  
**Prerequisites**: Data share consumed (`FMG_SHARED_DATA` database exists)

⚠️ **This lab is INDEPENDENT** - run it in any order!

## What You'll See

| Feature | Why It Matters |
|---------|---------------|
| **Data Sharing** | Instant access to shared data - no copies! |
| **Separation of Compute** | Independent warehouses for different workloads |
| **Instant Resizing** | Scale up/down in seconds with zero downtime |
| **Role-Based Access** | Analyst can read, Engineer can write |

## Quick Start

1. Accept the data share and create `FMG_SHARED_DATA` database
2. Open Snowsight and create a new SQL Worksheet
3. Copy/paste `lab1_complete.sql`
4. Run each section and observe the results

## The "Wow" Moments

### Data Sharing (Zero-Copy!)
```sql
-- Instant access to shared data - no ETL, no copies
CREATE TABLE CUSTOMERS AS SELECT * FROM FMG_SHARED_DATA.FMG.CUSTOMERS;
```

### Instant Resizing
```sql
ALTER WAREHOUSE FMG_ANALYTICS_WH SET WAREHOUSE_SIZE = 'MEDIUM';
-- Done in seconds! Queries keep running!
```

### Role-Based Access Control
```sql
-- As ANALYST: Can read
SELECT * FROM CUSTOMERS;  -- ✅ Works

-- As ANALYST: Cannot write
INSERT INTO CUSTOMERS...;  -- ❌ Denied
```

## Key Takeaways

- **Data Sharing** = instant access, always current, no copies
- **Separation of Compute** = no resource contention between workloads
- **Instant Resizing** = scale on demand, pay only for what you use
- **RBAC** = security is simple and intuitive

## Other Labs

All labs are independent - try any of them!

- [Lab 2: Governance & FinOps](../lab2-governance-finops/)
- [Lab 3: Medallion Architecture](../lab3-transformations/)
- [Lab 4: AI & Cortex](../lab4-agents-intelligence/)
