# Lab 3: Data Transformations

**Time**: ~20 minutes  
**Prerequisites**: Labs 1-2 completed

## What You'll See

| Feature | Why It Matters |
|---------|---------------|
| **Dynamic Tables** | Auto-refreshing aggregations - no ETL code! |
| **Zero-Copy Cloning** | Instant dev environments, no extra storage |
| **Time Travel** | Query and recover historical data |
| **Undrop** | Recover accidentally dropped objects |

## Quick Start

1. Open Snowsight and create a new SQL Worksheet
2. Copy/paste `lab3_complete.sql`
3. Run each section and observe the results

## The "Wow" Moments

### Dynamic Tables
```sql
CREATE DYNAMIC TABLE customer_360
    TARGET_LAG = '1 minute'  -- Auto-refresh!
AS SELECT ...
```
No cron jobs. No Airflow. No maintenance.

### Zero-Copy Cloning
```sql
CREATE DATABASE dev_copy CLONE production;
-- Instant! Even for terabytes of data
```

### Time Travel
```sql
SELECT * FROM customers AT(OFFSET => -3600);
-- Query data from 1 hour ago
```

## Key Takeaways

- **Dynamic Tables eliminate ETL complexity**
- **Cloning is instant** regardless of data size
- **Time Travel = built-in disaster recovery**
- **No extra cost** for clones (pay only for changes)

## Next Lab

→ [Lab 4: AI & Cortex](../lab4-agents-intelligence/)
