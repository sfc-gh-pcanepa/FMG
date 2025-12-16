# Lab 2: Governance & FinOps

**Time**: ~20 minutes  
**Prerequisites**: Lab 1 completed

## What You'll See

| Feature | Why It Matters |
|---------|---------------|
| **Data Tagging** | Classify sensitive data instantly |
| **Dynamic Masking** | Same query, different results by role |
| **Resource Monitors** | Budget alerts and auto-suspend |
| **Audit History** | Built-in compliance trail |

## Quick Start

1. Open Snowsight and create a new SQL Worksheet
2. Copy/paste `lab2_complete.sql`
3. Run each section and observe the results

## The "Wow" Moment

Run the same `SELECT` query as two different roles:

```sql
-- As ADMIN: See john.smith@acmefinancial.com
-- As ANALYST: See ****@****.***
```

**No code changes needed** - security follows the data automatically!

## Key Takeaways

- **Governance is built-in**: Not a separate tool to buy
- **Masking is automatic**: Define once, enforced everywhere
- **Cost control is easy**: Set budgets, get alerts, auto-stop
- **Audit is free**: 365 days of query history included

## Next Lab

→ [Lab 3: Data Transformations](../lab3-transformations/)
