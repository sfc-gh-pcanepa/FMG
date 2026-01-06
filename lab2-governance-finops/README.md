# Lab 2: Governance & FinOps

**Time**: ~20 minutes  
**Prerequisites**: Data share consumed (`FMG_SHARED_DATA` database exists)

⚠️ **This lab is INDEPENDENT** - run it in any order!

## What You'll See

| Feature | Why It Matters |
|---------|---------------|
| **Data Tagging** | Classify sensitive data in seconds |
| **Dynamic Masking** | Same query, different results by role |
| **Resource Monitors** | Budget alerts and auto-suspend |
| **Audit History** | Built-in compliance trail |

## Quick Start

1. Accept the data share and create `FMG_SHARED_DATA` database
2. Open Snowsight and create a new SQL Worksheet
3. Copy/paste `lab2_complete.sql`
4. Run each section and observe the results

## The "Wow" Moments

### Dynamic Data Masking
```sql
-- Same query, different results!

-- As ADMIN:
SELECT email, phone FROM USERS;
-- john.smith@acme.com, (555) 123-4567

-- As ANALYST:
SELECT email, phone FROM USERS;
-- ****@acme.com, (***) ***-4567
```
No code changes needed - security follows the user's role!

### Resource Monitors
```sql
CREATE RESOURCE MONITOR FMG_BUDGET
    WITH CREDIT_QUOTA = 100
    TRIGGERS
        ON 75 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND;
```
Never get surprised by a bill again.

## Key Takeaways

- **Governance is built-in**, not bolted-on
- **Security policies follow the data** automatically
- **Cost control** with budget alerts and auto-suspend
- **Complete audit trail** for compliance (365 days)

## Other Labs

All labs are independent - try any of them!

- [Lab 1: Getting Started](../lab1-getting-started/)
- [Lab 3: Medallion Architecture](../lab3-transformations/)
- [Lab 4: AI & Cortex](../lab4-agents-intelligence/)
