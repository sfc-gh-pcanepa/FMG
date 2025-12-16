# FMG Suite - Snowflake Hands-On Labs

## Setup Flow

### For Data Provider (Snowflake Team)
1. Run `setup/data_provider_setup.sql` in your account
2. Add prospect's account to the share:
   ```sql
   ALTER SHARE FMG_LABS_SHARE ADD ACCOUNTS = <prospect_account_locator>;
   ```

### For Prospect (Trial Account)
1. Run `setup/prospect_setup.sql` to:
   - Create database from share
   - Set up roles and warehouses
   - Copy data for lab exercises

---

## Labs

| Lab | Topic | Time | Key Demos |
|-----|-------|------|-----------|
| [Lab 1](lab1-getting-started/) | Getting Started | 20 min | Warehouses, Roles, RBAC |
| [Lab 2](lab2-governance-finops/) | Governance | 20 min | Masking, Tags, Cost Control |
| [Lab 3](lab3-transformations/) | Transformations | 20 min | Dynamic Tables, Cloning |
| [Lab 4](lab4-agents-intelligence/) | AI & Cortex | 20 min | Sentiment, Search, LLMs |

---

## Running the Labs

1. Open **Snowsight** in your Snowflake account
2. Create a new **SQL Worksheet**
3. Copy/paste the `lab#_complete.sql` file
4. Run each section and observe results

---

## What You'll See

### Lab 1: Getting Started
- Two independent warehouses (separation of compute)
- Instant warehouse resizing with zero downtime
- RBAC in action - different roles, different access

### Lab 2: Governance
- **Dynamic Masking**: Same query, different results by role
- Tag sensitive data for classification
- Resource monitors prevent runaway costs

### Lab 3: Transformations
- **Dynamic Tables**: Auto-refreshing aggregations (no ETL!)
- **Zero-Copy Cloning**: Instant dev environments
- **Time Travel**: Query and recover historical data

### Lab 4: AI & Cortex
- Sentiment analysis in one SQL function
- Semantic search that understands meaning
- AI text generation for customer responses

---

## Files

```
FMG/
├── setup/
│   ├── data_provider_setup.sql  ← Run in YOUR account (creates share)
│   ├── prospect_setup.sql       ← Run in PROSPECT account
│   └── 99_cleanup.sql           ← Clean up after labs
│
├── lab1-getting-started/
│   └── lab1_complete.sql
├── lab2-governance-finops/
│   └── lab2_complete.sql
├── lab3-transformations/
│   └── lab3_complete.sql
└── lab4-agents-intelligence/
    └── lab4_complete.sql
```

---

**Questions?** Contact your Snowflake account team.
