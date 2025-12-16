# FMG Suite - Snowflake Hands-On Labs

## Quick Start

| Lab | Topic | Time | Key Demos |
|-----|-------|------|-----------|
| [Lab 1](lab1-getting-started/) | Getting Started | 20 min | Roles, Warehouses, RBAC |
| [Lab 2](lab2-governance-finops/) | Governance | 20 min | Masking, Tags, Cost Control |
| [Lab 3](lab3-transformations/) | Transformations | 20 min | Dynamic Tables, Cloning |
| [Lab 4](lab4-agents-intelligence/) | AI & Cortex | 20 min | Sentiment, Search, LLMs |

## How to Run

1. **Open Snowsight** in your Snowflake account
2. **Create a new SQL Worksheet**
3. **Copy/paste** the `lab#_complete.sql` file
4. **Run each section** and observe the results

## What You'll See

### Lab 1: Getting Started
- Create roles with different privileges in seconds
- Separate compute for different workloads
- Instant warehouse resizing with zero downtime

### Lab 2: Governance
- **Dynamic Masking**: Same query returns different results by role
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

## Prerequisites

- Snowflake account (trial works fine)
- `ACCOUNTADMIN` role access
- For Lab 4: Cortex enabled on your account

## Files

```
FMG/
├── lab1-getting-started/
│   └── lab1_complete.sql     ← Run this for Lab 1
├── lab2-governance-finops/
│   └── lab2_complete.sql     ← Run this for Lab 2
├── lab3-transformations/
│   └── lab3_complete.sql     ← Run this for Lab 3
├── lab4-agents-intelligence/
│   └── lab4_complete.sql     ← Run this for Lab 4
└── setup/
    └── 99_cleanup.sql        ← Optional: Clean up after labs
```

---

**Questions?** Contact your Snowflake account team.
