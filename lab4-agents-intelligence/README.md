# Lab 4: Snowflake AI & Cortex

**Time**: ~20 minutes  
**Prerequisites**: Labs 1-3 completed, Cortex enabled on account

## What You'll See

| Feature | Why It Matters |
|---------|---------------|
| **Sentiment Analysis** | One function call - no ML expertise needed |
| **Text Summarization** | Summarize feedback, documents instantly |
| **AI Text Generation** | Generate responses, emails, content |
| **Cortex Search** | Semantic search that understands meaning |

## Quick Start

1. Open Snowsight and create a new SQL Worksheet
2. Copy/paste `lab4_complete.sql`
3. Run each section and observe the results

## The "Wow" Moments

### Sentiment Analysis in SQL
```sql
SELECT 
    feedback_text,
    SNOWFLAKE.CORTEX.SENTIMENT(feedback_text) AS score
FROM customer_feedback;
```
No Python. No external APIs. Just SQL.

### Semantic Search
```sql
-- Search by MEANING, not keywords
-- Query: "my emails are not being received"
-- Finds: "Troubleshooting Email Delivery" article
```

### AI-Generated Responses
```sql
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-large',
    'Write a response to this unhappy customer: ...'
) AS suggested_response;
```

## Key Takeaways

- **AI is built into SQL** - no external tools needed
- **Your data stays in Snowflake** - governed and secure
- **Semantic search understands context** - not just keywords
- **Pay per use** - no GPU management or ML infrastructure

## Workshop Complete! 🎉

You've seen:
- ✅ Instant role and warehouse provisioning
- ✅ Built-in governance and cost control
- ✅ Auto-refreshing transformations
- ✅ AI/ML in SQL

**Next Steps**: Talk to your Snowflake team about a proof of concept!
