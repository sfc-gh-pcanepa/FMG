# Lab 4: Snowflake AI & Cortex Agents

**Time**: ~30 minutes  
**Prerequisites**: Data share consumed (`FMG_SHARED_DATA` database exists), Cortex enabled

⚠️ **This lab is INDEPENDENT** - run it in any order!

## What You'll Build

| Feature | Why It Matters |
|---------|---------------|
| **Cortex LLM Functions** | AI in SQL - sentiment, summarization, generation |
| **Cortex Search** | Semantic search - understands meaning, not just keywords |
| **Semantic View** | Natural language queries over structured data |
| **Cortex Agent** | Unified AI assistant combining all capabilities |

## Quick Start

1. Accept the data share and create `FMG_SHARED_DATA` database
2. Open Snowsight and create a new SQL Worksheet
3. Copy/paste `lab4_complete.sql`
4. Run each section and observe the results

## The "Wow" Moments

### AI in SQL (One Line!)
```sql
-- Sentiment analysis
SELECT 
    feedback_text,
    SNOWFLAKE.CORTEX.SENTIMENT(feedback_text) AS sentiment
FROM CUSTOMER_FEEDBACK;

-- AI-generated responses
SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2',
    'Write a response to: ' || feedback_text
) AS ai_response
FROM CUSTOMER_FEEDBACK;
```

### Semantic Search (Understands Meaning!)
```sql
-- "emails not working" matches "Troubleshooting Email Delivery"
SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'KB_SEARCH',
    '{"query": "my emails are not being received"}'
);
```

### Natural Language → SQL
```sql
-- Semantic View enables business users to ask:
-- "What is our total MRR by segment?"
-- And get real SQL results!
```

### Cortex Agent (Built in Snowsight UI)
Combines all tools into one AI assistant that:
- Routes questions to the right tool automatically
- Queries structured data via Semantic View
- Searches unstructured content via Cortex Search
- All data stays in Snowflake - secure and governed

## Sample Questions for Your Agent

1. "What is our total MRR by segment?"
2. "Show me feedback from unhappy customers"
3. "How do I troubleshoot email delivery issues?"
4. "Which customers have the most products?"
5. "What are customers saying about MyRepChat?"

## Key Takeaways

- **AI/ML is built into the platform** - no external tools needed
- **Semantic Search** understands intent, not just keywords
- **Natural language queries** over structured data
- **All data stays in Snowflake** - secure and governed

## Other Labs

All labs are independent - try any of them!

- [Lab 1: Getting Started](../lab1-getting-started/)
- [Lab 2: Governance & FinOps](../lab2-governance-finops/)
- [Lab 3: Medallion Architecture](../lab3-transformations/)
