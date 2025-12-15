# Lab 4: Snowflake Agents & Intelligence

---

## Overview

**Duration**: 90 minutes  
**Level**: Advanced

In this lab, you'll build an AI-powered intelligence layer using Cortex LLM functions, Semantic Views, Cortex Search, and Cortex Agents.

### What You'll Learn

- ✅ Use Cortex LLM functions (sentiment, summarize, complete)
- ✅ Create Semantic Views for Cortex Analyst
- ✅ Build a Cortex Search service for documentation
- ✅ Create Cortex Agents that combine Analyst + Search

### Prerequisites

- [ ] Completed Labs 1-3
- [ ] Cortex enabled on your Snowflake account
- [ ] `FMG_ADMIN` role access

---

## Step 1: Cortex LLM Functions (20 min)

### 1.1 Set Up Context

```sql
USE ROLE FMG_ADMIN;
USE WAREHOUSE FMG_ML_L;
USE DATABASE FMG_ANALYTICS;

CREATE SCHEMA IF NOT EXISTS AI;
USE SCHEMA AI;
```

### 1.2 Sentiment Analysis

Analyze the sentiment of customer feedback:

```sql
-- Analyze NPS feedback sentiment
SELECT 
    response_id,
    nps_score,
    feedback_text,
    SNOWFLAKE.CORTEX.SENTIMENT(feedback_text) AS sentiment_score,
    CASE 
        WHEN SNOWFLAKE.CORTEX.SENTIMENT(feedback_text) > 0.3 THEN 'Positive'
        WHEN SNOWFLAKE.CORTEX.SENTIMENT(feedback_text) < -0.3 THEN 'Negative'
        ELSE 'Neutral'
    END AS sentiment_label
FROM FMG_PRODUCTION.RAW.NPS_RESPONSES
WHERE feedback_text IS NOT NULL
LIMIT 10;
```

**✅ Success Check**: Sentiment scores range from -1 to 1.

### 1.3 Text Summarization

```sql
-- Summarize customer feedback
WITH feedback AS (
    SELECT LISTAGG(feedback_text, '. ') AS all_feedback
    FROM FMG_PRODUCTION.RAW.NPS_RESPONSES
    WHERE feedback_text IS NOT NULL
    LIMIT 20
)
SELECT SNOWFLAKE.CORTEX.SUMMARIZE(all_feedback) AS summary
FROM feedback;
```

### 1.4 Text Generation

```sql
-- Generate customer outreach message
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-large',
    'Write a brief, friendly email to a financial advisor customer whose engagement has dropped. 
    Offer to schedule a training session. Keep it under 100 words.'
) AS outreach_email;
```

---

## Step 2: Create Semantic Views (25 min)

### 2.1 What are Semantic Views?

Semantic Views map business terms to data, enabling natural language queries:

```
User: "What is our MRR by segment?"
        │
        ▼
┌─────────────────────┐
│   CORTEX ANALYST    │
│  Uses Semantic View │
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│   SV_REVENUE        │
│  • mrr              │
│  • customer_segment │
└─────────────────────┘
```

### 2.2 Create Base Views

```sql
-- Revenue base view
CREATE OR REPLACE VIEW V_REVENUE_BASE AS
SELECT 
    s.subscription_id,
    s.customer_id,
    c.company_name AS customer_name,
    c.segment AS customer_segment,
    c.industry,
    s.product_name,
    s.plan_tier,
    s.status AS subscription_status,
    s.mrr_amount AS mrr,
    s.arr_amount AS arr,
    s.start_date,
    DATE_TRUNC('month', s.start_date) AS cohort_month
FROM FMG_PRODUCTION.RAW.SUBSCRIPTIONS s
JOIN FMG_PRODUCTION.RAW.CUSTOMERS c ON s.customer_id = c.customer_id;

-- Customer base view  
CREATE OR REPLACE VIEW V_CUSTOMER_BASE AS
SELECT 
    c.customer_id,
    c.company_name AS customer_name,
    c.segment AS customer_segment,
    c.industry,
    c.account_status,
    c.csm_owner,
    DATEDIFF('month', c.created_date, CURRENT_DATE()) AS tenure_months,
    h.overall_health_score,
    h.churn_risk
FROM FMG_PRODUCTION.RAW.CUSTOMERS c
LEFT JOIN (
    SELECT * FROM FMG_PRODUCTION.RAW.CUSTOMER_HEALTH_SCORES
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY snapshot_date DESC) = 1
) h ON c.customer_id = h.customer_id;
```

### 2.3 Create Semantic Views

```sql
-- Revenue semantic view
CREATE OR REPLACE SEMANTIC VIEW SV_REVENUE
    COMMENT = 'Revenue analytics - use for MRR, ARR, product, and subscription questions'
AS
SELECT 
    subscription_id COMMENT 'Unique subscription ID',
    customer_id COMMENT 'Customer account ID',
    customer_name COMMENT 'Company name',
    customer_segment COMMENT 'Segment: SMB, Mid-Market, Enterprise',
    industry COMMENT 'Customer industry',
    product_name COMMENT 'Product: Marketing Suite, Website Pro, MyRepChat, Do It For Me',
    plan_tier COMMENT 'Plan level: Starter, Professional, Enterprise',
    subscription_status COMMENT 'Active, Cancelled, Pending',
    mrr COMMENT 'Monthly Recurring Revenue in dollars',
    arr COMMENT 'Annual Recurring Revenue in dollars',
    start_date COMMENT 'Subscription start date',
    cohort_month COMMENT 'Month customer started for cohort analysis'
FROM V_REVENUE_BASE;

-- Customer semantic view
CREATE OR REPLACE SEMANTIC VIEW SV_CUSTOMERS
    COMMENT = 'Customer analytics - use for customer, health, and churn questions'
AS
SELECT 
    customer_id COMMENT 'Customer account ID',
    customer_name COMMENT 'Company name',
    customer_segment COMMENT 'Segment: SMB, Mid-Market, Enterprise',
    industry COMMENT 'Customer industry',
    account_status COMMENT 'Active, Churned, Paused, Trial',
    csm_owner COMMENT 'Customer Success Manager name',
    tenure_months COMMENT 'Months as a customer',
    overall_health_score COMMENT 'Health score 0-100',
    churn_risk COMMENT 'Risk level: Low, Medium, High, Critical'
FROM V_CUSTOMER_BASE;

-- Verify
SHOW SEMANTIC VIEWS;
```

### 2.4 Test Semantic Views

```sql
-- These will be queried by Cortex Analyst
SELECT customer_segment, SUM(mrr) AS total_mrr
FROM SV_REVENUE WHERE subscription_status = 'Active'
GROUP BY 1 ORDER BY 2 DESC;

SELECT churn_risk, COUNT(*) AS customers
FROM SV_CUSTOMERS WHERE account_status = 'Active'
GROUP BY 1;
```

**✅ Success Check**: Semantic views return data correctly.

---

## Step 3: Create Cortex Search (15 min)

### 3.1 Create Knowledge Base

```sql
-- Support knowledge base table
CREATE OR REPLACE TABLE SUPPORT_KB (
    article_id VARCHAR PRIMARY KEY,
    title VARCHAR,
    category VARCHAR,
    product VARCHAR,
    content TEXT
);

-- Insert sample articles
INSERT INTO SUPPORT_KB VALUES
    ('KB-001', 'How to Set Up Email Campaigns', 'Email Marketing', 'Marketing Suite',
     'Step-by-step guide to creating email campaigns. Navigate to Marketing Tools, click Create Campaign, choose a template, customize content, select recipients, and send.'),
    ('KB-002', 'Troubleshooting Email Delivery', 'Email Marketing', 'Marketing Suite', 
     'If emails are not delivering, check SPF/DKIM settings, verify recipient addresses, review bounce reports, and ensure content is not triggering spam filters.'),
    ('KB-003', 'Getting Started with MyRepChat', 'MyRepChat', 'MyRepChat',
     'Download the MyRepChat app, log in with FMG credentials, complete compliance acknowledgment, and start messaging clients. All messages are archived.'),
    ('KB-004', 'Website Analytics Dashboard', 'Website', 'Website Pro',
     'Access analytics via Website > Analytics. View page views, visitors, session duration, leads, and conversions. Export reports for records.'),
    ('KB-005', 'Scheduling Social Media Posts', 'Social Media', 'Marketing Suite',
     'Go to Social Media, create or choose content, select platforms, click Schedule instead of Post Now, pick date/time. View scheduled posts in Calendar.');
```

### 3.2 Create Search Service

```sql
-- Create Cortex Search service
CREATE OR REPLACE CORTEX SEARCH SERVICE FMG_KB_SEARCH
    ON content
    ATTRIBUTES category, product, title
    WAREHOUSE = FMG_ML_L
    TARGET_LAG = '1 hour'
AS (
    SELECT article_id, title, category, product, content
    FROM SUPPORT_KB
);
```

### 3.3 Test Search

```sql
-- Search the knowledge base
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'FMG_ANALYTICS.AI.FMG_KB_SEARCH',
        '{
            "query": "emails not being received",
            "columns": ["article_id", "title", "category"],
            "limit": 3
        }'
    )
) AS results;
```

**✅ Success Check**: Search returns relevant articles.

---

## Step 4: Create Cortex Agents (20 min)

### 4.1 What are Cortex Agents?

Agents combine multiple tools (Analyst + Search) into one interface:

```
┌─────────────────────────────────────────────────────────────────┐
│                      CORTEX AGENT                                │
│                                                                  │
│  User: "Show at-risk customers and how to retain them"         │
│                                                                  │
│         ┌──────────────┐        ┌──────────────┐               │
│         │   ANALYST    │        │   SEARCH     │               │
│         │ Query data   │        │ Find docs    │               │
│         └──────────────┘        └──────────────┘               │
│                                                                  │
│  Response: "5 critical-risk customers [table]                   │
│            Retention resources: KB-003, KB-001..."              │
└─────────────────────────────────────────────────────────────────┘
```

### 4.2 Create the FMG Agent

```sql
-- Create unified agent
CREATE OR REPLACE CORTEX AGENT FMG_INTELLIGENCE_AGENT
    DISPLAY_NAME = 'FMG Intelligence Assistant'
    DESCRIPTION = 'Ask about FMG data or search our knowledge base'
    MODEL = 'claude-3-5-sonnet'
    
    SYSTEM_PROMPT = 'You are the FMG Intelligence Assistant.

For DATA questions (MRR, customers, usage): Use the Analyst tool with semantic views.
For HELP questions (how-to, troubleshooting): Use the Search tool for KB articles.

Be concise. Format data as tables. Cite KB articles by ID.'
    
    TOOLS = (
        CORTEX_ANALYST (
            SEMANTIC_VIEWS => (
                FMG_ANALYTICS.AI.SV_REVENUE,
                FMG_ANALYTICS.AI.SV_CUSTOMERS
            )
        ),
        CORTEX_SEARCH (
            SEARCH_SERVICE => 'FMG_ANALYTICS.AI.FMG_KB_SEARCH'
        )
    );
```

### 4.3 Create Specialized Agents

```sql
-- Customer Success Agent
CREATE OR REPLACE CORTEX AGENT FMG_CS_AGENT
    DISPLAY_NAME = 'CS Assistant'
    DESCRIPTION = 'Customer health and retention insights'
    MODEL = 'claude-3-5-sonnet'
    SYSTEM_PROMPT = 'Help CSMs understand customer health and find retention resources. Focus on churn risk and engagement.'
    TOOLS = (
        CORTEX_ANALYST (SEMANTIC_VIEWS => (FMG_ANALYTICS.AI.SV_CUSTOMERS)),
        CORTEX_SEARCH (SEARCH_SERVICE => 'FMG_ANALYTICS.AI.FMG_KB_SEARCH')
    );

-- Sales Agent  
CREATE OR REPLACE CORTEX AGENT FMG_SALES_AGENT
    DISPLAY_NAME = 'Sales Assistant'
    DESCRIPTION = 'Revenue and pipeline insights'
    MODEL = 'claude-3-5-sonnet'
    SYSTEM_PROMPT = 'Help sales understand MRR, ARR, and product adoption. Focus on revenue metrics.'
    TOOLS = (
        CORTEX_ANALYST (SEMANTIC_VIEWS => (FMG_ANALYTICS.AI.SV_REVENUE))
    );

-- Verify
SHOW CORTEX AGENTS;
```

### 4.4 Test the Agents

```sql
-- Test data question
SELECT SNOWFLAKE.CORTEX.AGENT(
    'FMG_ANALYTICS.AI.FMG_INTELLIGENCE_AGENT',
    'What is our total MRR by segment?'
) AS response;

-- Test search question
SELECT SNOWFLAKE.CORTEX.AGENT(
    'FMG_ANALYTICS.AI.FMG_INTELLIGENCE_AGENT',
    'How do I troubleshoot email delivery issues?'
) AS response;

-- Test combined question
SELECT SNOWFLAKE.CORTEX.AGENT(
    'FMG_ANALYTICS.AI.FMG_INTELLIGENCE_AGENT',
    'Which customers have critical churn risk, and what resources can help?'
) AS response;
```

**✅ Success Check**: Agent answers data and search questions correctly.

---

## Step 5: Hands-On Exercise (10 min)

### Challenge: Add a Usage Semantic View

Create `SV_USAGE` for platform usage questions and add it to the main agent:

<details>
<summary>💡 Click for Solution</summary>

```sql
-- Create usage base view
CREATE OR REPLACE VIEW V_USAGE_BASE AS
SELECT 
    usage_date,
    customer_id,
    c.company_name AS customer_name,
    c.segment AS customer_segment,
    emails_sent,
    social_posts_published AS social_posts,
    website_leads_generated AS leads,
    total_logins AS logins
FROM FMG_PRODUCTION.RAW.PLATFORM_USAGE_DAILY u
JOIN FMG_PRODUCTION.RAW.CUSTOMERS c ON u.customer_id = c.customer_id;

-- Create semantic view
CREATE OR REPLACE SEMANTIC VIEW SV_USAGE
    COMMENT = 'Usage analytics - for engagement and feature adoption questions'
AS
SELECT 
    usage_date COMMENT 'Date of activity',
    customer_id COMMENT 'Customer ID',
    customer_name COMMENT 'Company name',
    customer_segment COMMENT 'Segment',
    emails_sent COMMENT 'Emails sent that day',
    social_posts COMMENT 'Social posts published',
    leads COMMENT 'Website leads captured',
    logins COMMENT 'Platform logins'
FROM V_USAGE_BASE;

-- Update agent (recreate with new semantic view)
CREATE OR REPLACE CORTEX AGENT FMG_INTELLIGENCE_AGENT
    DISPLAY_NAME = 'FMG Intelligence Assistant'
    MODEL = 'claude-3-5-sonnet'
    SYSTEM_PROMPT = 'FMG assistant for data and documentation.'
    TOOLS = (
        CORTEX_ANALYST (
            SEMANTIC_VIEWS => (
                FMG_ANALYTICS.AI.SV_REVENUE,
                FMG_ANALYTICS.AI.SV_CUSTOMERS,
                FMG_ANALYTICS.AI.SV_USAGE
            )
        ),
        CORTEX_SEARCH (SEARCH_SERVICE => 'FMG_ANALYTICS.AI.FMG_KB_SEARCH')
    );
```

</details>

---

## Summary

In this lab, you learned:

| Topic | What You Built |
|-------|---------------|
| **LLM Functions** | Sentiment, summarization, text generation |
| **Semantic Views** | SV_REVENUE, SV_CUSTOMERS for natural language |
| **Search Service** | FMG_KB_SEARCH for documentation |
| **Agents** | FMG_INTELLIGENCE_AGENT, FMG_CS_AGENT, FMG_SALES_AGENT |

---

## 🎉 Workshop Complete!

Congratulations! You've built a complete Snowflake data platform with:

| Lab | What You Built |
|-----|---------------|
| **Lab 1** | Roles, warehouses, data sharing |
| **Lab 2** | Tags, budgets, masking, RLS |
| **Lab 3** | Streams, tasks, dynamic tables, cloning |
| **Lab 4** | Cortex AI, semantic views, agents |

### Next Steps for FMG

1. **Deploy to Production**: Migrate real data sources
2. **Expand Agents**: Add more semantic views and KB articles
3. **Build Streamlit Apps**: Create user interfaces for agents
4. **Monitor & Iterate**: Track usage and improve prompts

---

## Resources

- [Cortex Documentation](https://docs.snowflake.com/en/guides-overview-ai-features)
- [Semantic Views](https://docs.snowflake.com/en/user-guide/views-semantic/overview)
- [Cortex Agents](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents)
- [Snowflake Quickstarts](https://quickstarts.snowflake.com/)
