/*=============================================================================
  FMG SUITE - LAB 4: SNOWFLAKE AI & CORTEX AGENTS
  
  What you'll build:
  ✅ Cortex LLM Functions - Sentiment analysis, summarization, AI responses
  ✅ Cortex Search Service - Semantic search over customer feedback
  ✅ Semantic View - Natural language queries over structured data
  ✅ Cortex Agent - Unified AI assistant combining both capabilities
  
  Time: ~30 minutes
  Prerequisites: Data share consumed (FMG_SHARED_DATA database exists)
  
  ⚠️  This lab is INDEPENDENT - run it in any order!
=============================================================================*/

-- ============================================================================
-- SETUP: CREATE LAB ENVIRONMENT FROM SHARED DATA
-- ============================================================================
USE ROLE ACCOUNTADMIN;

-- Create lab-specific database
CREATE DATABASE IF NOT EXISTS FMG_LAB4;
CREATE SCHEMA IF NOT EXISTS FMG_LAB4.PRODUCTION;

-- Create warehouse
CREATE WAREHOUSE IF NOT EXISTS FMG_ANALYTICS_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

-- Create roles
CREATE ROLE IF NOT EXISTS FMG_ADMIN;
CREATE ROLE IF NOT EXISTS FMG_ANALYST;
GRANT ROLE FMG_ADMIN TO ROLE ACCOUNTADMIN;
GRANT USAGE ON WAREHOUSE FMG_ANALYTICS_WH TO ROLE FMG_ADMIN;
GRANT USAGE ON WAREHOUSE FMG_ANALYTICS_WH TO ROLE FMG_ANALYST;
GRANT ALL ON DATABASE FMG_LAB4 TO ROLE FMG_ADMIN;
GRANT USAGE ON DATABASE FMG_LAB4 TO ROLE FMG_ANALYST;

USE WAREHOUSE FMG_ANALYTICS_WH;
USE SCHEMA FMG_LAB4.PRODUCTION;

-- Copy data from share
CREATE OR REPLACE TABLE CUSTOMERS AS SELECT * FROM FMG_SHARED_DATA.FMG.CUSTOMERS;
CREATE OR REPLACE TABLE SUBSCRIPTIONS AS SELECT * FROM FMG_SHARED_DATA.FMG.SUBSCRIPTIONS;
CREATE OR REPLACE TABLE CUSTOMER_FEEDBACK AS SELECT * FROM FMG_SHARED_DATA.FMG.CUSTOMER_FEEDBACK;
CREATE OR REPLACE TABLE KNOWLEDGE_BASE AS SELECT * FROM FMG_SHARED_DATA.FMG.KNOWLEDGE_BASE;

-- Grant access
GRANT SELECT ON ALL TABLES IN SCHEMA FMG_LAB4.PRODUCTION TO ROLE FMG_ANALYST;
GRANT ALL ON ALL TABLES IN SCHEMA FMG_LAB4.PRODUCTION TO ROLE FMG_ADMIN;

-- Verify data
SELECT 'CUSTOMERS' AS table_name, COUNT(*) AS row_count FROM CUSTOMERS
UNION ALL SELECT 'SUBSCRIPTIONS', COUNT(*) FROM SUBSCRIPTIONS
UNION ALL SELECT 'CUSTOMER_FEEDBACK', COUNT(*) FROM CUSTOMER_FEEDBACK
UNION ALL SELECT 'KNOWLEDGE_BASE', COUNT(*) FROM KNOWLEDGE_BASE;

-- ============================================================================
-- STEP 1: CORTEX LLM FUNCTIONS (AI Built Into SQL!)
-- ============================================================================

-- 1.1 Sentiment Analysis on Customer Feedback
SELECT 
    feedback_id,
    customer_id,
    nps_score,
    LEFT(feedback_text, 60) || '...' AS feedback_preview,
    ROUND(SNOWFLAKE.CORTEX.SENTIMENT(feedback_text), 3) AS sentiment_score,
    CASE 
        WHEN SNOWFLAKE.CORTEX.SENTIMENT(feedback_text) > 0.3 THEN '😊 Positive'
        WHEN SNOWFLAKE.CORTEX.SENTIMENT(feedback_text) < -0.3 THEN '😟 Negative'
        ELSE '😐 Neutral'
    END AS sentiment_label
FROM CUSTOMER_FEEDBACK
ORDER BY sentiment_score;

-- 🎯 Key insight: AI-powered sentiment analysis with ONE function call!

-- 1.2 Summarize All Customer Feedback
SELECT SNOWFLAKE.CORTEX.SUMMARIZE(
    'Summarize the key themes and concerns from this customer feedback: ' ||
    LISTAGG(feedback_text, ' | ') WITHIN GROUP (ORDER BY submitted_date)
) AS feedback_summary
FROM CUSTOMER_FEEDBACK;

-- 1.3 Generate Response to Negative Feedback
SELECT 
    feedback_id,
    nps_score,
    LEFT(feedback_text, 80) AS feedback_preview,
    SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large2',
        'You are a customer success manager at FMG, a marketing software company for financial advisors. ' ||
        'Write a brief, empathetic response (under 75 words) to this customer feedback. ' ||
        'Acknowledge their concern and offer a concrete next step: ' || feedback_text
    ) AS suggested_response
FROM CUSTOMER_FEEDBACK
WHERE nps_score <= 5;

-- ============================================================================
-- STEP 2: CORTEX SEARCH SERVICE (Semantic Search!)
-- ============================================================================

/*
  Cortex Search enables SEMANTIC search - it understands meaning, not just keywords.
  "emails not working" will match "email deliverability issues"
*/

-- 2.1 Create Cortex Search Service for Customer Feedback
CREATE OR REPLACE CORTEX SEARCH SERVICE FMG_LAB4.PRODUCTION.FEEDBACK_SEARCH
    ON feedback_text
    ATTRIBUTES customer_id, nps_score
    WAREHOUSE = FMG_ANALYTICS_WH
    TARGET_LAG = '1 hour'
AS (
    SELECT 
        feedback_id,
        customer_id,
        nps_score,
        feedback_text,
        submitted_date
    FROM CUSTOMER_FEEDBACK
);

-- 2.2 Create Cortex Search Service for Knowledge Base
CREATE OR REPLACE CORTEX SEARCH SERVICE FMG_LAB4.PRODUCTION.KB_SEARCH
    ON content
    ATTRIBUTES title, category
    WAREHOUSE = FMG_ANALYTICS_WH
    TARGET_LAG = '1 hour'
AS (
    SELECT 
        article_id,
        title,
        content,
        category
    FROM KNOWLEDGE_BASE
);

-- 2.3 Test Semantic Search on Feedback
-- Notice: We search for "pricing" and it finds feedback about "price increase"
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'FMG_LAB4.PRODUCTION.FEEDBACK_SEARCH',
        '{
            "query": "customers unhappy about pricing or cost",
            "columns": ["customer_id", "feedback_text", "nps_score"],
            "limit": 3
        }'
    )
):results AS pricing_concerns;

-- 2.4 Test Semantic Search on Knowledge Base
-- Notice: "cant send emails" matches "Troubleshooting Email Delivery"
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'FMG_LAB4.PRODUCTION.KB_SEARCH',
        '{
            "query": "my emails are not being received by clients",
            "columns": ["title", "content", "category"],
            "limit": 2
        }'
    )
):results AS help_articles;

-- 🎯 Key insight: Semantic search understands INTENT, not just keywords!

-- ============================================================================
-- STEP 3: CREATE SEMANTIC VIEW (Natural Language → SQL)
-- ============================================================================

/*
  A Semantic View maps business concepts to your database schema:
  - Dimensions (categorical attributes for filtering/grouping)
  - Metrics (aggregations like SUM, COUNT, AVG)
  - Relationships between tables
  
  This enables natural language queries via Cortex Analyst!
*/

CREATE OR REPLACE SEMANTIC VIEW FMG_LAB4.PRODUCTION.FMG_CUSTOMER_ANALYTICS
  TABLES (
    customers AS FMG_LAB4.PRODUCTION.CUSTOMERS PRIMARY KEY (customer_id),
    subscriptions AS FMG_LAB4.PRODUCTION.SUBSCRIPTIONS PRIMARY KEY (subscription_id),
    feedback AS FMG_LAB4.PRODUCTION.CUSTOMER_FEEDBACK PRIMARY KEY (feedback_id)
  )
  RELATIONSHIPS (
    subscriptions (customer_id) REFERENCES customers (customer_id),
    feedback (customer_id) REFERENCES customers (customer_id)
  )
  DIMENSIONS (
    -- Customer dimensions
    customers.customer_id AS customer_id LABEL 'Customer ID',
    customers.company_name AS company_name LABEL 'Company Name',
    customers.segment AS segment LABEL 'Customer Segment' 
      SYNONYMS ('tier', 'size', 'customer tier'),
    customers.industry AS industry LABEL 'Industry Type',
    customers.created_date AS customer_created_date LABEL 'Customer Since',
    
    -- Subscription dimensions  
    subscriptions.subscription_id AS subscription_id LABEL 'Subscription ID',
    subscriptions.product AS product LABEL 'Product Name'
      SYNONYMS ('subscription product', 'FMG product'),
    subscriptions.status AS subscription_status LABEL 'Subscription Status',
    
    -- Feedback dimensions
    feedback.feedback_id AS feedback_id LABEL 'Feedback ID',
    
    -- Calculated dimension
    CASE 
      WHEN feedback.nps_score >= 9 THEN 'Promoter'
      WHEN feedback.nps_score >= 7 THEN 'Passive'
      ELSE 'Detractor'
    END AS nps_category LABEL 'NPS Category'
  )
  FACTS (
    customers.mrr AS customer_mrr LABEL 'Customer MRR',
    customers.health_score AS health_score LABEL 'Health Score',
    subscriptions.mrr AS subscription_mrr LABEL 'Subscription MRR',
    feedback.nps_score AS nps_score LABEL 'NPS Score'
  )
  METRICS (
    COUNT(DISTINCT customers.customer_id) AS customer_count 
      LABEL 'Number of Customers'
      SYNONYMS ('total customers', 'customer total'),
    
    SUM(customers.mrr) AS total_mrr 
      LABEL 'Total MRR'
      SYNONYMS ('monthly revenue', 'revenue'),
    
    AVG(customers.mrr) AS average_mrr 
      LABEL 'Average MRR'
      SYNONYMS ('avg MRR', 'ARPU'),
    
    AVG(customers.health_score) AS avg_health_score 
      LABEL 'Average Health Score',
    
    COUNT(DISTINCT subscriptions.subscription_id) AS subscription_count 
      LABEL 'Number of Subscriptions',
    
    AVG(feedback.nps_score) AS avg_nps 
      LABEL 'Average NPS Score'
      SYNONYMS ('NPS score', 'satisfaction score'),
    
    COUNT(DISTINCT CASE WHEN feedback.nps_score >= 9 THEN feedback.feedback_id END) 
      AS promoter_count LABEL 'Number of Promoters',
    
    COUNT(DISTINCT CASE WHEN feedback.nps_score <= 6 THEN feedback.feedback_id END) 
      AS detractor_count LABEL 'Number of Detractors'
  )
  COMMENT = 'FMG Customer Analytics for natural language queries';

-- Verify the semantic view
SHOW SEMANTIC VIEWS IN SCHEMA FMG_LAB4.PRODUCTION;

-- Query using semantic view
SELECT 
    segment,
    customer_count,
    total_mrr,
    avg_health_score
FROM FMG_LAB4.PRODUCTION.FMG_CUSTOMER_ANALYTICS
GROUP BY segment
ORDER BY total_mrr DESC;

-- 🎯 Key insight: Business users can now ask questions in plain English!

-- ============================================================================
-- STEP 4: CREATE CORTEX AGENT (via Snowsight UI)
-- ============================================================================

/*
  A Cortex Agent orchestrates multiple tools:
  - Cortex Analyst (semantic view) for structured data queries
  - Cortex Search (search services) for unstructured text search
  
  CREATE THE AGENT VIA SNOWSIGHT UI:
  
  1. Navigate to: AI & ML > Cortex Agents
  2. Click "Create Agent"
  3. Configure:
     - Name: FMG_CUSTOMER_INSIGHTS
     - Description: AI assistant for FMG customer analytics and feedback
     
  4. Add Tools:
     
     a) Cortex Analyst Tool:
        - Name: Customer Analytics
        - Semantic View: FMG_LAB4.PRODUCTION.FMG_CUSTOMER_ANALYTICS
        - Description: Query customer data, revenue, segments, NPS metrics
     
     b) Cortex Search Tool (Feedback):
        - Name: Customer Feedback Search  
        - Search Service: FMG_LAB4.PRODUCTION.FEEDBACK_SEARCH
        - Description: Search customer feedback and NPS comments
     
     c) Cortex Search Tool (KB):
        - Name: Product Documentation
        - Search Service: FMG_LAB4.PRODUCTION.KB_SEARCH
        - Description: Search FMG product help articles
  
  5. Set Response Instructions:
     "You are an AI assistant for FMG, a marketing software company for 
     financial advisors. Be helpful, accurate, and concise."
  
  6. Sample Questions to Try:
     - "What is our total MRR by segment?"
     - "Show me feedback from unhappy customers"
     - "How do I set up email campaigns?"
     - "Which Enterprise customers are at risk?"
  
  7. Click "Create Agent"
*/

-- ============================================================================
-- STEP 5: GRANT PERMISSIONS FOR AGENT ACCESS
-- ============================================================================

-- Grant access to search services
GRANT USAGE ON CORTEX SEARCH SERVICE FMG_LAB4.PRODUCTION.FEEDBACK_SEARCH 
    TO ROLE FMG_ADMIN;
GRANT USAGE ON CORTEX SEARCH SERVICE FMG_LAB4.PRODUCTION.KB_SEARCH 
    TO ROLE FMG_ADMIN;

-- Grant access to semantic view
GRANT SELECT, REFERENCES ON SEMANTIC VIEW FMG_LAB4.PRODUCTION.FMG_CUSTOMER_ANALYTICS 
    TO ROLE FMG_ADMIN;

-- Also for analyst role
GRANT USAGE ON CORTEX SEARCH SERVICE FMG_LAB4.PRODUCTION.FEEDBACK_SEARCH 
    TO ROLE FMG_ANALYST;
GRANT USAGE ON CORTEX SEARCH SERVICE FMG_LAB4.PRODUCTION.KB_SEARCH 
    TO ROLE FMG_ANALYST;
GRANT SELECT, REFERENCES ON SEMANTIC VIEW FMG_LAB4.PRODUCTION.FMG_CUSTOMER_ANALYTICS 
    TO ROLE FMG_ANALYST;

-- ============================================================================
-- STEP 6: COMBINED INSIGHTS DEMO
-- ============================================================================

-- Find at-risk customers with negative feedback and AI-generated action items
WITH at_risk AS (
    SELECT 
        c.customer_id,
        c.company_name,
        c.segment,
        c.mrr,
        c.health_score,
        f.nps_score,
        f.feedback_text,
        SNOWFLAKE.CORTEX.SENTIMENT(f.feedback_text) AS sentiment
    FROM CUSTOMERS c
    JOIN CUSTOMER_FEEDBACK f ON c.customer_id = f.customer_id
    WHERE c.health_score < 75 OR f.nps_score <= 6
)
SELECT 
    company_name,
    segment,
    mrr,
    health_score,
    nps_score,
    ROUND(sentiment, 2) AS sentiment_score,
    LEFT(feedback_text, 80) || '...' AS feedback_preview
FROM at_risk
ORDER BY mrr DESC;

-- ============================================================================
-- CLEANUP (Optional)
-- ============================================================================
-- USE ROLE ACCOUNTADMIN;
-- DROP DATABASE FMG_LAB4;

-- ============================================================================
-- 🎉 LAB 4 COMPLETE!
-- ============================================================================

/*
  What you built:
  
  ✅ Cortex LLM Functions
     - SENTIMENT() - Analyze customer feedback sentiment
     - SUMMARIZE() - Summarize large text
     - COMPLETE() - Generate AI responses
  
  ✅ Cortex Search Services
     - FEEDBACK_SEARCH - Semantic search over customer feedback
     - KB_SEARCH - Semantic search over product documentation
  
  ✅ Semantic View
     - FMG_CUSTOMER_ANALYTICS - Business-friendly data abstraction
     - Enables natural language queries
  
  ✅ Cortex Agent (via UI)
     - Combines all tools into unified AI assistant
     - Routes questions to the right tool automatically
  
  Key Snowflake Benefits:
  • AI/ML built into the platform - no external tools
  • Semantic Search understands meaning, not just keywords
  • Natural language queries over structured data
  • All data stays in Snowflake - secure and governed
  
  Try These Questions with Your Agent:
  1. "What is our total MRR by segment?"
  2. "Show me feedback from customers who mentioned pricing"
  3. "How do I troubleshoot email delivery issues?"
  4. "Which customers have the most products?"
  5. "What are customers saying about MyRepChat?"
  
  Ready for more? Try any other lab - they're all independent!
*/

SELECT '✅ Lab 4 Complete! Your AI tools are ready.' AS STATUS;
