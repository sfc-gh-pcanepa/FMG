/*=============================================================================
  FMG SUITE - LAB 4: SNOWFLAKE AI & CORTEX
  
  What you'll see:
  ✅ Built-in LLM functions (sentiment, summarize, complete)
  ✅ Cortex Search - semantic search over your data
  ✅ Cortex Analyst - natural language queries
  
  Time: ~20 minutes
  Prerequisites: Labs 1-3 completed, Cortex enabled on account
=============================================================================*/

-- ============================================================================
-- STEP 1: ADD CUSTOMER FEEDBACK DATA
-- ============================================================================
USE ROLE FMG_ADMIN;
USE WAREHOUSE FMG_ANALYTICS_WH;
USE SCHEMA FMG_DATA.PRODUCTION;

-- Create NPS/feedback table
CREATE OR REPLACE TABLE CUSTOMER_FEEDBACK (
    feedback_id VARCHAR(20),
    customer_id VARCHAR(20),
    nps_score INT,
    feedback_text VARCHAR(1000),
    submitted_date DATE
);

INSERT INTO CUSTOMER_FEEDBACK VALUES
    ('F001', 'C001', 9, 'Love the email marketing tools! Our open rates have increased 40% since switching to FMG.', '2024-01-15'),
    ('F002', 'C002', 7, 'Good product overall but the social media scheduler could be more intuitive.', '2024-01-18'),
    ('F003', 'C003', 10, 'Amazing support team! They helped us set up everything in one day. Highly recommend!', '2024-02-01'),
    ('F004', 'C004', 4, 'Disappointed with the recent price increase. Considering other options.', '2024-02-10'),
    ('F005', 'C005', 8, 'The website builder is fantastic. Would love to see more templates though.', '2024-02-15'),
    ('F006', 'C001', 9, 'MyRepChat has been a game-changer for client communication. Compliance loves it!', '2024-03-01');

-- ============================================================================
-- STEP 2: SENTIMENT ANALYSIS (One Function Call!)
-- ============================================================================

-- Analyze sentiment of customer feedback instantly
SELECT 
    feedback_id,
    nps_score,
    LEFT(feedback_text, 50) AS feedback_preview,
    SNOWFLAKE.CORTEX.SENTIMENT(feedback_text) AS sentiment_score,
    CASE 
        WHEN SNOWFLAKE.CORTEX.SENTIMENT(feedback_text) > 0.3 THEN '😊 Positive'
        WHEN SNOWFLAKE.CORTEX.SENTIMENT(feedback_text) < -0.3 THEN '😟 Negative'
        ELSE '😐 Neutral'
    END AS sentiment_label
FROM CUSTOMER_FEEDBACK;

-- 🎯 Key insight: No ML model training, no Python, no external APIs. Just SQL!

-- ============================================================================
-- STEP 3: TEXT SUMMARIZATION
-- ============================================================================

-- Summarize all feedback into key themes
SELECT SNOWFLAKE.CORTEX.SUMMARIZE(
    LISTAGG(feedback_text, '. ') WITHIN GROUP (ORDER BY submitted_date)
) AS feedback_summary
FROM CUSTOMER_FEEDBACK;

-- ============================================================================
-- STEP 4: AI-POWERED TEXT GENERATION
-- ============================================================================

-- Generate a response to negative feedback
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-large',
    'Write a brief, empathetic response to this customer feedback. Keep it under 50 words: ' || feedback_text
) AS suggested_response
FROM CUSTOMER_FEEDBACK
WHERE nps_score < 6;

-- Generate a customer outreach email
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-large',
    'Write a short email (under 75 words) to a financial advisor customer thanking them for their feedback and offering a free training session on our new features.'
) AS outreach_email;

-- ============================================================================
-- STEP 5: CORTEX SEARCH (Semantic Search)
-- ============================================================================

-- Create a knowledge base table
CREATE OR REPLACE TABLE KNOWLEDGE_BASE (
    article_id VARCHAR(10),
    title VARCHAR(200),
    content VARCHAR(2000),
    category VARCHAR(50)
);

INSERT INTO KNOWLEDGE_BASE VALUES
    ('KB001', 'How to Create Email Campaigns', 'Navigate to Marketing Tools, click Create Campaign, select a template, customize your content, choose recipients from your contact list, and schedule or send immediately. Pro tip: Use A/B testing for subject lines.', 'Email'),
    ('KB002', 'Troubleshooting Email Delivery', 'If emails are not delivering, check: 1) SPF/DKIM settings in your domain, 2) Verify recipient email addresses, 3) Review bounce reports in Analytics, 4) Ensure content is not triggering spam filters.', 'Email'),
    ('KB003', 'Setting Up MyRepChat', 'Download the MyRepChat app from your app store. Log in with your FMG credentials. Complete the compliance acknowledgment. Start messaging clients - all conversations are automatically archived for compliance.', 'MyRepChat'),
    ('KB004', 'Website Analytics Overview', 'Access your website analytics via Dashboard > Website > Analytics. View page views, unique visitors, session duration, lead captures, and conversion rates. Export reports for client meetings.', 'Website'),
    ('KB005', 'Scheduling Social Media Posts', 'Go to Social Media > Create Post. Write your content or use our AI assistant. Select platforms (LinkedIn, Facebook, Twitter). Click Schedule, pick date and time. View all scheduled posts in the Calendar view.', 'Social');

-- Create Cortex Search service
CREATE OR REPLACE CORTEX SEARCH SERVICE FMG_DATA.PRODUCTION.KB_SEARCH
    ON content
    ATTRIBUTES title, category
    WAREHOUSE = FMG_ANALYTICS_WH
    TARGET_LAG = '1 hour'
AS (
    SELECT article_id, title, category, content
    FROM KNOWLEDGE_BASE
);

-- Search with natural language (not keywords!)
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'FMG_DATA.PRODUCTION.KB_SEARCH',
        '{"query": "my emails are not being received by clients", "columns": ["title", "content"], "limit": 2}'
    )
) AS search_results;

-- 🎯 Key insight: Semantic search understands MEANING, not just keywords!

-- ============================================================================
-- STEP 6: CORTEX ANALYST (Natural Language → SQL)
-- ============================================================================

-- First, create a semantic view that describes your data
CREATE OR REPLACE VIEW FMG_DATA.PRODUCTION.V_CUSTOMER_ANALYTICS AS
SELECT 
    c.customer_id,
    c.company_name,
    c.segment,
    c.health_score,
    COUNT(DISTINCT s.subscription_id) AS products,
    SUM(s.mrr) AS monthly_revenue,
    AVG(f.nps_score) AS avg_nps
FROM CUSTOMERS c
LEFT JOIN SUBSCRIPTIONS s ON c.customer_id = s.customer_id AND s.status = 'Active'
LEFT JOIN CUSTOMER_FEEDBACK f ON c.customer_id = f.customer_id
GROUP BY c.customer_id, c.company_name, c.segment, c.health_score;

-- View the analytics-ready data
SELECT * FROM V_CUSTOMER_ANALYTICS ORDER BY monthly_revenue DESC;

-- ============================================================================
-- STEP 7: ASK QUESTIONS IN PLAIN ENGLISH
-- ============================================================================

-- Use COMPLETE to translate natural language to SQL (simplified Analyst demo)
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-large',
    'Given a table V_CUSTOMER_ANALYTICS with columns: customer_id, company_name, segment, health_score, products, monthly_revenue, avg_nps.
    
    Write a SQL query to answer: "Which Enterprise customers have the highest revenue?"
    
    Return only the SQL query, nothing else.'
) AS generated_sql;

-- ============================================================================
-- 🎉 LAB 4 COMPLETE!
-- ============================================================================
/*
  What you just saw:
  
  ✅ Sentiment analysis with one function call
  ✅ Text summarization built into SQL
  ✅ AI text generation (customer responses, emails)
  ✅ Semantic search that understands meaning
  ✅ Natural language to SQL translation
  
  Key Snowflake Benefits:
  • AI/ML built into the platform - no external tools needed
  • Runs on your data without moving it
  • Governed by the same security policies
  • Pay-per-use, no GPU management
  
  For production Cortex Analyst:
  • Create Semantic Views to describe your data model
  • Build Cortex Agents to combine Analyst + Search
  • See docs.snowflake.com for full setup guide
*/

