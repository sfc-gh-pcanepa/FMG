/*=============================================================================
  FMG SUITE - LAB 4: SNOWFLAKE AGENTS AND INTELLIGENCE
  Script 1: Cortex 101
  
  Description: Introduction to Snowflake Cortex LLM and ML functions
  Prerequisites: Cortex enabled on account, FMG data loaded
  Duration: ~20 minutes
  
  Note: Cortex availability varies by region. Some functions may require
  specific model access or account enablement.
=============================================================================*/

-- ============================================================================
-- SECTION 1: SET CONTEXT
-- ============================================================================

USE ROLE FMG_ADMIN;
USE WAREHOUSE FMG_ML_L;  -- Use larger warehouse for ML workloads
USE DATABASE FMG_ANALYTICS;

-- Create schema for AI/ML work
CREATE SCHEMA IF NOT EXISTS FMG_ANALYTICS.AI
    COMMENT = 'AI and ML experiments and outputs';

USE SCHEMA FMG_ANALYTICS.AI;

-- ============================================================================
-- SECTION 2: UNDERSTANDING CORTEX LLM FUNCTIONS
-- ============================================================================

/*
    Cortex LLM Functions:
    
    COMPLETE()       - General purpose text generation/completion
    SUMMARIZE()      - Summarize long text
    TRANSLATE()      - Translate between languages
    EXTRACT_ANSWER() - Answer questions based on context
    SENTIMENT()      - Analyze sentiment (-1 to 1)
    
    Available Models (varies by region):
    - snowflake-arctic          (Snowflake's own model)
    - mistral-large             (Strong general purpose)
    - llama3-70b                (Meta's open model)
    - claude-3-sonnet           (Anthropic)
    - gpt-4o                    (OpenAI)
    
    Usage:
    SNOWFLAKE.CORTEX.FUNCTION_NAME(model, prompt)
    or
    SNOWFLAKE.CORTEX.FUNCTION_NAME(text)  -- Uses default model
*/

-- ============================================================================
-- SECTION 3: SENTIMENT ANALYSIS
-- ============================================================================

-- Analyze sentiment of NPS feedback
SELECT 
    response_id,
    customer_id,
    nps_score,
    feedback_text,
    SNOWFLAKE.CORTEX.SENTIMENT(feedback_text) AS sentiment_score,
    CASE 
        WHEN SNOWFLAKE.CORTEX.SENTIMENT(feedback_text) > 0.3 THEN 'Positive'
        WHEN SNOWFLAKE.CORTEX.SENTIMENT(feedback_text) < -0.3 THEN 'Negative'
        ELSE 'Neutral'
    END AS sentiment_category
FROM FMG_PRODUCTION.RAW.NPS_RESPONSES
WHERE feedback_text IS NOT NULL
LIMIT 20;

-- Correlate NPS score with sentiment
SELECT 
    nps_category,
    COUNT(*) AS response_count,
    AVG(SNOWFLAKE.CORTEX.SENTIMENT(feedback_text)) AS avg_sentiment,
    MIN(SNOWFLAKE.CORTEX.SENTIMENT(feedback_text)) AS min_sentiment,
    MAX(SNOWFLAKE.CORTEX.SENTIMENT(feedback_text)) AS max_sentiment
FROM FMG_PRODUCTION.RAW.NPS_RESPONSES
WHERE feedback_text IS NOT NULL
GROUP BY nps_category
ORDER BY avg_sentiment DESC;

-- Analyze support ticket sentiment
SELECT 
    ticket_id,
    category,
    priority,
    ticket_summary,
    SNOWFLAKE.CORTEX.SENTIMENT(ticket_summary) AS sentiment_score
FROM FMG_PRODUCTION.RAW.SUPPORT_TICKETS
WHERE ticket_summary IS NOT NULL
ORDER BY sentiment_score ASC  -- Most negative first
LIMIT 20;

-- ============================================================================
-- SECTION 4: TEXT SUMMARIZATION
-- ============================================================================

-- Summarize customer feedback for executives
WITH feedback_batch AS (
    SELECT 
        LISTAGG(feedback_text, '. ') AS all_feedback
    FROM FMG_PRODUCTION.RAW.NPS_RESPONSES
    WHERE feedback_text IS NOT NULL
    AND survey_date >= DATEADD('day', -30, CURRENT_DATE())
    LIMIT 50
)
SELECT 
    SNOWFLAKE.CORTEX.SUMMARIZE(all_feedback) AS feedback_summary
FROM feedback_batch;

-- Create weekly feedback digest
CREATE OR REPLACE VIEW V_WEEKLY_FEEDBACK_DIGEST AS
WITH weekly_feedback AS (
    SELECT 
        DATE_TRUNC('week', survey_date) AS week_start,
        nps_category,
        LISTAGG(feedback_text, '. ') WITHIN GROUP (ORDER BY survey_date) AS combined_feedback
    FROM FMG_PRODUCTION.RAW.NPS_RESPONSES
    WHERE feedback_text IS NOT NULL
    GROUP BY 1, 2
)
SELECT 
    week_start,
    nps_category,
    SNOWFLAKE.CORTEX.SUMMARIZE(combined_feedback) AS weekly_summary
FROM weekly_feedback
WHERE LEN(combined_feedback) > 100;

-- ============================================================================
-- SECTION 5: TEXT GENERATION WITH COMPLETE()
-- ============================================================================

-- Generate customer outreach suggestions based on health score
SELECT 
    customer_id,
    company_name,
    overall_health_score,
    churn_risk,
    SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large',
        'You are a customer success manager at a SaaS company. Based on the following customer information, write a brief personalized outreach message:

Customer: ' || company_name || '
Health Score: ' || overall_health_score || '/100
Churn Risk: ' || churn_risk || '
Industry: ' || industry || '

Write a 2-3 sentence outreach message that is empathetic and offers help.'
    ) AS suggested_outreach
FROM FMG_ANALYTICS.DYNAMIC.DT_CUSTOMER_360
WHERE churn_risk IN ('High', 'Critical')
LIMIT 5;

-- Generate content ideas for marketing
SELECT 
    SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large',
        'You are a content marketer for FMG Suite, a marketing platform for financial advisors.
        
Generate 5 blog post ideas that would help financial advisors improve their marketing. 
Focus on practical tips they can implement immediately.
Format as a numbered list with title and brief description.'
    ) AS content_ideas;

-- ============================================================================
-- SECTION 6: QUESTION ANSWERING
-- ============================================================================

-- Answer questions about customer data
WITH customer_context AS (
    SELECT 
        'Customer: ' || company_name || 
        '. Segment: ' || segment || 
        '. Industry: ' || industry ||
        '. MRR: $' || total_mrr ||
        '. Health Score: ' || overall_health_score ||
        '. Products: ' || COALESCE(products_list, 'None') ||
        '. Last Login: ' || days_since_last_login || ' days ago' AS context
    FROM FMG_ANALYTICS.DYNAMIC.DT_CUSTOMER_360
    WHERE customer_id = 'CUST-001000'
)
SELECT 
    SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
        context,
        'What products does this customer use?'
    ) AS answer
FROM customer_context;

-- ============================================================================
-- SECTION 7: LANGUAGE DETECTION AND TRANSLATION
-- ============================================================================

-- Detect language of feedback (if multilingual)
SELECT 
    response_id,
    feedback_text,
    SNOWFLAKE.CORTEX.DETECT_LANGUAGE(feedback_text) AS detected_language
FROM FMG_PRODUCTION.RAW.NPS_RESPONSES
WHERE feedback_text IS NOT NULL
LIMIT 20;

-- Translate feedback to English (if non-English detected)
-- SELECT 
--     feedback_text,
--     SNOWFLAKE.CORTEX.TRANSLATE(feedback_text, 'es', 'en') AS translated_text
-- FROM FMG_PRODUCTION.RAW.NPS_RESPONSES
-- WHERE SNOWFLAKE.CORTEX.DETECT_LANGUAGE(feedback_text) = 'es';

-- ============================================================================
-- SECTION 8: TEXT EMBEDDINGS
-- ============================================================================

/*
    Embeddings convert text to numerical vectors that capture meaning.
    Similar texts have similar embeddings (close in vector space).
    
    Use cases:
    - Semantic search
    - Clustering similar documents
    - Finding similar customers based on feedback
*/

-- Create embeddings for support tickets
CREATE OR REPLACE TABLE SUPPORT_TICKET_EMBEDDINGS AS
SELECT 
    ticket_id,
    ticket_summary,
    SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', ticket_summary) AS embedding
FROM FMG_PRODUCTION.RAW.SUPPORT_TICKETS
WHERE ticket_summary IS NOT NULL
LIMIT 100;  -- Limit for demo

-- Find similar tickets using vector similarity
-- (Useful for finding related issues or duplicate tickets)
WITH query_embedding AS (
    SELECT SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', 'email delivery problems') AS query_vec
)
SELECT 
    t.ticket_id,
    t.ticket_summary,
    VECTOR_COSINE_SIMILARITY(t.embedding, q.query_vec) AS similarity_score
FROM SUPPORT_TICKET_EMBEDDINGS t
CROSS JOIN query_embedding q
ORDER BY similarity_score DESC
LIMIT 5;

-- ============================================================================
-- SECTION 9: TEXT CLASSIFICATION
-- ============================================================================

-- Classify support tickets into categories
SELECT 
    ticket_id,
    ticket_summary,
    SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
        ticket_summary,
        ['Technical Issue', 'Billing Question', 'Feature Request', 'Training Needed', 'Account Management']
    ) AS predicted_category,
    category AS actual_category
FROM FMG_PRODUCTION.RAW.SUPPORT_TICKETS
WHERE ticket_summary IS NOT NULL
LIMIT 20;

-- ============================================================================
-- SECTION 10: CREATE AI-ENHANCED VIEWS
-- ============================================================================

-- NPS Analysis View with Sentiment
CREATE OR REPLACE VIEW V_NPS_SENTIMENT_ANALYSIS AS
SELECT 
    n.response_id,
    n.customer_id,
    c.company_name,
    c.segment,
    n.survey_date,
    n.nps_score,
    n.nps_category,
    n.feedback_text,
    SNOWFLAKE.CORTEX.SENTIMENT(n.feedback_text) AS sentiment_score,
    CASE 
        WHEN SNOWFLAKE.CORTEX.SENTIMENT(n.feedback_text) > 0.3 THEN 'Positive'
        WHEN SNOWFLAKE.CORTEX.SENTIMENT(n.feedback_text) < -0.3 THEN 'Negative'
        ELSE 'Neutral'
    END AS sentiment_category,
    -- Flag mismatches (e.g., high NPS but negative sentiment)
    CASE 
        WHEN n.nps_category = 'Promoter' AND SNOWFLAKE.CORTEX.SENTIMENT(n.feedback_text) < 0 
            THEN 'Review needed - mismatch'
        WHEN n.nps_category = 'Detractor' AND SNOWFLAKE.CORTEX.SENTIMENT(n.feedback_text) > 0 
            THEN 'Review needed - mismatch'
        ELSE 'Aligned'
    END AS alignment_flag
FROM FMG_PRODUCTION.RAW.NPS_RESPONSES n
LEFT JOIN FMG_PRODUCTION.RAW.CUSTOMERS c ON n.customer_id = c.customer_id
WHERE n.feedback_text IS NOT NULL;

-- Support Ticket Intelligence View
CREATE OR REPLACE VIEW V_SUPPORT_TICKET_INTELLIGENCE AS
SELECT 
    t.ticket_id,
    t.customer_id,
    c.company_name,
    c.segment,
    t.category,
    t.priority,
    t.status,
    t.ticket_summary,
    SNOWFLAKE.CORTEX.SENTIMENT(t.ticket_summary) AS sentiment_score,
    CASE 
        WHEN SNOWFLAKE.CORTEX.SENTIMENT(t.ticket_summary) < -0.5 THEN 'Escalation Risk'
        WHEN SNOWFLAKE.CORTEX.SENTIMENT(t.ticket_summary) < -0.2 THEN 'Monitor Closely'
        ELSE 'Normal'
    END AS escalation_risk,
    t.created_date,
    t.resolution_time_hours,
    t.csat_score
FROM FMG_PRODUCTION.RAW.SUPPORT_TICKETS t
LEFT JOIN FMG_PRODUCTION.RAW.CUSTOMERS c ON t.customer_id = c.customer_id
WHERE t.ticket_summary IS NOT NULL;

-- ============================================================================
-- SECTION 11: BATCH PROCESSING FOR PRODUCTION
-- ============================================================================

-- Create a table to store AI-enriched data (avoid repeated LLM calls)
CREATE OR REPLACE TABLE AI_ENRICHED_NPS AS
SELECT 
    response_id,
    customer_id,
    survey_date,
    nps_score,
    nps_category,
    feedback_text,
    SNOWFLAKE.CORTEX.SENTIMENT(feedback_text) AS sentiment_score,
    -- Only call summarize for longer feedback
    CASE 
        WHEN LEN(feedback_text) > 200 
        THEN SNOWFLAKE.CORTEX.SUMMARIZE(feedback_text)
        ELSE feedback_text
    END AS feedback_summary,
    CURRENT_TIMESTAMP() AS processed_at
FROM FMG_PRODUCTION.RAW.NPS_RESPONSES
WHERE feedback_text IS NOT NULL;

-- Create a task to process new NPS responses daily
-- CREATE OR REPLACE TASK TASK_ENRICH_NPS
--     WAREHOUSE = FMG_ML_L
--     SCHEDULE = 'USING CRON 0 6 * * * America/Los_Angeles'
-- AS
-- INSERT INTO AI_ENRICHED_NPS
-- SELECT ...
-- WHERE response_id NOT IN (SELECT response_id FROM AI_ENRICHED_NPS);

-- ============================================================================
-- SECTION 12: CORTEX BEST PRACTICES
-- ============================================================================

/*
    CORTEX BEST PRACTICES:
    
    1. MODEL SELECTION
       - Use smaller models (mistral-7b) for simple tasks
       - Use larger models (mistral-large, claude) for complex reasoning
       - Test model performance on your specific use cases
    
    2. COST MANAGEMENT
       - Cache results when possible (don't re-process same text)
       - Use batch processing during off-peak hours
       - Monitor token usage via ACCOUNT_USAGE
    
    3. PROMPT ENGINEERING
       - Be specific and clear in prompts
       - Provide context and examples
       - Use system prompts for consistent behavior
    
    4. ERROR HANDLING
       - Handle NULL values before calling functions
       - Check for empty or very short text
       - Use TRY_* variants where available
    
    5. SECURITY
       - Don't send PII to external models without approval
       - Consider data residency requirements
       - Review model terms of service
*/

-- ============================================================================
-- SCRIPT COMPLETE!
-- ============================================================================

SELECT '✅ Cortex 101 Complete!' AS STATUS,
       'Try the sentiment and summarization functions on your data' AS NEXT_STEP,
       CURRENT_TIMESTAMP() AS COMPLETED_AT;

