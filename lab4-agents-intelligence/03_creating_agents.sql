/*=============================================================================
  FMG SUITE - LAB 4: SNOWFLAKE AGENTS AND INTELLIGENCE
  Script 3: Creating Cortex Agents
  
  Description: Create a Cortex Agent that combines Cortex Analyst (semantic views)
               with Cortex Search for a unified FMG intelligence experience
  Prerequisites: 02_cortex_search_analyst.sql completed
  Duration: ~20 minutes
  
  Reference: https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents
=============================================================================*/

-- ============================================================================
-- SECTION 1: SET CONTEXT
-- ============================================================================

USE ROLE FMG_ADMIN;
USE WAREHOUSE FMG_ML_L;
USE DATABASE FMG_ANALYTICS;
USE SCHEMA AI;

-- ============================================================================
-- SECTION 2: UNDERSTANDING CORTEX AGENTS
-- ============================================================================

/*
    Cortex Agents are AI orchestrators that combine multiple tools:
    
    ┌─────────────────────────────────────────────────────────────────────────┐
    │                       CORTEX AGENT ARCHITECTURE                          │
    ├─────────────────────────────────────────────────────────────────────────┤
    │                                                                          │
    │   User Question                                                          │
    │   "What is our MRR by segment and how do I fix email delivery issues?" │
    │                                                                          │
    │                      ┌─────────────────────────────┐                    │
    │                      │      CORTEX AGENT           │                    │
    │                      │                             │                    │
    │                      │  ┌─────────────────────┐   │                    │
    │                      │  │   ORCHESTRATOR      │   │                    │
    │                      │  │   (LLM + Routing)   │   │                    │
    │                      │  └──────────┬──────────┘   │                    │
    │                      │             │              │                    │
    │                      │      ┌──────┴──────┐       │                    │
    │                      │      ▼             ▼       │                    │
    │                      │  ┌───────┐   ┌───────┐    │                    │
    │                      │  │ANALYST│   │SEARCH │    │                    │
    │                      │  │ TOOL  │   │ TOOL  │    │                    │
    │                      │  └───┬───┘   └───┬───┘    │                    │
    │                      │      │           │        │                    │
    │                      │      ▼           ▼        │                    │
    │                      │  ┌───────┐   ┌───────┐    │                    │
    │                      │  │SEMANTIC│  │SUPPORT│    │                    │
    │                      │  │ VIEWS  │  │  KB   │    │                    │
    │                      │  └───────┘   └───────┘    │                    │
    │                      │                           │                    │
    │                      └─────────────────────────────┘                    │
    │                                                                          │
    │   Response: "Here is the MRR by segment: [table]                        │
    │              For email delivery issues, see KB-002: [article]"          │
    │                                                                          │
    └─────────────────────────────────────────────────────────────────────────┘
    
    Tools Available:
    - cortex_analyst_tool: Queries data using semantic views
    - cortex_search_tool: Searches knowledge base for documentation
    - sql_tool: Executes custom SQL queries
    - python_tool: Runs Python functions
*/

-- ============================================================================
-- SECTION 3: CREATE THE FMG INTELLIGENCE AGENT
-- ============================================================================

/*
    The FMG Intelligence Agent combines:
    1. Cortex Analyst - For data questions using semantic views
    2. Cortex Search - For support/documentation questions
    
    This enables a single interface for all FMG intelligence needs.
*/

-- Create the Cortex Agent
CREATE OR REPLACE CORTEX AGENT FMG_INTELLIGENCE_AGENT
    COMMENT = 'FMG unified intelligence agent combining analytics and support knowledge'
    
    -- Agent display configuration
    DISPLAY_NAME = 'FMG Intelligence Assistant'
    DESCRIPTION = 'Ask questions about FMG data (customers, revenue, usage) or search our knowledge base for help articles'
    
    -- LLM configuration
    MODEL = 'claude-3-5-sonnet'
    
    -- System prompt for the agent
    SYSTEM_PROMPT = 'You are the FMG Intelligence Assistant, helping FMG team members with:

1. DATA ANALYTICS: Answer questions about customers, revenue (MRR/ARR), platform usage, and support metrics. Use the analyst tool to query our semantic views.

2. SUPPORT KNOWLEDGE: Help users find documentation and troubleshooting guides. Use the search tool to find relevant help articles.

Guidelines:
- Be concise and business-focused
- When showing data, format it clearly as tables
- For support questions, provide article links and summaries
- If you need clarification, ask specific questions
- Always cite your sources (semantic view or KB article)

FMG Products: Marketing Suite, Website Pro, MyRepChat, Do It For Me
Customer Segments: SMB, Mid-Market, Enterprise
Industries: RIA, Broker-Dealer, Insurance, Bank/Credit Union, Wirehouse'
    
    -- Tools configuration
    TOOLS = (
        -- Tool 1: Cortex Analyst for data queries
        CORTEX_ANALYST (
            SEMANTIC_VIEWS => (
                FMG_ANALYTICS.AI.SV_CUSTOMERS,
                FMG_ANALYTICS.AI.SV_REVENUE,
                FMG_ANALYTICS.AI.SV_USAGE,
                FMG_ANALYTICS.AI.SV_SUPPORT
            )
        ),
        
        -- Tool 2: Cortex Search for knowledge base
        CORTEX_SEARCH (
            SEARCH_SERVICE => 'FMG_ANALYTICS.AI.FMG_SUPPORT_SEARCH_SERVICE'
        )
    );

-- Verify the agent was created
SHOW CORTEX AGENTS;

-- ============================================================================
-- SECTION 4: TEST THE AGENT
-- ============================================================================

-- Test analytics query via agent
SELECT SNOWFLAKE.CORTEX.AGENT(
    'FMG_ANALYTICS.AI.FMG_INTELLIGENCE_AGENT',
    'What is our total MRR by customer segment?'
) AS agent_response;

-- Test support search via agent
SELECT SNOWFLAKE.CORTEX.AGENT(
    'FMG_ANALYTICS.AI.FMG_INTELLIGENCE_AGENT',
    'How do I schedule social media posts in advance?'
) AS agent_response;

-- Test combined query (analytics + search)
SELECT SNOWFLAKE.CORTEX.AGENT(
    'FMG_ANALYTICS.AI.FMG_INTELLIGENCE_AGENT',
    'Which customers have critical churn risk, and what resources can help our CS team retain them?'
) AS agent_response;

-- ============================================================================
-- SECTION 5: CREATE SPECIALIZED AGENTS
-- ============================================================================

/*
    Create role-specific agents with tailored capabilities:
    - CS Agent: Customer success focused
    - Sales Agent: Pipeline and revenue focused
    - Executive Agent: High-level KPIs
*/

-- Customer Success Agent
CREATE OR REPLACE CORTEX AGENT FMG_CS_AGENT
    DISPLAY_NAME = 'FMG Customer Success Assistant'
    DESCRIPTION = 'Helps CS team understand customer health, identify risks, and find retention resources'
    MODEL = 'claude-3-5-sonnet'
    
    SYSTEM_PROMPT = 'You are the FMG Customer Success Assistant. Your role is to help CSMs:

1. Understand customer health and engagement
2. Identify at-risk accounts before they churn
3. Find resources to help retain customers
4. Track support ticket patterns

Focus areas:
- Health scores and trends
- Usage patterns and engagement
- Support ticket history
- NPS and satisfaction data

Always provide actionable recommendations for the CSM to follow up on.
When identifying risks, prioritize by MRR impact.'
    
    TOOLS = (
        CORTEX_ANALYST (
            SEMANTIC_VIEWS => (
                FMG_ANALYTICS.AI.SV_CUSTOMERS,
                FMG_ANALYTICS.AI.SV_USAGE,
                FMG_ANALYTICS.AI.SV_SUPPORT
            )
        ),
        CORTEX_SEARCH (
            SEARCH_SERVICE => 'FMG_ANALYTICS.AI.FMG_SUPPORT_SEARCH_SERVICE'
        )
    );

-- Sales Intelligence Agent
CREATE OR REPLACE CORTEX AGENT FMG_SALES_AGENT
    DISPLAY_NAME = 'FMG Sales Intelligence Assistant'
    DESCRIPTION = 'Helps sales team understand revenue, identify expansion opportunities, and track performance'
    MODEL = 'claude-3-5-sonnet'
    
    SYSTEM_PROMPT = 'You are the FMG Sales Intelligence Assistant. Your role is to help the sales team:

1. Track MRR and ARR by segment, product, and territory
2. Identify expansion and upsell opportunities
3. Understand product adoption patterns
4. Analyze win/loss trends

Key metrics to focus on:
- MRR/ARR growth
- Average deal size
- Product penetration
- Customer segment distribution

When presenting data, always include period-over-period comparisons when relevant.
Highlight opportunities for revenue expansion.'
    
    TOOLS = (
        CORTEX_ANALYST (
            SEMANTIC_VIEWS => (
                FMG_ANALYTICS.AI.SV_CUSTOMERS,
                FMG_ANALYTICS.AI.SV_REVENUE
            )
        )
    );

-- Executive Dashboard Agent
CREATE OR REPLACE CORTEX AGENT FMG_EXECUTIVE_AGENT
    DISPLAY_NAME = 'FMG Executive Dashboard'
    DESCRIPTION = 'Provides executive-level insights on company KPIs and performance'
    MODEL = 'claude-3-5-sonnet'
    
    SYSTEM_PROMPT = 'You are the FMG Executive Dashboard Assistant. Your role is to provide leadership with:

1. High-level KPI summaries
2. Trend analysis and forecasts
3. Risk and opportunity highlights
4. Cross-functional insights

Present information in a concise, executive-friendly format:
- Lead with the headline/insight
- Support with key numbers
- Highlight significant changes
- Suggest areas needing attention

Avoid overwhelming detail - focus on what matters most for strategic decisions.'
    
    TOOLS = (
        CORTEX_ANALYST (
            SEMANTIC_VIEWS => (
                FMG_ANALYTICS.AI.SV_CUSTOMERS,
                FMG_ANALYTICS.AI.SV_REVENUE,
                FMG_ANALYTICS.AI.SV_USAGE,
                FMG_ANALYTICS.AI.SV_SUPPORT
            )
        )
    );

-- ============================================================================
-- SECTION 6: AGENT WITH CUSTOM SQL TOOL
-- ============================================================================

/*
    For more advanced use cases, you can add a SQL tool that allows
    the agent to run custom queries beyond the semantic views.
*/

-- Create a curated function for the agent to use
CREATE OR REPLACE FUNCTION AGENT_GET_CUSTOMER_360(customer_name_pattern VARCHAR)
RETURNS TABLE (
    customer_id VARCHAR,
    customer_name VARCHAR,
    segment VARCHAR,
    industry VARCHAR,
    mrr DECIMAL(10,2),
    health_score INTEGER,
    churn_risk VARCHAR,
    open_tickets INTEGER,
    last_login_days_ago INTEGER
)
LANGUAGE SQL
COMMENT = 'Get comprehensive 360-degree view of a customer'
AS
$$
    SELECT 
        c.customer_id,
        c.company_name,
        c.segment,
        c.industry,
        COALESCE(s.mrr, 0) AS mrr,
        h.overall_health_score,
        h.churn_risk,
        COALESCE(t.open_tickets, 0) AS open_tickets,
        COALESCE(u.days_since_login, 999) AS last_login_days_ago
    FROM FMG_PRODUCTION.RAW.CUSTOMERS c
    LEFT JOIN (
        SELECT customer_id, SUM(mrr_amount) AS mrr
        FROM FMG_PRODUCTION.RAW.SUBSCRIPTIONS WHERE status = 'Active'
        GROUP BY customer_id
    ) s ON c.customer_id = s.customer_id
    LEFT JOIN (
        SELECT * FROM FMG_PRODUCTION.RAW.CUSTOMER_HEALTH_SCORES
        QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY snapshot_date DESC) = 1
    ) h ON c.customer_id = h.customer_id
    LEFT JOIN (
        SELECT customer_id, COUNT(*) AS open_tickets
        FROM FMG_PRODUCTION.RAW.SUPPORT_TICKETS
        WHERE status IN ('Open', 'In Progress')
        GROUP BY customer_id
    ) t ON c.customer_id = t.customer_id
    LEFT JOIN (
        SELECT customer_id, 
               DATEDIFF('day', MAX(usage_date), CURRENT_DATE()) AS days_since_login
        FROM FMG_PRODUCTION.RAW.PLATFORM_USAGE_DAILY
        GROUP BY customer_id
    ) u ON c.customer_id = u.customer_id
    WHERE LOWER(c.company_name) LIKE '%' || LOWER(customer_name_pattern) || '%'
    AND c.account_status = 'Active'
$$;

-- Create a function for at-risk analysis
CREATE OR REPLACE FUNCTION AGENT_GET_AT_RISK_CUSTOMERS(min_mrr DECIMAL DEFAULT 0)
RETURNS TABLE (
    customer_name VARCHAR,
    segment VARCHAR,
    industry VARCHAR,
    mrr DECIMAL(10,2),
    health_score INTEGER,
    churn_risk VARCHAR,
    health_trend VARCHAR,
    days_since_login INTEGER,
    risk_summary VARCHAR
)
LANGUAGE SQL
COMMENT = 'Get customers with high or critical churn risk, optionally filtered by minimum MRR'
AS
$$
    SELECT 
        c.company_name,
        c.segment,
        c.industry,
        COALESCE(s.mrr, 0) AS mrr,
        h.overall_health_score,
        h.churn_risk,
        h.health_trend,
        COALESCE(u.days_since_login, 999) AS days_since_login,
        CASE 
            WHEN h.churn_risk = 'Critical' AND COALESCE(s.mrr, 0) > 500 THEN 'URGENT: High-value account at critical risk'
            WHEN h.churn_risk = 'Critical' THEN 'Critical risk - immediate outreach needed'
            WHEN h.health_trend = 'Declining' THEN 'Declining health - proactive intervention recommended'
            WHEN COALESCE(u.days_since_login, 999) > 30 THEN 'Low engagement - reactivation campaign suggested'
            ELSE 'At risk - monitor closely'
        END AS risk_summary
    FROM FMG_PRODUCTION.RAW.CUSTOMERS c
    LEFT JOIN (
        SELECT customer_id, SUM(mrr_amount) AS mrr
        FROM FMG_PRODUCTION.RAW.SUBSCRIPTIONS WHERE status = 'Active'
        GROUP BY customer_id
    ) s ON c.customer_id = s.customer_id
    LEFT JOIN (
        SELECT * FROM FMG_PRODUCTION.RAW.CUSTOMER_HEALTH_SCORES
        QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY snapshot_date DESC) = 1
    ) h ON c.customer_id = h.customer_id
    LEFT JOIN (
        SELECT customer_id, 
               DATEDIFF('day', MAX(usage_date), CURRENT_DATE()) AS days_since_login
        FROM FMG_PRODUCTION.RAW.PLATFORM_USAGE_DAILY
        GROUP BY customer_id
    ) u ON c.customer_id = u.customer_id
    WHERE c.account_status = 'Active'
    AND h.churn_risk IN ('High', 'Critical')
    AND COALESCE(s.mrr, 0) >= min_mrr
    ORDER BY s.mrr DESC NULLS LAST
$$;

-- Test the custom functions
SELECT * FROM TABLE(AGENT_GET_CUSTOMER_360('Pinnacle')) LIMIT 5;
SELECT * FROM TABLE(AGENT_GET_AT_RISK_CUSTOMERS(100)) LIMIT 10;

-- ============================================================================
-- SECTION 7: CREATE AGENT WITH SQL TOOL
-- ============================================================================

-- Agent with both semantic views and SQL tool access
CREATE OR REPLACE CORTEX AGENT FMG_POWER_USER_AGENT
    DISPLAY_NAME = 'FMG Power User Assistant'
    DESCRIPTION = 'Advanced assistant with full analytics capabilities for power users'
    MODEL = 'claude-3-5-sonnet'
    
    SYSTEM_PROMPT = 'You are the FMG Power User Assistant with advanced capabilities.

You have access to:
1. Semantic views for standard analytics (customers, revenue, usage, support)
2. Custom SQL functions for specialized queries
3. Knowledge base search for documentation

Available functions:
- AGENT_GET_CUSTOMER_360(customer_name): Get complete customer profile
- AGENT_GET_AT_RISK_CUSTOMERS(min_mrr): Find at-risk customers above MRR threshold

Use the most efficient tool for each query. Provide detailed analysis when requested.'
    
    TOOLS = (
        CORTEX_ANALYST (
            SEMANTIC_VIEWS => (
                FMG_ANALYTICS.AI.SV_CUSTOMERS,
                FMG_ANALYTICS.AI.SV_REVENUE,
                FMG_ANALYTICS.AI.SV_USAGE,
                FMG_ANALYTICS.AI.SV_SUPPORT
            )
        ),
        CORTEX_SEARCH (
            SEARCH_SERVICE => 'FMG_ANALYTICS.AI.FMG_SUPPORT_SEARCH_SERVICE'
        ),
        SQL (
            FUNCTIONS => (
                FMG_ANALYTICS.AI.AGENT_GET_CUSTOMER_360,
                FMG_ANALYTICS.AI.AGENT_GET_AT_RISK_CUSTOMERS
            )
        )
    );

-- ============================================================================
-- SECTION 8: CONVERSATIONAL AGENT INTERFACE
-- ============================================================================

/*
    Agents support multi-turn conversations. Use the conversation history
    to maintain context across multiple exchanges.
*/

-- Create a conversation history table
CREATE OR REPLACE TABLE AGENT_CONVERSATIONS (
    conversation_id VARCHAR DEFAULT UUID_STRING(),
    user_id VARCHAR,
    agent_name VARCHAR,
    message_role VARCHAR,  -- 'user' or 'assistant'
    message_content VARCHAR,
    message_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    tokens_used INTEGER
);

-- Create a procedure for conversational interaction
CREATE OR REPLACE PROCEDURE CHAT_WITH_AGENT(
    agent_name VARCHAR,
    user_message VARCHAR,
    conversation_id VARCHAR DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    conv_id VARCHAR;
    agent_response VARCHAR;
    response_obj VARIANT;
BEGIN
    -- Generate conversation ID if not provided
    conv_id := COALESCE(conversation_id, UUID_STRING());
    
    -- Log user message
    INSERT INTO AGENT_CONVERSATIONS (conversation_id, user_id, agent_name, message_role, message_content)
    VALUES (:conv_id, CURRENT_USER(), :agent_name, 'user', :user_message);
    
    -- Call the agent
    SELECT SNOWFLAKE.CORTEX.AGENT(
        'FMG_ANALYTICS.AI.' || :agent_name,
        :user_message
    ) INTO agent_response;
    
    -- Log agent response
    INSERT INTO AGENT_CONVERSATIONS (conversation_id, user_id, agent_name, message_role, message_content)
    VALUES (:conv_id, CURRENT_USER(), :agent_name, 'assistant', :agent_response);
    
    -- Return response with conversation ID
    response_obj := OBJECT_CONSTRUCT(
        'conversation_id', conv_id,
        'response', agent_response
    );
    
    RETURN response_obj;
END;
$$;

-- Test the conversational interface
CALL CHAT_WITH_AGENT('FMG_INTELLIGENCE_AGENT', 'What is our total MRR?');

-- Continue the conversation (use the conversation_id from previous response)
-- CALL CHAT_WITH_AGENT('FMG_INTELLIGENCE_AGENT', 'Break that down by segment', '<conversation_id>');

-- ============================================================================
-- SECTION 9: AGENT MONITORING AND ANALYTICS
-- ============================================================================

-- View recent agent conversations
CREATE OR REPLACE VIEW V_AGENT_USAGE_ANALYTICS AS
SELECT 
    agent_name,
    DATE_TRUNC('day', message_timestamp) AS usage_date,
    COUNT(DISTINCT conversation_id) AS conversations,
    COUNT(CASE WHEN message_role = 'user' THEN 1 END) AS user_messages,
    COUNT(CASE WHEN message_role = 'assistant' THEN 1 END) AS assistant_responses,
    COUNT(DISTINCT user_id) AS unique_users
FROM AGENT_CONVERSATIONS
GROUP BY 1, 2
ORDER BY usage_date DESC, agent_name;

-- View conversation details
CREATE OR REPLACE VIEW V_AGENT_CONVERSATIONS_DETAIL AS
SELECT 
    conversation_id,
    user_id,
    agent_name,
    message_role,
    LEFT(message_content, 500) AS message_preview,
    message_timestamp,
    LAG(message_timestamp) OVER (PARTITION BY conversation_id ORDER BY message_timestamp) AS prev_message_time,
    DATEDIFF('second', 
        LAG(message_timestamp) OVER (PARTITION BY conversation_id ORDER BY message_timestamp),
        message_timestamp
    ) AS response_time_seconds
FROM AGENT_CONVERSATIONS
ORDER BY conversation_id, message_timestamp;

-- ============================================================================
-- SECTION 10: AGENT ACCESS CONTROL
-- ============================================================================

-- Grant access to agents for different roles
GRANT USAGE ON CORTEX AGENT FMG_INTELLIGENCE_AGENT TO ROLE FMG_ANALYST;
GRANT USAGE ON CORTEX AGENT FMG_CS_AGENT TO ROLE FMG_ANALYST;
GRANT USAGE ON CORTEX AGENT FMG_SALES_AGENT TO ROLE FMG_ANALYST;

-- Executive agent only for admin and viewer roles
GRANT USAGE ON CORTEX AGENT FMG_EXECUTIVE_AGENT TO ROLE FMG_ADMIN;
GRANT USAGE ON CORTEX AGENT FMG_EXECUTIVE_AGENT TO ROLE FMG_VIEWER;

-- Power user agent for engineers and data scientists
GRANT USAGE ON CORTEX AGENT FMG_POWER_USER_AGENT TO ROLE FMG_ENGINEER;
GRANT USAGE ON CORTEX AGENT FMG_POWER_USER_AGENT TO ROLE FMG_DATA_SCIENTIST;

-- ============================================================================
-- SECTION 11: LIST ALL AGENTS
-- ============================================================================

-- Show all agents created
SHOW CORTEX AGENTS IN SCHEMA FMG_ANALYTICS.AI;

-- Get agent details
SELECT 
    agent_name,
    display_name,
    description,
    model,
    created_on
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- ============================================================================
-- SECTION 12: BEST PRACTICES
-- ============================================================================

/*
    CORTEX AGENT BEST PRACTICES:
    
    1. TOOL SELECTION
       - Use Analyst for structured data questions
       - Use Search for documentation/knowledge questions
       - Use SQL for specialized, pre-defined queries
       - Combine tools for comprehensive assistance
    
    2. SYSTEM PROMPTS
       - Be specific about the agent's role and capabilities
       - List available tools and when to use them
       - Define output format preferences
       - Include domain-specific context (products, segments, etc.)
    
    3. SEMANTIC VIEWS
       - Create focused semantic views per domain
       - Add comprehensive column comments
       - Include verified queries for common questions
       - Use business-friendly naming
    
    4. SECURITY
       - Create role-specific agents with appropriate access
       - Grant minimal necessary permissions
       - Audit agent usage regularly
       - Review conversation logs for sensitive data
    
    5. MONITORING
       - Track usage patterns by agent and user
       - Monitor response quality
       - Collect user feedback
       - Iterate on prompts and tools based on data
    
    6. USER EXPERIENCE
       - Provide clear agent descriptions
       - Support multi-turn conversations
       - Handle errors gracefully
       - Offer examples of what to ask
*/

-- ============================================================================
-- SCRIPT COMPLETE!
-- ============================================================================

SELECT '✅ Cortex Agents Created Successfully!' AS STATUS,
       'Agents: FMG_INTELLIGENCE_AGENT, FMG_CS_AGENT, FMG_SALES_AGENT, FMG_EXECUTIVE_AGENT, FMG_POWER_USER_AGENT' AS AGENTS_CREATED,
       'Each agent combines Cortex Analyst + Search for unified intelligence' AS CAPABILITY,
       CURRENT_TIMESTAMP() AS COMPLETED_AT;
