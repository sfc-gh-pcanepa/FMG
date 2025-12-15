/*=============================================================================
  FMG SUITE - LAB 4: SNOWFLAKE AGENTS AND INTELLIGENCE
  Script 2: Cortex Search & Analyst with Semantic Views
  
  Description: Build semantic search and natural language analytics using
               Semantic Views for Cortex Analyst
  Prerequisites: Cortex enabled, sample data loaded
  Duration: ~25 minutes
  
  Reference: https://docs.snowflake.com/en/user-guide/views-semantic/overview
=============================================================================*/

-- ============================================================================
-- SECTION 1: SET CONTEXT
-- ============================================================================

USE ROLE FMG_ADMIN;
USE WAREHOUSE FMG_ML_L;
USE DATABASE FMG_ANALYTICS;

-- Create schema for AI/ML work
CREATE SCHEMA IF NOT EXISTS FMG_ANALYTICS.AI
    COMMENT = 'AI and ML experiments, semantic views, and agents';

USE SCHEMA FMG_ANALYTICS.AI;

-- ============================================================================
-- SECTION 2: UNDERSTANDING SEMANTIC VIEWS
-- ============================================================================

/*
    Semantic Views are the foundation for Cortex Analyst:
    
    ┌─────────────────────────────────────────────────────────────────────────┐
    │                     SEMANTIC VIEW ARCHITECTURE                           │
    ├─────────────────────────────────────────────────────────────────────────┤
    │                                                                          │
    │   Natural Language Query                                                │
    │   "What was our MRR by segment last quarter?"                          │
    │                      │                                                   │
    │                      ▼                                                   │
    │   ┌────────────────────────────────────────────┐                        │
    │   │           CORTEX ANALYST                    │                        │
    │   │   • Understands business terms              │                        │
    │   │   • Maps to semantic model                  │                        │
    │   │   • Generates optimized SQL                 │                        │
    │   └────────────────────────────────────────────┘                        │
    │                      │                                                   │
    │                      ▼                                                   │
    │   ┌────────────────────────────────────────────┐                        │
    │   │           SEMANTIC VIEW                     │                        │
    │   │   • Business-friendly column names          │                        │
    │   │   • Defined measures & dimensions           │                        │
    │   │   • Relationships between entities          │                        │
    │   │   • Sample/verified queries                 │                        │
    │   └────────────────────────────────────────────┘                        │
    │                      │                                                   │
    │                      ▼                                                   │
    │   ┌────────────────────────────────────────────┐                        │
    │   │           BASE TABLES                       │                        │
    │   │   • CUSTOMERS                               │                        │
    │   │   • SUBSCRIPTIONS                           │                        │
    │   │   • PLATFORM_USAGE_DAILY                    │                        │
    │   └────────────────────────────────────────────┘                        │
    │                                                                          │
    └─────────────────────────────────────────────────────────────────────────┘
*/

-- ============================================================================
-- SECTION 3: CREATE BASE VIEWS FOR SEMANTIC MODEL
-- ============================================================================

-- Create denormalized base views that the semantic model will reference

-- Customer Analytics Base View
CREATE OR REPLACE VIEW V_CUSTOMER_ANALYTICS_BASE AS
SELECT 
    -- Customer Dimensions
    c.customer_id,
    c.company_name AS customer_name,
    c.segment AS customer_segment,
    c.industry,
    c.sub_industry,
    c.state,
    c.city,
    c.account_status,
    c.csm_owner AS customer_success_manager,
    c.sales_owner AS account_executive,
    c.is_strategic_account,
    c.aum_band AS assets_under_management_band,
    c.employee_count_band,
    c.acquisition_channel,
    c.created_date AS customer_since_date,
    DATEDIFF('month', c.created_date, CURRENT_DATE()) AS customer_tenure_months,
    DATEDIFF('year', c.created_date, CURRENT_DATE()) AS customer_tenure_years,
    
    -- Health Metrics (latest)
    h.overall_health_score,
    h.usage_score,
    h.engagement_score,
    h.support_score,
    h.payment_score,
    h.expansion_score,
    h.churn_risk,
    h.health_trend,
    h.snapshot_date AS health_score_date,
    
    -- Timestamps
    CURRENT_TIMESTAMP() AS _view_refreshed_at
    
FROM FMG_PRODUCTION.RAW.CUSTOMERS c
LEFT JOIN (
    SELECT * FROM FMG_PRODUCTION.RAW.CUSTOMER_HEALTH_SCORES
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY snapshot_date DESC) = 1
) h ON c.customer_id = h.customer_id;

-- Revenue Analytics Base View
CREATE OR REPLACE VIEW V_REVENUE_ANALYTICS_BASE AS
SELECT 
    -- Subscription Dimensions
    s.subscription_id,
    s.customer_id,
    c.company_name AS customer_name,
    c.segment AS customer_segment,
    c.industry,
    s.product_name,
    s.plan_tier,
    s.billing_frequency,
    s.status AS subscription_status,
    s.start_date AS subscription_start_date,
    s.end_date AS subscription_end_date,
    s.renewal_date,
    s.contract_term_months,
    s.auto_renew,
    s.cancellation_reason,
    DATE_TRUNC('month', s.start_date) AS cohort_month,
    DATE_TRUNC('quarter', s.start_date) AS cohort_quarter,
    DATE_TRUNC('year', s.start_date) AS cohort_year,
    
    -- Revenue Measures
    s.mrr_amount AS monthly_recurring_revenue,
    s.arr_amount AS annual_recurring_revenue,
    s.discount_percent,
    s.mrr_amount * (1 - COALESCE(s.discount_percent, 0)/100) AS net_mrr,
    
    -- Timestamps
    s._loaded_at,
    CURRENT_TIMESTAMP() AS _view_refreshed_at
    
FROM FMG_PRODUCTION.RAW.SUBSCRIPTIONS s
JOIN FMG_PRODUCTION.RAW.CUSTOMERS c ON s.customer_id = c.customer_id;

-- Usage Analytics Base View
CREATE OR REPLACE VIEW V_USAGE_ANALYTICS_BASE AS
SELECT 
    -- Time Dimensions
    u.usage_date,
    DATE_TRUNC('week', u.usage_date) AS usage_week,
    DATE_TRUNC('month', u.usage_date) AS usage_month,
    DATE_TRUNC('quarter', u.usage_date) AS usage_quarter,
    DAYNAME(u.usage_date) AS day_of_week,
    
    -- Customer Dimensions
    u.customer_id,
    c.company_name AS customer_name,
    c.segment AS customer_segment,
    c.industry,
    u.user_id,
    
    -- Email Marketing Metrics
    u.emails_sent,
    u.emails_opened,
    u.emails_clicked,
    u.email_templates_used,
    CASE WHEN u.emails_sent > 0 THEN u.emails_opened * 100.0 / u.emails_sent ELSE 0 END AS email_open_rate,
    CASE WHEN u.emails_opened > 0 THEN u.emails_clicked * 100.0 / u.emails_opened ELSE 0 END AS email_click_rate,
    
    -- Social Media Metrics
    u.social_posts_created,
    u.social_posts_published,
    u.social_accounts_connected,
    
    -- Website Metrics
    u.website_page_views,
    u.website_leads_generated,
    u.blog_posts_published,
    
    -- MyRepChat Metrics
    u.myrepchat_messages_sent,
    u.myrepchat_messages_received,
    u.myrepchat_templates_used,
    u.myrepchat_messages_sent + u.myrepchat_messages_received AS total_myrepchat_messages,
    
    -- Events & Cards
    u.events_created,
    u.greeting_cards_sent,
    
    -- Engagement Metrics
    u.total_logins,
    u.session_duration_minutes,
    u.features_used,
    
    CURRENT_TIMESTAMP() AS _view_refreshed_at
    
FROM FMG_PRODUCTION.RAW.PLATFORM_USAGE_DAILY u
JOIN FMG_PRODUCTION.RAW.CUSTOMERS c ON u.customer_id = c.customer_id;

-- Support Analytics Base View  
CREATE OR REPLACE VIEW V_SUPPORT_ANALYTICS_BASE AS
SELECT 
    -- Ticket Dimensions
    t.ticket_id,
    t.customer_id,
    c.company_name AS customer_name,
    c.segment AS customer_segment,
    t.category AS ticket_category,
    t.subcategory AS ticket_subcategory,
    t.priority AS ticket_priority,
    t.status AS ticket_status,
    t.channel AS support_channel,
    t.assigned_agent,
    t.ticket_summary,
    
    -- Time Dimensions
    t.created_date AS ticket_created_date,
    t.resolved_date AS ticket_resolved_date,
    DATE_TRUNC('month', t.created_date) AS ticket_month,
    DATE_TRUNC('week', t.created_date) AS ticket_week,
    
    -- Performance Metrics
    t.resolution_time_hours,
    t.first_response_time_minutes,
    t.csat_score AS customer_satisfaction_score,
    t.sla_met,
    
    -- Calculated Metrics
    CASE 
        WHEN t.status IN ('Resolved', 'Closed') THEN 1 
        ELSE 0 
    END AS is_resolved,
    CASE 
        WHEN t.priority IN ('High', 'Urgent') THEN 1 
        ELSE 0 
    END AS is_high_priority,
    
    CURRENT_TIMESTAMP() AS _view_refreshed_at
    
FROM FMG_PRODUCTION.RAW.SUPPORT_TICKETS t
JOIN FMG_PRODUCTION.RAW.CUSTOMERS c ON t.customer_id = c.customer_id;

-- ============================================================================
-- SECTION 4: CREATE SEMANTIC VIEWS
-- ============================================================================

/*
    Semantic Views define the business logic and terminology that
    Cortex Analyst uses to interpret natural language queries.
    
    Key components:
    - Business-friendly column aliases
    - Defined relationships between entities
    - Metric definitions with formulas
    - Sample queries for training
*/

-- Customer Semantic View
CREATE OR REPLACE SEMANTIC VIEW SV_CUSTOMERS
    COMMENT = 'FMG Customer Analytics - Use for questions about customers, segments, health scores, and churn risk'
AS
SELECT 
    -- Primary Key
    customer_id COMMENT 'Unique identifier for each customer account',
    
    -- Customer Identity
    customer_name COMMENT 'Company or firm name',
    customer_segment COMMENT 'Customer size segment: SMB, Mid-Market, or Enterprise',
    industry COMMENT 'Primary industry: RIA, Broker-Dealer, Insurance, Bank/Credit Union, Wirehouse',
    sub_industry COMMENT 'Specific industry sub-category',
    state COMMENT 'US state where customer is located',
    city COMMENT 'City where customer is located',
    
    -- Account Details
    account_status COMMENT 'Current status: Active, Churned, Paused, or Trial',
    customer_success_manager COMMENT 'Assigned CSM name',
    account_executive COMMENT 'Assigned sales rep',
    is_strategic_account COMMENT 'True if customer is flagged as strategic/high-value',
    acquisition_channel COMMENT 'How customer was acquired: Direct, Partner, Referral, Marketing, Event',
    
    -- Tenure
    customer_since_date COMMENT 'Date customer first signed up',
    customer_tenure_months COMMENT 'Number of months as a customer',
    customer_tenure_years COMMENT 'Number of years as a customer',
    
    -- Health Metrics
    overall_health_score COMMENT 'Overall customer health score from 0-100',
    usage_score COMMENT 'Component score for platform usage',
    engagement_score COMMENT 'Component score for engagement level',
    support_score COMMENT 'Component score based on support interactions',
    payment_score COMMENT 'Component score for payment history',
    churn_risk COMMENT 'Churn risk level: Low, Medium, High, or Critical',
    health_trend COMMENT 'Health score trend: Improving, Stable, or Declining'
    
FROM V_CUSTOMER_ANALYTICS_BASE;

-- Revenue Semantic View
CREATE OR REPLACE SEMANTIC VIEW SV_REVENUE
    COMMENT = 'FMG Revenue Analytics - Use for questions about MRR, ARR, subscriptions, products, and revenue trends'
AS
SELECT 
    -- Keys
    subscription_id COMMENT 'Unique identifier for each subscription',
    customer_id COMMENT 'Related customer account ID',
    customer_name COMMENT 'Company name',
    
    -- Dimensions
    customer_segment COMMENT 'Customer segment: SMB, Mid-Market, Enterprise',
    industry COMMENT 'Customer industry vertical',
    product_name COMMENT 'FMG product: Marketing Suite, Website Pro, MyRepChat, Do It For Me',
    plan_tier COMMENT 'Subscription tier: Starter, Professional, Enterprise, Custom',
    billing_frequency COMMENT 'Monthly or Annual billing',
    subscription_status COMMENT 'Active, Cancelled, Pending, or Expired',
    
    -- Time Dimensions
    subscription_start_date COMMENT 'When subscription began',
    renewal_date COMMENT 'Next renewal date',
    cohort_month COMMENT 'Month customer started - for cohort analysis',
    cohort_quarter COMMENT 'Quarter customer started',
    cohort_year COMMENT 'Year customer started',
    
    -- Revenue Metrics
    monthly_recurring_revenue AS mrr COMMENT 'Monthly Recurring Revenue in dollars',
    annual_recurring_revenue AS arr COMMENT 'Annual Recurring Revenue in dollars',
    net_mrr COMMENT 'MRR after discounts',
    discount_percent COMMENT 'Discount percentage applied'
    
FROM V_REVENUE_ANALYTICS_BASE;

-- Usage Semantic View
CREATE OR REPLACE SEMANTIC VIEW SV_USAGE
    COMMENT = 'FMG Platform Usage Analytics - Use for questions about feature usage, engagement, emails, social posts, and MyRepChat'
AS
SELECT 
    -- Time Dimensions
    usage_date COMMENT 'Date of usage activity',
    usage_week COMMENT 'Week of usage for weekly aggregations',
    usage_month COMMENT 'Month of usage for monthly aggregations',
    day_of_week COMMENT 'Day name (Monday, Tuesday, etc.)',
    
    -- Customer Dimensions
    customer_id COMMENT 'Customer account ID',
    customer_name COMMENT 'Company name',
    customer_segment COMMENT 'Customer segment',
    industry COMMENT 'Customer industry',
    user_id COMMENT 'Individual user ID',
    
    -- Email Marketing
    emails_sent COMMENT 'Number of marketing emails sent',
    emails_opened COMMENT 'Number of emails opened by recipients',
    emails_clicked COMMENT 'Number of email links clicked',
    email_open_rate COMMENT 'Percentage of emails opened',
    email_click_rate COMMENT 'Percentage of opened emails clicked',
    
    -- Social Media
    social_posts_created COMMENT 'Social media posts created',
    social_posts_published COMMENT 'Social media posts published',
    social_accounts_connected COMMENT 'Number of connected social accounts',
    
    -- Website
    website_page_views COMMENT 'Page views on customer website',
    website_leads_generated COMMENT 'Leads captured via website forms',
    blog_posts_published COMMENT 'Blog articles published',
    
    -- MyRepChat (Compliant Texting)
    myrepchat_messages_sent COMMENT 'Text messages sent via MyRepChat',
    myrepchat_messages_received COMMENT 'Text messages received',
    total_myrepchat_messages COMMENT 'Total MyRepChat message volume',
    
    -- Engagement
    total_logins COMMENT 'Number of platform logins',
    session_duration_minutes COMMENT 'Average session length in minutes',
    features_used COMMENT 'Count of distinct features used'
    
FROM V_USAGE_ANALYTICS_BASE;

-- Support Semantic View
CREATE OR REPLACE SEMANTIC VIEW SV_SUPPORT
    COMMENT = 'FMG Support Analytics - Use for questions about tickets, resolution times, CSAT, and support performance'
AS
SELECT 
    -- Keys
    ticket_id COMMENT 'Unique support ticket identifier',
    customer_id COMMENT 'Customer account ID',
    customer_name COMMENT 'Company name',
    customer_segment COMMENT 'Customer segment',
    
    -- Ticket Details
    ticket_category COMMENT 'Ticket category: Technical, Billing, Feature Request, Training, Compliance',
    ticket_subcategory COMMENT 'Specific issue type within category',
    ticket_priority COMMENT 'Priority level: Low, Medium, High, Urgent',
    ticket_status COMMENT 'Current status: Open, In Progress, Waiting, Resolved, Closed',
    support_channel COMMENT 'How ticket was submitted: Email, Phone, Chat, Self-Service',
    assigned_agent COMMENT 'Support agent handling the ticket',
    ticket_summary COMMENT 'Brief description of the issue',
    
    -- Time Dimensions
    ticket_created_date COMMENT 'When ticket was opened',
    ticket_resolved_date COMMENT 'When ticket was resolved',
    ticket_month COMMENT 'Month for aggregations',
    
    -- Performance Metrics
    resolution_time_hours COMMENT 'Hours to resolve the ticket',
    first_response_time_minutes COMMENT 'Minutes until first response',
    customer_satisfaction_score AS csat COMMENT 'Customer satisfaction rating 1-5',
    sla_met COMMENT 'Whether SLA was met for this ticket',
    is_resolved COMMENT 'Whether ticket is resolved (1) or open (0)',
    is_high_priority COMMENT 'Whether ticket is high/urgent priority'
    
FROM V_SUPPORT_ANALYTICS_BASE;

-- ============================================================================
-- SECTION 5: ADD VERIFIED QUERIES TO SEMANTIC VIEWS
-- ============================================================================

/*
    Verified queries improve Cortex Analyst accuracy by providing
    example question-to-SQL mappings.
    
    In Snowsight UI:
    1. Navigate to AI & ML > Cortex Analyst
    2. Select your semantic view
    3. Go to Verified Queries
    4. Add sample questions with their SQL answers
    
    Here we document the verified queries to add via UI:
*/

-- Create a reference table for verified queries
CREATE OR REPLACE TABLE VERIFIED_QUERIES_REFERENCE (
    semantic_view VARCHAR,
    sample_question VARCHAR,
    sql_answer VARCHAR,
    category VARCHAR
);

INSERT INTO VERIFIED_QUERIES_REFERENCE VALUES
    -- Customer Queries
    ('SV_CUSTOMERS', 'How many active customers do we have?', 
     'SELECT COUNT(*) AS active_customers FROM SV_CUSTOMERS WHERE account_status = ''Active''', 'Customer'),
    ('SV_CUSTOMERS', 'What is the breakdown of customers by segment?',
     'SELECT customer_segment, COUNT(*) AS customer_count FROM SV_CUSTOMERS WHERE account_status = ''Active'' GROUP BY customer_segment ORDER BY customer_count DESC', 'Customer'),
    ('SV_CUSTOMERS', 'Which customers have critical churn risk?',
     'SELECT customer_name, customer_segment, overall_health_score, churn_risk FROM SV_CUSTOMERS WHERE churn_risk = ''Critical'' AND account_status = ''Active'' ORDER BY overall_health_score', 'Customer'),
    ('SV_CUSTOMERS', 'What is the average health score by industry?',
     'SELECT industry, ROUND(AVG(overall_health_score), 1) AS avg_health_score FROM SV_CUSTOMERS WHERE account_status = ''Active'' GROUP BY industry ORDER BY avg_health_score DESC', 'Customer'),
     
    -- Revenue Queries
    ('SV_REVENUE', 'What is our total MRR?',
     'SELECT SUM(mrr) AS total_mrr FROM SV_REVENUE WHERE subscription_status = ''Active''', 'Revenue'),
    ('SV_REVENUE', 'What is the MRR by product?',
     'SELECT product_name, SUM(mrr) AS total_mrr FROM SV_REVENUE WHERE subscription_status = ''Active'' GROUP BY product_name ORDER BY total_mrr DESC', 'Revenue'),
    ('SV_REVENUE', 'What is the MRR by segment?',
     'SELECT customer_segment, SUM(mrr) AS total_mrr, COUNT(DISTINCT customer_id) AS customers FROM SV_REVENUE WHERE subscription_status = ''Active'' GROUP BY customer_segment ORDER BY total_mrr DESC', 'Revenue'),
    ('SV_REVENUE', 'What is our average MRR per customer?',
     'SELECT ROUND(SUM(mrr) / COUNT(DISTINCT customer_id), 2) AS avg_mrr_per_customer FROM SV_REVENUE WHERE subscription_status = ''Active''', 'Revenue'),
    ('SV_REVENUE', 'How much revenue churned this month?',
     'SELECT SUM(mrr) AS churned_mrr FROM SV_REVENUE WHERE subscription_status = ''Cancelled'' AND subscription_end_date >= DATE_TRUNC(''month'', CURRENT_DATE())', 'Revenue'),
     
    -- Usage Queries
    ('SV_USAGE', 'How many emails were sent last month?',
     'SELECT SUM(emails_sent) AS total_emails FROM SV_USAGE WHERE usage_month = DATE_TRUNC(''month'', DATEADD(''month'', -1, CURRENT_DATE()))', 'Usage'),
    ('SV_USAGE', 'What is the average email open rate?',
     'SELECT ROUND(AVG(email_open_rate), 2) AS avg_open_rate FROM SV_USAGE WHERE emails_sent > 0', 'Usage'),
    ('SV_USAGE', 'Which customers have the most MyRepChat usage?',
     'SELECT customer_name, SUM(total_myrepchat_messages) AS total_messages FROM SV_USAGE GROUP BY customer_name ORDER BY total_messages DESC LIMIT 10', 'Usage'),
    ('SV_USAGE', 'What is the average session duration by segment?',
     'SELECT customer_segment, ROUND(AVG(session_duration_minutes), 1) AS avg_session_minutes FROM SV_USAGE GROUP BY customer_segment ORDER BY avg_session_minutes DESC', 'Usage'),
     
    -- Support Queries
    ('SV_SUPPORT', 'How many open tickets do we have?',
     'SELECT COUNT(*) AS open_tickets FROM SV_SUPPORT WHERE ticket_status IN (''Open'', ''In Progress'')', 'Support'),
    ('SV_SUPPORT', 'What is the average resolution time?',
     'SELECT ROUND(AVG(resolution_time_hours), 1) AS avg_resolution_hours FROM SV_SUPPORT WHERE is_resolved = 1', 'Support'),
    ('SV_SUPPORT', 'What is our average CSAT score?',
     'SELECT ROUND(AVG(csat), 2) AS avg_csat FROM SV_SUPPORT WHERE csat IS NOT NULL', 'Support'),
    ('SV_SUPPORT', 'What percentage of tickets meet SLA?',
     'SELECT ROUND(AVG(CASE WHEN sla_met THEN 1 ELSE 0 END) * 100, 1) AS sla_met_pct FROM SV_SUPPORT', 'Support');

-- View the verified queries reference
SELECT * FROM VERIFIED_QUERIES_REFERENCE ORDER BY semantic_view, category;

-- ============================================================================
-- SECTION 6: CREATE CORTEX SEARCH SERVICE
-- ============================================================================

-- Create a table for FMG support articles (knowledge base)
CREATE OR REPLACE TABLE SUPPORT_KNOWLEDGE_BASE (
    article_id VARCHAR(20) PRIMARY KEY,
    title VARCHAR(500),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    content TEXT,
    keywords VARCHAR(500),
    product VARCHAR(100),
    created_date DATE,
    last_updated DATE,
    view_count INTEGER DEFAULT 0,
    helpful_votes INTEGER DEFAULT 0,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Insert sample support articles
INSERT INTO SUPPORT_KNOWLEDGE_BASE (article_id, title, category, subcategory, content, keywords, product, created_date, last_updated, view_count, helpful_votes)
VALUES
    ('KB-001', 'How to Set Up Your First Email Campaign', 'Email Marketing', 'Getting Started', 
     'This guide walks you through creating your first email campaign in FMG Suite. Step 1: Navigate to Marketing Tools from the main menu. Step 2: Click Create Campaign button. Step 3: Choose a template from our library of pre-approved, compliance-ready designs that are perfect for financial advisors. Step 4: Customize your content using our intuitive drag-and-drop editor. You can add your logo, change colors, and personalize the message. Step 5: Select your recipient list from your contacts. Step 6: Preview your email and either schedule for later or send immediately. All emails are automatically archived for compliance.',
     'email, campaign, setup, template, marketing, getting started', 'Marketing Suite', '2024-01-15', '2024-12-01', 1250, 89),
     
    ('KB-002', 'Troubleshooting Email Delivery Issues', 'Email Marketing', 'Troubleshooting',
     'If your marketing emails are not being delivered to recipients, here are the most common issues and solutions. First, verify your sender domain is properly authenticated with SPF, DKIM, and DMARC records. Contact your IT team or domain provider if these are not set up. Second, check if recipient email addresses are valid and not bouncing. Third, review your bounce reports in the Analytics section to identify patterns. Fourth, ensure your email content does not trigger spam filters by avoiding excessive links, all-caps text, or spam trigger words. Fifth, if you see bounce rates over 5%, contact FMG support immediately as this may indicate a deliverability issue.',
     'email, delivery, bounce, spam, troubleshooting, not received, missing', 'Marketing Suite', '2024-02-10', '2024-11-15', 890, 67),
     
    ('KB-003', 'Getting Started with MyRepChat Compliant Texting', 'MyRepChat', 'Getting Started',
     'MyRepChat allows financial advisors to text clients compliantly while maintaining full records for regulatory requirements. Setup is easy: First, download the MyRepChat app from the App Store for iOS or Google Play for Android devices. Second, log in using your existing FMG Suite credentials - no separate account needed. Third, complete your profile information and read through the compliance acknowledgment. Fourth, you are ready to start messaging clients! All messages are automatically archived and can be searched by compliance officers. You can also access MyRepChat from your desktop through the FMG Suite web interface.',
     'myrepchat, texting, sms, compliance, mobile, text message, clients', 'MyRepChat', '2024-03-01', '2024-10-20', 2100, 156),
     
    ('KB-004', 'Understanding Your Website Analytics Dashboard', 'Website', 'Analytics',
     'Your FMG website includes a comprehensive built-in analytics dashboard. To access your stats, go to Website section and then click Analytics. You can see key metrics including page views, unique visitors, session duration, and traffic sources. The lead capture section shows all form submissions from your website. You can export reports for your records or to share with your team. Key metrics to monitor: Bounce rate should ideally be under 60 percent - if higher, consider improving your content. Time on site should average over 2 minutes for good engagement. Conversion rate measures how many visitors take action.',
     'analytics, traffic, website, visitors, metrics, dashboard, reports', 'Website Pro', '2024-03-15', '2024-09-30', 780, 45),
     
    ('KB-005', 'How to Schedule Social Media Posts in Advance', 'Social Media', 'Features',
     'Save time and maintain consistent posting by scheduling your social media content ahead of time. Here is how to schedule posts in FMG Suite. Go to Social Media and click Create Post. Write your content from scratch or choose from our curated library of financial advisor content. Select which platforms to post to - you can choose LinkedIn, Facebook, and Twitter/X. Instead of clicking Post Now, click the Schedule button. Choose your preferred date and time for the post to go live. You can view all your scheduled posts in the Calendar view to see your content pipeline. Best practice: Schedule posts during peak engagement hours, typically Tuesday through Thursday between 10am and 2pm.',
     'social media, scheduling, linkedin, facebook, twitter, posts, content calendar', 'Marketing Suite', '2024-04-01', '2024-11-01', 650, 52),
     
    ('KB-006', 'The Compliance Review Process Explained', 'Compliance', 'Process',
     'All marketing content created in FMG Suite goes through a compliance review process to ensure regulatory adherence. Here is how it works. When you create new custom content, submit it for compliance review. Our compliance team reviews submissions within 1-2 business days. You will receive either an approval to proceed or specific revision requests. Approved content is immediately available for use. If revisions are needed, the feedback will explain exactly what changes are required. Pro tip: Use our pre-approved templates to bypass the review process entirely - these templates have already been reviewed and approved for immediate use by financial advisors.',
     'compliance, review, approval, marketing, content, finra, sec, regulatory', 'Marketing Suite', '2024-04-15', '2024-08-15', 1450, 98),
     
    ('KB-007', 'Setting Up Lead Capture Forms on Your Website', 'Website', 'Lead Generation',
     'Capture leads effectively with custom forms on your FMG website. To create a new form, go to Website and then Forms. Choose from several form types including Contact Us, Newsletter Signup, Free Consultation Request, or Event Registration. Customize the form fields to collect the information you need. Set up email notifications so you receive an alert when a new lead comes in. If you have a CRM integration, configure it to automatically sync new leads. Finally, embed the form on your desired webpage using the provided code or our simple page editor. Forms are mobile-responsive and GDPR compliant.',
     'leads, forms, capture, contact, website, landing page, conversion', 'Website Pro', '2024-05-01', '2024-10-01', 920, 71),
     
    ('KB-008', 'Managing Your Billing and Subscription', 'Account', 'Billing',
     'Manage your FMG subscription and billing details easily through your account settings. Go to Account and then Billing to access all billing features. View your current plan details and feature usage. Update your payment method including credit card or ACH. Download past invoices for your records or expense reporting. Upgrade to a higher tier or add additional products. If you need to change your plan or have billing questions, contact our billing team at billing@fmgsuite.com or call (858) 251-2420 during business hours Monday through Friday 6am to 5pm Pacific time.',
     'billing, subscription, payment, invoice, plan, upgrade, pricing', 'Account', '2024-05-15', '2024-07-01', 560, 38),
     
    ('KB-009', 'Integrating FMG with Your CRM System', 'Integrations', 'Setup',
     'FMG Suite integrates seamlessly with popular CRM systems used by financial advisors including Salesforce, Redtail, and Wealthbox. To set up the integration, go to Settings and then Integrations. Select your CRM from the list of available integrations. Authorize the connection by logging in with your CRM credentials. Configure your sync settings to determine what data flows between systems - you can sync contacts, activities, leads, and more. Test the connection to ensure everything is working properly. Once connected, data syncs automatically every 15 minutes. If you need help with integration setup, contact our support team for a guided walkthrough.',
     'crm, integration, salesforce, redtail, wealthbox, sync, contacts, automation', 'Marketing Suite', '2024-06-01', '2024-11-10', 1100, 82),
     
    ('KB-010', 'Event Marketing: Promoting Client Seminars and Webinars', 'Events', 'Marketing',
     'Use FMG event marketing tools to promote your client seminars, webinars, and educational events. Features include custom event landing page creation with registration forms, email invitation templates designed for financial services, social media promotional content, full registration and attendee management, automated reminder sequences leading up to the event, and post-event follow-up email campaigns. To create an event, go to Events and click Create New Event. Fill in your event details, choose your promotion channels, and let FMG help you fill seats. You can track RSVPs and attendance in real-time.',
     'events, seminar, webinar, registration, promotion, marketing, clients', 'Marketing Suite', '2024-06-15', '2024-12-01', 730, 55);

-- Create the Cortex Search Service
CREATE OR REPLACE CORTEX SEARCH SERVICE FMG_SUPPORT_SEARCH_SERVICE
    ON content
    ATTRIBUTES category, subcategory, product, title
    WAREHOUSE = FMG_ML_L
    TARGET_LAG = '1 hour'
    COMMENT = 'Semantic search service for FMG support knowledge base'
AS (
    SELECT 
        article_id,
        title,
        category,
        subcategory,
        product,
        content,
        keywords
    FROM SUPPORT_KNOWLEDGE_BASE
);

-- ============================================================================
-- SECTION 7: QUERY THE SEARCH SERVICE
-- ============================================================================

-- Test search for articles about email issues
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'FMG_ANALYTICS.AI.FMG_SUPPORT_SEARCH_SERVICE',
        '{
            "query": "my emails are not being delivered to clients",
            "columns": ["article_id", "title", "category", "product"],
            "limit": 5
        }'
    )
) AS search_results;

-- Search with product filter
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'FMG_ANALYTICS.AI.FMG_SUPPORT_SEARCH_SERVICE',
        '{
            "query": "how do I connect my CRM",
            "columns": ["article_id", "title", "content"],
            "filter": {"@eq": {"product": "Marketing Suite"}},
            "limit": 3
        }'
    )
) AS search_results;

-- ============================================================================
-- SECTION 8: CREATE SEARCH FUNCTION FOR AGENT USE
-- ============================================================================

-- Create a SQL function to search the knowledge base
CREATE OR REPLACE FUNCTION SEARCH_FMG_KNOWLEDGE_BASE(
    search_query VARCHAR,
    max_results INTEGER DEFAULT 3
)
RETURNS TABLE (
    article_id VARCHAR,
    title VARCHAR,
    category VARCHAR,
    product VARCHAR,
    content_excerpt VARCHAR,
    relevance_score FLOAT
)
LANGUAGE SQL
AS
$$
    SELECT 
        result.value:article_id::VARCHAR AS article_id,
        result.value:title::VARCHAR AS title,
        result.value:category::VARCHAR AS category,
        result.value:product::VARCHAR AS product,
        LEFT(result.value:content::VARCHAR, 300) || '...' AS content_excerpt,
        result.value:score::FLOAT AS relevance_score
    FROM (
        SELECT PARSE_JSON(
            SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                'FMG_ANALYTICS.AI.FMG_SUPPORT_SEARCH_SERVICE',
                OBJECT_CONSTRUCT(
                    'query', search_query,
                    'columns', ARRAY_CONSTRUCT('article_id', 'title', 'category', 'product', 'content'),
                    'limit', max_results
                )::VARCHAR
            )
        ) AS results
    ),
    LATERAL FLATTEN(input => results:results) AS result
$$;

-- Test the search function
SELECT * FROM TABLE(SEARCH_FMG_KNOWLEDGE_BASE('scheduling social media posts'));

-- ============================================================================
-- SECTION 9: VERIFY SEMANTIC VIEWS
-- ============================================================================

-- List all semantic views
SHOW SEMANTIC VIEWS IN SCHEMA FMG_ANALYTICS.AI;

-- Test querying semantic views directly
SELECT customer_segment, COUNT(*) AS customers, ROUND(AVG(overall_health_score), 1) AS avg_health
FROM SV_CUSTOMERS
WHERE account_status = 'Active'
GROUP BY customer_segment
ORDER BY customers DESC;

SELECT product_name, SUM(mrr) AS total_mrr, COUNT(*) AS subscriptions
FROM SV_REVENUE
WHERE subscription_status = 'Active'
GROUP BY product_name
ORDER BY total_mrr DESC;

-- ============================================================================
-- SECTION 10: BEST PRACTICES
-- ============================================================================

/*
    SEMANTIC VIEW BEST PRACTICES:
    
    1. DESIGN
       - Use business-friendly column names (customer_name, not cust_nm)
       - Add descriptive COMMENT on every column
       - Create separate semantic views per domain (customers, revenue, etc.)
       - Include calculated fields for common metrics
    
    2. VERIFIED QUERIES
       - Add 10-20 verified queries per semantic view
       - Cover common question patterns
       - Include edge cases and variations
       - Update regularly based on user feedback
    
    3. OPTIMIZATION
       - Use denormalized base views for performance
       - Pre-calculate common aggregations
       - Refresh base views appropriately
       - Monitor query patterns for optimization
    
    4. GOVERNANCE
       - Apply appropriate access controls
       - Document data lineage
       - Version semantic models
       - Track usage and accuracy
*/

-- ============================================================================
-- SCRIPT COMPLETE!
-- ============================================================================

SELECT '✅ Cortex Search & Analyst Setup Complete!' AS STATUS,
       'Semantic views created: SV_CUSTOMERS, SV_REVENUE, SV_USAGE, SV_SUPPORT' AS SEMANTIC_VIEWS,
       'Search service created: FMG_SUPPORT_SEARCH_SERVICE' AS SEARCH_SERVICE,
       'Next: Create Cortex Agent to combine Analyst + Search' AS NEXT_STEP,
       CURRENT_TIMESTAMP() AS COMPLETED_AT;
