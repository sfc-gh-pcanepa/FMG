/*=============================================================================
  FMG SUITE - DATA PROVIDER SETUP
  
  Run this in YOUR account (the data provider) to:
  1. Generate 500k+ sample FMG records
  2. Create a share to deliver to the prospect's trial account
  
  After running this, add the prospect's account to the share.
  
  GENERATED DATA:
  • CUSTOMERS - ~100,000 customer records
  • USERS - ~250,000 user records (2-3 per customer)
  • SUBSCRIPTIONS - ~200,000 subscription records (2 per customer)
  • CUSTOMER_FEEDBACK - ~50,000 NPS feedback records
  • KNOWLEDGE_BASE - 10 help articles (reference data)
  
  TOTAL: ~600,000 records
  
  All labs consume from this share and are INDEPENDENT of each other.
=============================================================================*/

USE ROLE ACCOUNTADMIN;

-- ============================================================================
-- STEP 1: CREATE DATABASE AND SCHEMA
-- ============================================================================
CREATE DATABASE IF NOT EXISTS FMG_SAMPLE_DATA;
CREATE SCHEMA IF NOT EXISTS FMG_SAMPLE_DATA.FMG;
USE SCHEMA FMG_SAMPLE_DATA.FMG;

CREATE WAREHOUSE IF NOT EXISTS FMG_SETUP_WH
    WAREHOUSE_SIZE = 'MEDIUM'  -- Use MEDIUM for faster generation
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

USE WAREHOUSE FMG_SETUP_WH;

-- ============================================================================
-- STEP 2: CREATE TABLES
-- ============================================================================

-- CUSTOMERS
CREATE OR REPLACE TABLE CUSTOMERS (
    customer_id VARCHAR(20),
    company_name VARCHAR(200),
    segment VARCHAR(50),
    industry VARCHAR(100),
    mrr DECIMAL(10,2),
    health_score INT,
    created_date DATE
);

-- USERS (with PII for masking demo in Lab 2)
CREATE OR REPLACE TABLE USERS (
    user_id VARCHAR(20),
    customer_id VARCHAR(20),
    email VARCHAR(200),
    phone VARCHAR(20),
    full_name VARCHAR(100),
    role VARCHAR(50)
);

-- SUBSCRIPTIONS
CREATE OR REPLACE TABLE SUBSCRIPTIONS (
    subscription_id VARCHAR(20),
    customer_id VARCHAR(20),
    product VARCHAR(50),
    mrr DECIMAL(10,2),
    status VARCHAR(20),
    start_date DATE
);

-- CUSTOMER_FEEDBACK (for Cortex demos in Lab 4)
CREATE OR REPLACE TABLE CUSTOMER_FEEDBACK (
    feedback_id VARCHAR(20),
    customer_id VARCHAR(20),
    nps_score INT,
    feedback_text VARCHAR(2000),
    submitted_date DATE
);

-- KNOWLEDGE_BASE (for Cortex Search demo in Lab 4)
CREATE OR REPLACE TABLE KNOWLEDGE_BASE (
    article_id VARCHAR(10),
    title VARCHAR(200),
    content VARCHAR(4000),
    category VARCHAR(50)
);

-- ============================================================================
-- STEP 3: GENERATE 100,000 CUSTOMERS
-- ============================================================================

-- Reference arrays for realistic data generation
CREATE OR REPLACE TEMPORARY TABLE REF_DATA AS
SELECT 
    -- Company name components
    ARRAY_CONSTRUCT(
        'Apex', 'Summit', 'Peak', 'Horizon', 'Cascade', 'Alpine', 'Meridian', 'Coastal',
        'Pacific', 'Atlantic', 'Mountain', 'Valley', 'Harbor', 'Liberty', 'Patriot',
        'Heritage', 'Legacy', 'Premier', 'Elite', 'Crown', 'Royal', 'Noble', 'Capital',
        'Pinnacle', 'Sterling', 'Golden', 'Silver', 'Platinum', 'Diamond', 'Emerald',
        'Sapphire', 'Ruby', 'Cornerstone', 'Keystone', 'Milestone', 'Landmark', 'Beacon',
        'Lighthouse', 'Compass', 'Navigator', 'Voyager', 'Pioneer', 'Frontier', 'Trailblazer',
        'Vanguard', 'Sentinel', 'Guardian', 'Shield', 'Fortress', 'Citadel', 'Bastion'
    ) AS company_prefixes,
    ARRAY_CONSTRUCT(
        'Financial', 'Wealth', 'Investment', 'Advisory', 'Capital', 'Asset', 'Retirement',
        'Estate', 'Portfolio', 'Fiduciary', 'Fiscal', 'Monetary', 'Strategic', 'Private',
        'Family', 'Corporate', 'Business', 'Professional', 'Executive', 'Senior'
    ) AS company_middles,
    ARRAY_CONSTRUCT(
        'Group', 'Partners', 'Advisors', 'Management', 'Services', 'Solutions', 'Associates',
        'Consulting', 'Planning', 'Strategies', 'Resources', 'Holdings', 'Trust', 'LLC', 'Inc'
    ) AS company_suffixes,
    ARRAY_CONSTRUCT('Enterprise', 'Mid-Market', 'SMB') AS segments,
    ARRAY_CONSTRUCT('RIA', 'Independent RIA', 'Broker-Dealer', 'Wirehouse', 'Insurance', 'Bank Trust', 'Family Office', 'Hybrid') AS industries,
    ARRAY_CONSTRUCT(
        'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA',
        'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
        'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT',
        'VA', 'WA', 'WV', 'WI', 'WY'
    ) AS states;

INSERT INTO CUSTOMERS
WITH customer_gen AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY SEQ4()) AS rn,
        UNIFORM(1, 50, RANDOM()) AS prefix_idx,
        UNIFORM(1, 20, RANDOM()) AS middle_idx,
        UNIFORM(1, 15, RANDOM()) AS suffix_idx,
        UNIFORM(1, 3, RANDOM()) AS segment_idx,
        UNIFORM(1, 8, RANDOM()) AS industry_idx,
        UNIFORM(1, 50, RANDOM()) AS state_idx,
        UNIFORM(0, 100, RANDOM()) AS health_score,
        UNIFORM(1, 1500, RANDOM()) AS days_ago
    FROM TABLE(GENERATOR(ROWCOUNT => 100000))
),
ref AS (SELECT * FROM REF_DATA)
SELECT 
    'C' || LPAD(rn::VARCHAR, 6, '0') AS customer_id,
    ref.company_prefixes[prefix_idx-1]::VARCHAR || ' ' || 
    ref.company_middles[middle_idx-1]::VARCHAR || ' ' || 
    ref.company_suffixes[suffix_idx-1]::VARCHAR AS company_name,
    ref.segments[segment_idx-1]::VARCHAR AS segment,
    ref.industries[industry_idx-1]::VARCHAR AS industry,
    CASE ref.segments[segment_idx-1]::VARCHAR
        WHEN 'Enterprise' THEN UNIFORM(2000, 10000, RANDOM())
        WHEN 'Mid-Market' THEN UNIFORM(500, 2500, RANDOM())
        ELSE UNIFORM(99, 800, RANDOM())
    END AS mrr,
    health_score,
    DATEADD('day', -days_ago, CURRENT_DATE()) AS created_date
FROM customer_gen, ref;

SELECT 'CUSTOMERS generated: ' || COUNT(*) AS status FROM CUSTOMERS;

-- ============================================================================
-- STEP 4: GENERATE ~250,000 USERS (2-3 per customer)
-- ============================================================================

CREATE OR REPLACE TEMPORARY TABLE REF_NAMES AS
SELECT 
    ARRAY_CONSTRUCT(
        'James', 'John', 'Robert', 'Michael', 'William', 'David', 'Richard', 'Joseph', 'Thomas', 'Christopher',
        'Mary', 'Patricia', 'Jennifer', 'Linda', 'Elizabeth', 'Barbara', 'Susan', 'Jessica', 'Sarah', 'Karen',
        'Daniel', 'Matthew', 'Anthony', 'Mark', 'Donald', 'Steven', 'Paul', 'Andrew', 'Joshua', 'Kenneth',
        'Nancy', 'Betty', 'Margaret', 'Sandra', 'Ashley', 'Kimberly', 'Emily', 'Donna', 'Michelle', 'Dorothy',
        'Brian', 'George', 'Edward', 'Ronald', 'Timothy', 'Jason', 'Jeffrey', 'Ryan', 'Jacob', 'Gary',
        'Lisa', 'Helen', 'Samantha', 'Katherine', 'Christine', 'Deborah', 'Rachel', 'Laura', 'Carolyn', 'Janet'
    ) AS first_names,
    ARRAY_CONSTRUCT(
        'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez',
        'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson', 'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin',
        'Lee', 'Perez', 'Thompson', 'White', 'Harris', 'Sanchez', 'Clark', 'Ramirez', 'Lewis', 'Robinson',
        'Walker', 'Young', 'Allen', 'King', 'Wright', 'Scott', 'Torres', 'Nguyen', 'Hill', 'Flores',
        'Green', 'Adams', 'Nelson', 'Baker', 'Hall', 'Rivera', 'Campbell', 'Mitchell', 'Carter', 'Roberts'
    ) AS last_names,
    ARRAY_CONSTRUCT('Admin', 'Advisor', 'Analyst', 'Compliance', 'Associate', 'Manager', 'Director', 'VP') AS roles;

INSERT INTO USERS
WITH user_gen AS (
    SELECT 
        c.customer_id,
        LOWER(REPLACE(c.company_name, ' ', '')) AS email_domain,
        u.user_num,
        UNIFORM(1, 60, RANDOM()) AS first_idx,
        UNIFORM(1, 50, RANDOM()) AS last_idx,
        UNIFORM(1, 8, RANDOM()) AS role_idx,
        UNIFORM(100, 999, RANDOM()) AS area_code,
        UNIFORM(100, 999, RANDOM()) AS phone_prefix,
        UNIFORM(1000, 9999, RANDOM()) AS phone_suffix
    FROM CUSTOMERS c,
    LATERAL (
        SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) AS user_num
        FROM TABLE(GENERATOR(ROWCOUNT => 3))
        WHERE SEQ4() <= UNIFORM(2, 3, RANDOM(c.customer_id::INT))
    ) u
),
ref AS (SELECT * FROM REF_NAMES)
SELECT 
    'U' || LPAD(ROW_NUMBER() OVER (ORDER BY customer_id, user_num)::VARCHAR, 7, '0') AS user_id,
    customer_id,
    LOWER(ref.first_names[first_idx-1]::VARCHAR) || '.' || 
    LOWER(ref.last_names[last_idx-1]::VARCHAR) || '@' || 
    LEFT(email_domain, 15) || '.com' AS email,
    '(' || area_code || ') ' || phone_prefix || '-' || phone_suffix AS phone,
    ref.first_names[first_idx-1]::VARCHAR || ' ' || ref.last_names[last_idx-1]::VARCHAR AS full_name,
    ref.roles[role_idx-1]::VARCHAR AS role
FROM user_gen, ref;

SELECT 'USERS generated: ' || COUNT(*) AS status FROM USERS;

-- ============================================================================
-- STEP 5: GENERATE ~200,000 SUBSCRIPTIONS (2 per customer)
-- ============================================================================

CREATE OR REPLACE TEMPORARY TABLE REF_PRODUCTS AS
SELECT 
    ARRAY_CONSTRUCT('Marketing Suite', 'Website Pro', 'MyRepChat', 'Do It For Me', 'Social Pro', 'Email Plus', 'Compliance Vault', 'Analytics Pro') AS products,
    ARRAY_CONSTRUCT('Active', 'Active', 'Active', 'Active', 'Active', 'Active', 'Active', 'Cancelled', 'Paused') AS statuses;

INSERT INTO SUBSCRIPTIONS
WITH sub_gen AS (
    SELECT 
        c.customer_id,
        c.segment,
        c.created_date,
        s.sub_num,
        UNIFORM(1, 8, RANDOM()) AS product_idx,
        UNIFORM(1, 9, RANDOM()) AS status_idx
    FROM CUSTOMERS c,
    LATERAL (
        SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) AS sub_num
        FROM TABLE(GENERATOR(ROWCOUNT => 2))
    ) s
),
ref AS (SELECT * FROM REF_PRODUCTS)
SELECT 
    'S' || LPAD(ROW_NUMBER() OVER (ORDER BY customer_id, sub_num)::VARCHAR, 7, '0') AS subscription_id,
    customer_id,
    ref.products[product_idx-1]::VARCHAR AS product,
    CASE segment
        WHEN 'Enterprise' THEN UNIFORM(500, 3000, RANDOM())
        WHEN 'Mid-Market' THEN UNIFORM(200, 1000, RANDOM())
        ELSE UNIFORM(50, 400, RANDOM())
    END AS mrr,
    ref.statuses[status_idx-1]::VARCHAR AS status,
    DATEADD('day', UNIFORM(0, 30, RANDOM()), created_date) AS start_date
FROM sub_gen, ref;

SELECT 'SUBSCRIPTIONS generated: ' || COUNT(*) AS status FROM SUBSCRIPTIONS;

-- ============================================================================
-- STEP 6: GENERATE ~50,000 CUSTOMER FEEDBACK RECORDS
-- ============================================================================

CREATE OR REPLACE TEMPORARY TABLE REF_FEEDBACK AS
SELECT 
    -- Positive feedback templates (NPS 9-10)
    ARRAY_CONSTRUCT(
        'Love the email marketing tools! Our open rates have increased significantly since switching to FMG. The templates are professional and the analytics dashboard gives us great insights.',
        'Amazing support team! They helped us set up everything quickly. Highly recommend! The onboarding specialist was patient and knowledgeable about compliance requirements.',
        'Best decision we made this year. The ROI is incredible. We have seen a significant increase in leads from our website.',
        'MyRepChat has been a game-changer for client communication. Compliance loves it because everything is automatically archived.',
        'The compliance features are top-notch. Pre-approved content library saves us hours of work getting marketing materials approved.',
        'Just used the AI content generator for the first time - wow! It created a great draft newsletter in seconds. This feature alone is worth the subscription.',
        'Outstanding platform for financial advisors. The integration with our workflow has been seamless.',
        'The website builder exceeded our expectations. Our new site looks like we spent thousands on a custom design.',
        'Customer service is exceptional. Every time we have a question, the team responds quickly with helpful solutions.',
        'The social media scheduling feature has transformed our online presence. We are now consistently engaging with clients.'
    ) AS positive_feedback,
    -- Neutral feedback templates (NPS 7-8)
    ARRAY_CONSTRUCT(
        'Good product overall but the social media scheduler could be more intuitive. Sometimes posts dont publish at the scheduled time.',
        'The website builder is fantastic. Would love to see more templates though. Also mobile responsiveness could be improved.',
        'Solid platform but wish there was better integration with our CRM. Currently we have to manually export data.',
        'Great for our practice. The features are comprehensive but the learning curve was steeper than expected.',
        'Happy with the service but would appreciate more customization options for email templates.',
        'The platform does what we need but the reporting could be more detailed.',
        'Good value for money. A few minor bugs here and there but nothing major.',
        'Works well for our needs. Would be nice to have more video tutorials for advanced features.'
    ) AS neutral_feedback,
    -- Negative feedback templates (NPS 1-6)
    ARRAY_CONSTRUCT(
        'Disappointed with the recent price increase. We are considering other options. The value proposition is not as strong as it used to be.',
        'Had some issues with email deliverability last month. Support was helpful but it took too long to resolve.',
        'The reporting dashboard is confusing and hard to navigate. I need to export to Excel just to get basic metrics.',
        'Too expensive for what you get. There are cheaper alternatives that offer similar features.',
        'The mobile app needs work. It crashes frequently and is missing key features from the desktop version.',
        'Frustrated with the lack of updates. Competitors are adding new features while FMG seems stagnant.',
        'Customer support response times have gotten worse over the past few months.',
        'The integration with third-party tools is limited. We expected better connectivity.'
    ) AS negative_feedback;

INSERT INTO CUSTOMER_FEEDBACK
WITH feedback_gen AS (
    SELECT 
        c.customer_id,
        UNIFORM(1, 10, RANDOM()) AS nps_score,
        UNIFORM(1, 365, RANDOM()) AS days_ago
    FROM CUSTOMERS c
    WHERE UNIFORM(1, 100, RANDOM()) <= 50  -- 50% of customers have feedback
),
ref AS (SELECT * FROM REF_FEEDBACK)
SELECT 
    'F' || LPAD(ROW_NUMBER() OVER (ORDER BY customer_id)::VARCHAR, 6, '0') AS feedback_id,
    customer_id,
    nps_score,
    CASE 
        WHEN nps_score >= 9 THEN ref.positive_feedback[UNIFORM(0, 9, RANDOM())]::VARCHAR
        WHEN nps_score >= 7 THEN ref.neutral_feedback[UNIFORM(0, 7, RANDOM())]::VARCHAR
        ELSE ref.negative_feedback[UNIFORM(0, 7, RANDOM())]::VARCHAR
    END AS feedback_text,
    DATEADD('day', -days_ago, CURRENT_DATE()) AS submitted_date
FROM feedback_gen, ref;

SELECT 'CUSTOMER_FEEDBACK generated: ' || COUNT(*) AS status FROM CUSTOMER_FEEDBACK;

-- ============================================================================
-- STEP 7: CREATE KNOWLEDGE BASE (Static reference data)
-- ============================================================================

INSERT INTO KNOWLEDGE_BASE VALUES
    ('KB001', 'How to Create Email Campaigns', 'Navigate to Marketing Tools and click Create Campaign. Select a template from our library of pre-approved designs. Customize your content using the drag-and-drop editor. Add personalization tokens like {{first_name}} for a personal touch. Choose recipients from your contact list or create a new segment. Preview your email on desktop and mobile. Schedule for later or send immediately. Pro tip: Use A/B testing for subject lines to optimize open rates. Monitor results in the Analytics dashboard.', 'Email Marketing'),
    ('KB002', 'Troubleshooting Email Delivery Issues', 'If emails are not delivering, follow these steps: 1) Check SPF/DKIM settings in your domain DNS records - we provide the exact records needed in Settings > Email Authentication. 2) Verify recipient email addresses are valid and not bouncing. 3) Review bounce reports in Analytics > Email > Deliverability. 4) Ensure content is not triggering spam filters - avoid excessive caps, multiple exclamation points, and spam trigger words. 5) Check if you are on any blacklists using our built-in reputation checker. Contact support if issues persist after these steps.', 'Email Marketing'),
    ('KB003', 'Setting Up MyRepChat for Compliant Messaging', 'Download the MyRepChat app from the Apple App Store or Google Play Store. Log in with your FMG credentials - use the same email and password as your main account. Complete the compliance acknowledgment form that appears on first login. Configure your notification preferences in Settings. Start messaging clients - all conversations are automatically archived to our secure compliance vault. Messages are retained for 7 years by default (configurable). Supervisors can review conversations in the Compliance Dashboard. Export archives anytime for audits.', 'MyRepChat'),
    ('KB004', 'Website Analytics and Reporting', 'Access your website analytics via Dashboard > Website > Analytics. Key metrics available: Page views (total and unique), Visitor demographics and geography, Session duration and bounce rate, Lead capture form submissions, Conversion rates by traffic source, Most popular pages and content. Export reports in PDF or Excel format for client meetings or compliance records. Set up automated weekly reports via Settings > Scheduled Reports. Compare time periods to track growth trends.', 'Website'),
    ('KB005', 'Scheduling Social Media Posts', 'Go to Social Media > Create Post in your dashboard. Write your content manually or click AI Assist to generate content suggestions. Add images or videos - we auto-resize for each platform. Select target platforms: LinkedIn, Facebook, Twitter/X, Instagram. Click Schedule and pick your preferred date and time. View all scheduled posts in the Calendar view. Best practices: Post 3-5 times per week, use hashtags strategically, engage with comments within 24 hours. Compliance note: All posts are automatically submitted for review if you have pre-approval enabled.', 'Social Media'),
    ('KB006', 'Compliance Archive and Audit Access', 'All marketing communications are automatically archived for compliance. Access via Compliance > Archive in the main navigation. Search by date range, client name, communication type, or keyword. Filter by channel: email, social media, website forms, MyRepChat. Export records in SEC/FINRA compliant formats for audits. Retention period is configurable: 3, 5, 7, or 10 years based on your compliance requirements. Set up automatic audit reports for your compliance officer. Supervision workflows available for pre-approval of content.', 'Compliance'),
    ('KB007', 'Integrating with CRM Systems', 'FMG integrates with major CRM platforms including Salesforce, Redtail, Wealthbox, and Microsoft Dynamics. Go to Settings > Integrations to connect your CRM. Sync options: Contact sync (bidirectional), Activity logging, Lead capture routing, Campaign tracking. API access available for custom integrations - contact support for API documentation. Zapier integration enables connections with 3000+ other apps. Data sync frequency: real-time for most CRMs, hourly for legacy systems.', 'Integrations'),
    ('KB008', 'Billing and Subscription Management', 'View your current subscription in Settings > Billing. Change plans anytime - upgrades are prorated, downgrades take effect at next billing cycle. Add or remove user seats as needed. Payment methods: Credit card, ACH bank transfer, or invoice (Enterprise only). Download invoices for any billing period. Cancel anytime with 30 days notice - no long-term contracts required. Volume discounts available for 10+ seats. Contact your account manager for custom Enterprise pricing.', 'Account'),
    ('KB009', 'Using the AI Content Generator', 'Access AI-powered content creation in any text editor by clicking the AI Assist button. Choose content type: Email, Social Post, Blog Article, or Newsletter. Provide a brief topic or prompt. The AI generates compliant, professional content tailored for financial services. Edit and customize the output as needed. All AI-generated content is automatically checked against compliance rules. Save frequently used prompts as templates. Pro tip: Be specific in your prompts for better results.', 'AI Features'),
    ('KB010', 'Mobile App Features and Setup', 'Download the FMG mobile app from your app store. Log in with your existing credentials. Available features: View analytics dashboards, Approve pending content, Respond to leads, Check MyRepChat messages, Quick social media posting. Push notifications keep you updated on important activities. The app syncs automatically with your desktop account. Offline mode available for viewing cached reports. Touch ID and Face ID supported for secure login.', 'Mobile');

SELECT 'KNOWLEDGE_BASE created: ' || COUNT(*) AS status FROM KNOWLEDGE_BASE;

-- ============================================================================
-- STEP 8: VERIFY DATA GENERATION
-- ============================================================================

SELECT '═══════════════════════════════════════════════════════' AS divider;
SELECT 'DATA GENERATION COMPLETE' AS status;
SELECT '═══════════════════════════════════════════════════════' AS divider;

SELECT 
    'CUSTOMERS' AS table_name, 
    COUNT(*) AS row_count,
    MIN(created_date) AS earliest_date,
    MAX(created_date) AS latest_date
FROM CUSTOMERS
UNION ALL
SELECT 'USERS', COUNT(*), NULL, NULL FROM USERS
UNION ALL
SELECT 'SUBSCRIPTIONS', COUNT(*), MIN(start_date), MAX(start_date) FROM SUBSCRIPTIONS
UNION ALL
SELECT 'CUSTOMER_FEEDBACK', COUNT(*), MIN(submitted_date), MAX(submitted_date) FROM CUSTOMER_FEEDBACK
UNION ALL
SELECT 'KNOWLEDGE_BASE', COUNT(*), NULL, NULL FROM KNOWLEDGE_BASE;

SELECT SUM(cnt) AS total_records FROM (
    SELECT COUNT(*) AS cnt FROM CUSTOMERS
    UNION ALL SELECT COUNT(*) FROM USERS
    UNION ALL SELECT COUNT(*) FROM SUBSCRIPTIONS
    UNION ALL SELECT COUNT(*) FROM CUSTOMER_FEEDBACK
    UNION ALL SELECT COUNT(*) FROM KNOWLEDGE_BASE
);

-- ============================================================================
-- STEP 9: CREATE THE DATA SHARE
-- ============================================================================

CREATE OR REPLACE SHARE FMG_LABS_SHARE
    COMMENT = 'FMG Sample Data (600k+ records) for Hands-On Labs';

-- Grant access to the database and schema
GRANT USAGE ON DATABASE FMG_SAMPLE_DATA TO SHARE FMG_LABS_SHARE;
GRANT USAGE ON SCHEMA FMG_SAMPLE_DATA.FMG TO SHARE FMG_LABS_SHARE;

-- Grant access to all tables
GRANT SELECT ON TABLE FMG_SAMPLE_DATA.FMG.CUSTOMERS TO SHARE FMG_LABS_SHARE;
GRANT SELECT ON TABLE FMG_SAMPLE_DATA.FMG.USERS TO SHARE FMG_LABS_SHARE;
GRANT SELECT ON TABLE FMG_SAMPLE_DATA.FMG.SUBSCRIPTIONS TO SHARE FMG_LABS_SHARE;
GRANT SELECT ON TABLE FMG_SAMPLE_DATA.FMG.CUSTOMER_FEEDBACK TO SHARE FMG_LABS_SHARE;
GRANT SELECT ON TABLE FMG_SAMPLE_DATA.FMG.KNOWLEDGE_BASE TO SHARE FMG_LABS_SHARE;

-- Verify the share
SHOW GRANTS TO SHARE FMG_LABS_SHARE;

-- Scale warehouse back down
ALTER WAREHOUSE FMG_SETUP_WH SET WAREHOUSE_SIZE = 'XSMALL';

-- ============================================================================
-- STEP 10: ADD CONSUMER ACCOUNT
-- ============================================================================
-- Replace XXXXXXX.YYYYYYY with the prospect's account locator
-- ALTER SHARE FMG_LABS_SHARE ADD ACCOUNTS = XXXXXXX.YYYYYYY;

-- To find the prospect's account locator, have them run:
-- SELECT CURRENT_ORGANIZATION_NAME() || '.' || CURRENT_ACCOUNT_NAME();

SELECT '✅ Data Provider Setup Complete!' AS STATUS;
SELECT 'Total records: 600,000+' AS DATA_SIZE;
SELECT 'Next: ALTER SHARE FMG_LABS_SHARE ADD ACCOUNTS = <prospect_account>;' AS NEXT_STEP;

/*
  PROSPECT SETUP INSTRUCTIONS:
  
  After the prospect accepts the share, they run:
  
  CREATE DATABASE FMG_SHARED_DATA FROM SHARE <your_org>.<your_account>.FMG_LABS_SHARE;
  
  Then they can run ANY lab in ANY order - they're all independent!
  
  DATA VOLUME:
  • CUSTOMERS:          100,000 records
  • USERS:              ~250,000 records  
  • SUBSCRIPTIONS:      ~200,000 records
  • CUSTOMER_FEEDBACK:  ~50,000 records
  • KNOWLEDGE_BASE:     10 records
  ─────────────────────────────────────
  TOTAL:                ~600,000 records
*/
