/*=============================================================================
  FMG SUITE - SNOWFLAKE HANDS-ON LABS
  Synthetic Data Setup - Internal Reporting Data
  
  Description: Generate realistic synthetic data for FMG's internal reporting
               including customers, subscriptions, revenue, usage, and support
  Prerequisites: Run 00_environment_setup.sql first
  Duration: ~10 minutes
=============================================================================*/

-- ============================================================================
-- STEP 1: Set Context
-- ============================================================================
USE ROLE FMG_ADMIN;
USE DATABASE FMG_PRODUCTION;
USE SCHEMA RAW;
USE WAREHOUSE FMG_DEV_XS;

-- ============================================================================
-- STEP 2: Create Customer/Account Tables
-- ============================================================================

-- CUSTOMERS: Firms that subscribe to FMG (RIAs, broker-dealers, enterprises)
CREATE OR REPLACE TABLE CUSTOMERS (
    customer_id VARCHAR(20) PRIMARY KEY,
    company_name VARCHAR(200),
    segment VARCHAR(50),        -- 'SMB', 'Mid-Market', 'Enterprise'
    industry VARCHAR(100),      -- 'RIA', 'Broker-Dealer', 'Bank/Credit Union', 'Insurance', 'Wirehouse'
    sub_industry VARCHAR(100),  -- 'Independent RIA', 'Large RIA', 'IBD', 'IMO/FMO', 'P&C', 'Life & Annuity'
    state VARCHAR(2),
    city VARCHAR(100),
    timezone VARCHAR(50),
    created_date DATE,
    acquisition_channel VARCHAR(50),  -- 'Direct', 'Partner', 'Referral', 'Marketing', 'Event'
    csm_owner VARCHAR(100),           -- Customer Success Manager
    sales_owner VARCHAR(100),
    account_status VARCHAR(20),       -- 'Active', 'Churned', 'Paused', 'Trial'
    is_strategic_account BOOLEAN,
    employee_count_band VARCHAR(20),  -- '1-5', '6-20', '21-50', '51-100', '100+'
    aum_band VARCHAR(50),             -- Assets Under Management band
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- USERS: Individual advisors/users within customer accounts
CREATE OR REPLACE TABLE USERS (
    user_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20) REFERENCES CUSTOMERS(customer_id),
    email VARCHAR(200),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    role VARCHAR(50),           -- 'Admin', 'Advisor', 'Staff', 'Compliance Officer'
    title VARCHAR(100),
    phone VARCHAR(20),
    created_date DATE,
    last_login_date TIMESTAMP_NTZ,
    login_count INTEGER,
    is_primary_contact BOOLEAN,
    user_status VARCHAR(20),    -- 'Active', 'Inactive', 'Suspended'
    email_verified BOOLEAN,
    mfa_enabled BOOLEAN,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- STEP 3: Create Subscription & Revenue Tables
-- ============================================================================

-- SUBSCRIPTIONS: Product subscriptions for each customer
CREATE OR REPLACE TABLE SUBSCRIPTIONS (
    subscription_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20) REFERENCES CUSTOMERS(customer_id),
    product_name VARCHAR(100),      -- 'Marketing Suite', 'Website Pro', 'MyRepChat', 'Do It For Me'
    plan_tier VARCHAR(50),          -- 'Starter', 'Professional', 'Enterprise', 'Custom'
    billing_frequency VARCHAR(20),  -- 'Monthly', 'Annual'
    start_date DATE,
    end_date DATE,
    renewal_date DATE,
    status VARCHAR(20),             -- 'Active', 'Cancelled', 'Pending', 'Expired'
    mrr_amount DECIMAL(10,2),       -- Monthly Recurring Revenue
    arr_amount DECIMAL(12,2),       -- Annual Recurring Revenue
    discount_percent DECIMAL(5,2),
    contract_term_months INTEGER,
    auto_renew BOOLEAN,
    cancellation_reason VARCHAR(200),
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- INVOICES: Billing records
CREATE OR REPLACE TABLE INVOICES (
    invoice_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20) REFERENCES CUSTOMERS(customer_id),
    subscription_id VARCHAR(20) REFERENCES SUBSCRIPTIONS(subscription_id),
    invoice_date DATE,
    due_date DATE,
    paid_date DATE,
    amount DECIMAL(10,2),
    tax_amount DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    status VARCHAR(20),         -- 'Paid', 'Pending', 'Overdue', 'Void'
    payment_method VARCHAR(50), -- 'Credit Card', 'ACH', 'Wire', 'Check'
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- REVENUE_DAILY: Daily revenue metrics for trending
CREATE OR REPLACE TABLE REVENUE_DAILY (
    date_key DATE,
    customer_id VARCHAR(20),
    product_name VARCHAR(100),
    mrr DECIMAL(10,2),
    arr DECIMAL(12,2),
    is_new_revenue BOOLEAN,
    is_expansion BOOLEAN,
    is_contraction BOOLEAN,
    is_churned BOOLEAN,
    PRIMARY KEY (date_key, customer_id, product_name)
);

-- ============================================================================
-- STEP 4: Create Platform Usage Tables
-- ============================================================================

-- PLATFORM_USAGE_DAILY: Daily aggregated usage metrics per customer
CREATE OR REPLACE TABLE PLATFORM_USAGE_DAILY (
    usage_date DATE,
    customer_id VARCHAR(20),
    user_id VARCHAR(20),
    -- Email Marketing Usage
    emails_sent INTEGER DEFAULT 0,
    emails_opened INTEGER DEFAULT 0,
    emails_clicked INTEGER DEFAULT 0,
    email_templates_used INTEGER DEFAULT 0,
    -- Social Media Usage
    social_posts_created INTEGER DEFAULT 0,
    social_posts_published INTEGER DEFAULT 0,
    social_accounts_connected INTEGER DEFAULT 0,
    -- Website Usage
    website_page_views INTEGER DEFAULT 0,
    website_leads_generated INTEGER DEFAULT 0,
    blog_posts_published INTEGER DEFAULT 0,
    -- MyRepChat Usage
    myrepchat_messages_sent INTEGER DEFAULT 0,
    myrepchat_messages_received INTEGER DEFAULT 0,
    myrepchat_templates_used INTEGER DEFAULT 0,
    -- Events & Cards
    events_created INTEGER DEFAULT 0,
    greeting_cards_sent INTEGER DEFAULT 0,
    -- General Platform
    total_logins INTEGER DEFAULT 0,
    session_duration_minutes INTEGER DEFAULT 0,
    features_used INTEGER DEFAULT 0,
    PRIMARY KEY (usage_date, customer_id, user_id)
);

-- FEATURE_ADOPTION: Tracks which features each customer has used
CREATE OR REPLACE TABLE FEATURE_ADOPTION (
    customer_id VARCHAR(20),
    feature_name VARCHAR(100),
    first_used_date DATE,
    last_used_date DATE,
    usage_count INTEGER,
    adoption_status VARCHAR(20),  -- 'Not Started', 'Exploring', 'Adopted', 'Power User'
    PRIMARY KEY (customer_id, feature_name)
);

-- CONTENT_USAGE: How FMG's content library is being used
CREATE OR REPLACE TABLE CONTENT_USAGE (
    content_id VARCHAR(20),
    customer_id VARCHAR(20),
    user_id VARCHAR(20),
    content_type VARCHAR(50),     -- 'Email Template', 'Social Post', 'Article', 'Infographic', 'Video'
    content_category VARCHAR(100), -- 'Market Commentary', 'Retirement', 'Tax Planning', 'Estate', 'Insurance'
    content_title VARCHAR(300),
    action_type VARCHAR(20),       -- 'View', 'Use', 'Customize', 'Share'
    action_timestamp TIMESTAMP_NTZ,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- STEP 5: Create Customer Success Tables
-- ============================================================================

-- CUSTOMER_HEALTH_SCORES: Weekly health score snapshots
CREATE OR REPLACE TABLE CUSTOMER_HEALTH_SCORES (
    snapshot_date DATE,
    customer_id VARCHAR(20),
    overall_health_score INTEGER,    -- 0-100
    usage_score INTEGER,              -- Component: platform usage
    engagement_score INTEGER,         -- Component: logins, feature adoption
    support_score INTEGER,            -- Component: ticket volume, satisfaction
    payment_score INTEGER,            -- Component: payment history
    expansion_score INTEGER,          -- Component: growth signals
    churn_risk VARCHAR(20),           -- 'Low', 'Medium', 'High', 'Critical'
    health_trend VARCHAR(20),         -- 'Improving', 'Stable', 'Declining'
    PRIMARY KEY (snapshot_date, customer_id)
);

-- NPS_RESPONSES: Net Promoter Score survey responses
CREATE OR REPLACE TABLE NPS_RESPONSES (
    response_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20),
    user_id VARCHAR(20),
    survey_date DATE,
    nps_score INTEGER,            -- 0-10
    nps_category VARCHAR(20),     -- 'Detractor' (0-6), 'Passive' (7-8), 'Promoter' (9-10)
    feedback_text VARCHAR(2000),
    product_mentioned VARCHAR(100),
    follow_up_requested BOOLEAN,
    follow_up_completed BOOLEAN,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- SUPPORT_TICKETS: Customer support interactions
CREATE OR REPLACE TABLE SUPPORT_TICKETS (
    ticket_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20),
    user_id VARCHAR(20),
    created_date TIMESTAMP_NTZ,
    resolved_date TIMESTAMP_NTZ,
    category VARCHAR(50),          -- 'Technical', 'Billing', 'Feature Request', 'Training', 'Compliance'
    subcategory VARCHAR(100),
    priority VARCHAR(20),          -- 'Low', 'Medium', 'High', 'Urgent'
    status VARCHAR(20),            -- 'Open', 'In Progress', 'Waiting on Customer', 'Resolved', 'Closed'
    channel VARCHAR(30),           -- 'Email', 'Phone', 'Chat', 'Self-Service'
    assigned_agent VARCHAR(100),
    resolution_time_hours DECIMAL(10,2),
    first_response_time_minutes INTEGER,
    csat_score INTEGER,            -- 1-5 satisfaction rating
    sla_met BOOLEAN,
    ticket_summary VARCHAR(500),
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- STEP 6: Create Sales Pipeline Tables
-- ============================================================================

-- SALES_LEADS: Inbound leads for FMG sales team
CREATE OR REPLACE TABLE SALES_LEADS (
    lead_id VARCHAR(20) PRIMARY KEY,
    company_name VARCHAR(200),
    contact_name VARCHAR(100),
    contact_email VARCHAR(200),
    contact_phone VARCHAR(20),
    lead_source VARCHAR(50),       -- 'Website', 'Webinar', 'Referral', 'Partner', 'Event', 'Content Download'
    lead_source_detail VARCHAR(200),
    industry VARCHAR(100),
    company_size VARCHAR(20),
    created_date TIMESTAMP_NTZ,
    assigned_sdr VARCHAR(100),
    lead_status VARCHAR(30),       -- 'New', 'Contacted', 'Qualified', 'Unqualified', 'Converted'
    mql_date DATE,                 -- Marketing Qualified Lead date
    sql_date DATE,                 -- Sales Qualified Lead date
    conversion_date DATE,
    converted_customer_id VARCHAR(20),
    utm_source VARCHAR(100),
    utm_medium VARCHAR(100),
    utm_campaign VARCHAR(200),
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- SALES_OPPORTUNITIES: Sales pipeline opportunities
CREATE OR REPLACE TABLE SALES_OPPORTUNITIES (
    opportunity_id VARCHAR(20) PRIMARY KEY,
    lead_id VARCHAR(20),
    customer_id VARCHAR(20),       -- NULL for new business, populated for upsells
    opportunity_name VARCHAR(200),
    opportunity_type VARCHAR(30),  -- 'New Business', 'Upsell', 'Cross-sell', 'Renewal'
    stage VARCHAR(50),             -- 'Discovery', 'Demo', 'Proposal', 'Negotiation', 'Closed Won', 'Closed Lost'
    probability INTEGER,
    amount DECIMAL(12,2),
    arr_value DECIMAL(12,2),
    created_date DATE,
    close_date DATE,
    actual_close_date DATE,
    owner VARCHAR(100),
    products_interested VARCHAR(500),  -- Comma-separated product list
    competitor VARCHAR(100),
    loss_reason VARCHAR(200),
    win_reason VARCHAR(200),
    days_in_pipeline INTEGER,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- STEP 7: Generate Synthetic Data
-- ============================================================================

-- Helper: Generate random data using sequences and arrays
CREATE OR REPLACE SEQUENCE customer_seq START = 1000;
CREATE OR REPLACE SEQUENCE user_seq START = 5000;
CREATE OR REPLACE SEQUENCE subscription_seq START = 2000;

-- Generate Customers (500 customer accounts)
INSERT INTO CUSTOMERS
WITH 
segments AS (SELECT * FROM (VALUES ('SMB'), ('Mid-Market'), ('Enterprise')) AS t(segment)),
industries AS (SELECT * FROM (VALUES 
    ('RIA', 'Independent RIA'), ('RIA', 'Large RIA'), 
    ('Broker-Dealer', 'Independent Broker-Dealer'), ('Broker-Dealer', 'Regional BD'),
    ('Bank/Credit Union', 'Community Bank'), ('Bank/Credit Union', 'Credit Union'),
    ('Insurance', 'Life & Annuity'), ('Insurance', 'P&C'), ('Insurance', 'IMO/FMO'),
    ('Wirehouse', 'National Wirehouse')
) AS t(industry, sub_industry)),
states AS (SELECT * FROM (VALUES 
    ('CA'), ('TX'), ('FL'), ('NY'), ('IL'), ('PA'), ('OH'), ('GA'), ('NC'), ('MI'),
    ('NJ'), ('VA'), ('WA'), ('AZ'), ('MA'), ('TN'), ('IN'), ('MO'), ('MD'), ('CO')
) AS t(state)),
channels AS (SELECT * FROM (VALUES ('Direct'), ('Partner'), ('Referral'), ('Marketing'), ('Event'), ('Webinar')) AS t(channel)),
csms AS (SELECT * FROM (VALUES 
    ('Sarah Mitchell'), ('James Chen'), ('Emily Rodriguez'), ('Michael Thompson'), ('Lisa Park'),
    ('David Kumar'), ('Jennifer Walsh'), ('Robert Garcia'), ('Amanda Foster'), ('Chris Martinez')
) AS t(csm)),
sales_owners AS (SELECT * FROM (VALUES 
    ('Tom Brady'), ('Jessica Williams'), ('Marcus Johnson'), ('Rachel Kim'), ('Andrew Scott'),
    ('Stephanie Lee'), ('Brandon Davis'), ('Michelle Taylor'), ('Kevin Brown'), ('Nicole Adams')
) AS t(sales_owner)),
firm_prefixes AS (SELECT * FROM (VALUES 
    ('Pinnacle'), ('Summit'), ('Heritage'), ('Legacy'), ('Cornerstone'), ('Beacon'), ('Horizon'),
    ('Sterling'), ('Meridian'), ('Capstone'), ('Evergreen'), ('Vanguard'), ('Premier'), ('Elite'),
    ('Pacific'), ('Atlantic'), ('Mountain'), ('Valley'), ('Oak'), ('Maple'), ('Cedar'), ('Pine'),
    ('Golden'), ('Silver'), ('Diamond'), ('Sapphire'), ('Emerald'), ('Crystal'), ('Royal'), ('Noble')
) AS t(prefix)),
firm_suffixes AS (SELECT * FROM (VALUES 
    ('Financial Advisors'), ('Wealth Management'), ('Financial Group'), ('Advisory Services'),
    ('Capital Partners'), ('Investment Advisors'), ('Financial Planning'), ('Wealth Partners'),
    ('Asset Management'), ('Financial Services'), ('Advisory Group'), ('Wealth Advisors')
) AS t(suffix))
SELECT
    'CUST-' || LPAD(customer_seq.NEXTVAL::VARCHAR, 6, '0') AS customer_id,
    fp.prefix || ' ' || fs.suffix AS company_name,
    s.segment,
    i.industry,
    i.sub_industry,
    st.state,
    CASE st.state 
        WHEN 'CA' THEN (SELECT * FROM (VALUES ('San Francisco'), ('Los Angeles'), ('San Diego'), ('Sacramento')) ORDER BY RANDOM() LIMIT 1)
        WHEN 'TX' THEN (SELECT * FROM (VALUES ('Houston'), ('Dallas'), ('Austin'), ('San Antonio')) ORDER BY RANDOM() LIMIT 1)
        WHEN 'FL' THEN (SELECT * FROM (VALUES ('Miami'), ('Tampa'), ('Orlando'), ('Jacksonville')) ORDER BY RANDOM() LIMIT 1)
        WHEN 'NY' THEN (SELECT * FROM (VALUES ('New York'), ('Buffalo'), ('Albany'), ('Rochester')) ORDER BY RANDOM() LIMIT 1)
        ELSE 'Metro Area'
    END AS city,
    CASE 
        WHEN st.state IN ('CA', 'WA') THEN 'America/Los_Angeles'
        WHEN st.state IN ('TX', 'IL', 'MO', 'TN') THEN 'America/Chicago'
        WHEN st.state IN ('NY', 'FL', 'GA', 'NC', 'PA', 'OH', 'MI', 'NJ', 'VA', 'MA', 'IN', 'MD') THEN 'America/New_York'
        WHEN st.state IN ('AZ', 'CO') THEN 'America/Denver'
        ELSE 'America/New_York'
    END AS timezone,
    DATEADD('day', -UNIFORM(30, 2000, RANDOM()), CURRENT_DATE()) AS created_date,
    ch.channel AS acquisition_channel,
    csm.csm AS csm_owner,
    so.sales_owner,
    CASE 
        WHEN RANDOM() < 0.85 THEN 'Active'
        WHEN RANDOM() < 0.92 THEN 'Churned'
        WHEN RANDOM() < 0.97 THEN 'Paused'
        ELSE 'Trial'
    END AS account_status,
    RANDOM() < 0.15 AS is_strategic_account,
    CASE s.segment
        WHEN 'SMB' THEN (SELECT * FROM (VALUES ('1-5'), ('6-20')) ORDER BY RANDOM() LIMIT 1)
        WHEN 'Mid-Market' THEN (SELECT * FROM (VALUES ('21-50'), ('51-100')) ORDER BY RANDOM() LIMIT 1)
        ELSE '100+'
    END AS employee_count_band,
    CASE s.segment
        WHEN 'SMB' THEN (SELECT * FROM (VALUES ('$0-50M'), ('$50-100M')) ORDER BY RANDOM() LIMIT 1)
        WHEN 'Mid-Market' THEN (SELECT * FROM (VALUES ('$100-500M'), ('$500M-1B')) ORDER BY RANDOM() LIMIT 1)
        ELSE (SELECT * FROM (VALUES ('$1-5B'), ('$5B+')) ORDER BY RANDOM() LIMIT 1)
    END AS aum_band,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM 
    (SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) AS rn FROM TABLE(GENERATOR(ROWCOUNT => 500))) gen
    CROSS JOIN LATERAL (SELECT segment FROM segments ORDER BY RANDOM() LIMIT 1) s
    CROSS JOIN LATERAL (SELECT industry, sub_industry FROM industries ORDER BY RANDOM() LIMIT 1) i
    CROSS JOIN LATERAL (SELECT state FROM states ORDER BY RANDOM() LIMIT 1) st
    CROSS JOIN LATERAL (SELECT channel FROM channels ORDER BY RANDOM() LIMIT 1) ch
    CROSS JOIN LATERAL (SELECT csm FROM csms ORDER BY RANDOM() LIMIT 1) csm
    CROSS JOIN LATERAL (SELECT sales_owner FROM sales_owners ORDER BY RANDOM() LIMIT 1) so
    CROSS JOIN LATERAL (SELECT prefix FROM firm_prefixes ORDER BY RANDOM() LIMIT 1) fp
    CROSS JOIN LATERAL (SELECT suffix FROM firm_suffixes ORDER BY RANDOM() LIMIT 1) fs;

-- Generate Users (3-10 users per customer, ~2500 total)
INSERT INTO USERS
WITH 
first_names AS (SELECT * FROM (VALUES 
    ('James'), ('Mary'), ('John'), ('Patricia'), ('Robert'), ('Jennifer'), ('Michael'), ('Linda'),
    ('William'), ('Elizabeth'), ('David'), ('Barbara'), ('Richard'), ('Susan'), ('Joseph'), ('Jessica'),
    ('Thomas'), ('Sarah'), ('Charles'), ('Karen'), ('Christopher'), ('Nancy'), ('Daniel'), ('Lisa'),
    ('Matthew'), ('Betty'), ('Anthony'), ('Margaret'), ('Mark'), ('Sandra'), ('Donald'), ('Ashley'),
    ('Steven'), ('Kimberly'), ('Paul'), ('Emily'), ('Andrew'), ('Donna'), ('Joshua'), ('Michelle')
) AS t(fname)),
last_names AS (SELECT * FROM (VALUES 
    ('Smith'), ('Johnson'), ('Williams'), ('Brown'), ('Jones'), ('Garcia'), ('Miller'), ('Davis'),
    ('Rodriguez'), ('Martinez'), ('Hernandez'), ('Lopez'), ('Gonzalez'), ('Wilson'), ('Anderson'),
    ('Thomas'), ('Taylor'), ('Moore'), ('Jackson'), ('Martin'), ('Lee'), ('Perez'), ('Thompson'),
    ('White'), ('Harris'), ('Sanchez'), ('Clark'), ('Ramirez'), ('Lewis'), ('Robinson')
) AS t(lname)),
roles AS (SELECT * FROM (VALUES ('Admin', 0.15), ('Advisor', 0.60), ('Staff', 0.20), ('Compliance Officer', 0.05)) AS t(role, weight)),
titles AS (SELECT * FROM (VALUES 
    ('Financial Advisor'), ('Senior Advisor'), ('Wealth Manager'), ('Client Relationship Manager'),
    ('Practice Manager'), ('Operations Manager'), ('Administrative Assistant'), ('Compliance Manager'),
    ('Partner'), ('Managing Director'), ('Vice President'), ('Associate Advisor')
) AS t(title))
SELECT
    'USER-' || LPAD(user_seq.NEXTVAL::VARCHAR, 6, '0') AS user_id,
    c.customer_id,
    LOWER(fn.fname) || '.' || LOWER(ln.lname) || '@' || 
        LOWER(REPLACE(REPLACE(c.company_name, ' ', ''), '''', '')) || '.com' AS email,
    fn.fname AS first_name,
    ln.lname AS last_name,
    r.role,
    t.title,
    '(' || LPAD(UNIFORM(200, 999, RANDOM())::VARCHAR, 3, '0') || ') ' ||
        LPAD(UNIFORM(200, 999, RANDOM())::VARCHAR, 3, '0') || '-' ||
        LPAD(UNIFORM(1000, 9999, RANDOM())::VARCHAR, 4, '0') AS phone,
    DATEADD('day', UNIFORM(0, 365, RANDOM()), c.created_date) AS created_date,
    CASE WHEN c.account_status = 'Active' 
        THEN DATEADD('hour', -UNIFORM(1, 720, RANDOM()), CURRENT_TIMESTAMP()) 
        ELSE DATEADD('day', -UNIFORM(30, 180, RANDOM()), CURRENT_TIMESTAMP()) 
    END AS last_login_date,
    UNIFORM(5, 500, RANDOM()) AS login_count,
    user_num = 1 AS is_primary_contact,
    CASE 
        WHEN c.account_status = 'Active' THEN (CASE WHEN RANDOM() < 0.92 THEN 'Active' ELSE 'Inactive' END)
        ELSE 'Inactive'
    END AS user_status,
    RANDOM() < 0.95 AS email_verified,
    RANDOM() < 0.70 AS mfa_enabled,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM 
    CUSTOMERS c
    CROSS JOIN LATERAL (
        SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) AS user_num 
        FROM TABLE(GENERATOR(ROWCOUNT => 10))
        LIMIT CASE c.segment 
            WHEN 'SMB' THEN UNIFORM(2, 5, RANDOM())
            WHEN 'Mid-Market' THEN UNIFORM(4, 8, RANDOM())
            ELSE UNIFORM(6, 15, RANDOM())
        END
    ) nums
    CROSS JOIN LATERAL (SELECT fname FROM first_names ORDER BY RANDOM() LIMIT 1) fn
    CROSS JOIN LATERAL (SELECT lname FROM last_names ORDER BY RANDOM() LIMIT 1) ln
    CROSS JOIN LATERAL (SELECT role FROM roles ORDER BY RANDOM() LIMIT 1) r
    CROSS JOIN LATERAL (SELECT title FROM titles ORDER BY RANDOM() LIMIT 1) t;

-- Generate Subscriptions
INSERT INTO SUBSCRIPTIONS
WITH 
products AS (SELECT * FROM (VALUES 
    ('Marketing Suite', 'Starter', 149, 0.30),
    ('Marketing Suite', 'Professional', 299, 0.45),
    ('Marketing Suite', 'Enterprise', 599, 0.20),
    ('Website Pro', 'Starter', 99, 0.25),
    ('Website Pro', 'Professional', 199, 0.50),
    ('Website Pro', 'Enterprise', 399, 0.20),
    ('MyRepChat', 'Starter', 49, 0.35),
    ('MyRepChat', 'Professional', 99, 0.45),
    ('MyRepChat', 'Enterprise', 199, 0.15),
    ('Do It For Me', 'Standard', 499, 0.40),
    ('Do It For Me', 'Premium', 899, 0.35),
    ('Do It For Me', 'Elite', 1499, 0.15)
) AS t(product_name, plan_tier, base_price, weight))
SELECT
    'SUB-' || LPAD(subscription_seq.NEXTVAL::VARCHAR, 6, '0') AS subscription_id,
    c.customer_id,
    p.product_name,
    p.plan_tier,
    CASE WHEN RANDOM() < 0.70 THEN 'Annual' ELSE 'Monthly' END AS billing_frequency,
    DATEADD('day', UNIFORM(0, 90, RANDOM()), c.created_date) AS start_date,
    CASE WHEN c.account_status = 'Churned' 
        THEN DATEADD('month', UNIFORM(3, 24, RANDOM()), DATEADD('day', UNIFORM(0, 90, RANDOM()), c.created_date))
        ELSE NULL 
    END AS end_date,
    DATEADD('year', 1, DATEADD('day', UNIFORM(0, 90, RANDOM()), c.created_date)) AS renewal_date,
    CASE c.account_status
        WHEN 'Active' THEN 'Active'
        WHEN 'Churned' THEN 'Cancelled'
        WHEN 'Paused' THEN 'Pending'
        ELSE 'Active'
    END AS status,
    p.base_price * (1 - COALESCE(CASE WHEN c.segment = 'Enterprise' THEN UNIFORM(0.10, 0.25, RANDOM()) ELSE 0 END, 0)) AS mrr_amount,
    p.base_price * 12 * (1 - COALESCE(CASE WHEN c.segment = 'Enterprise' THEN UNIFORM(0.10, 0.25, RANDOM()) ELSE 0 END, 0)) AS arr_amount,
    CASE WHEN c.segment = 'Enterprise' THEN UNIFORM(10, 25, RANDOM()) ELSE 0 END AS discount_percent,
    CASE WHEN RANDOM() < 0.70 THEN 12 ELSE 24 END AS contract_term_months,
    RANDOM() < 0.80 AS auto_renew,
    CASE WHEN c.account_status = 'Churned' 
        THEN (SELECT * FROM (VALUES 
            ('Switched to competitor'), ('Budget constraints'), ('Not using the product'),
            ('Missing features'), ('Poor support experience'), ('Company closed')
        ) ORDER BY RANDOM() LIMIT 1)
        ELSE NULL 
    END AS cancellation_reason,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM 
    CUSTOMERS c
    CROSS JOIN LATERAL (
        SELECT product_name, plan_tier, base_price 
        FROM products 
        WHERE RANDOM() < weight
        ORDER BY RANDOM() 
        LIMIT CASE c.segment WHEN 'Enterprise' THEN 4 WHEN 'Mid-Market' THEN 3 ELSE 2 END
    ) p;

-- Generate Platform Usage Daily (last 90 days)
INSERT INTO PLATFORM_USAGE_DAILY
SELECT
    d.date_val AS usage_date,
    u.customer_id,
    u.user_id,
    -- Email Marketing (varies by day of week - more on Tuesdays/Thursdays)
    CASE WHEN DAYOFWEEK(d.date_val) IN (3, 5) THEN UNIFORM(0, 50, RANDOM()) ELSE UNIFORM(0, 20, RANDOM()) END AS emails_sent,
    UNIFORM(0, 30, RANDOM()) AS emails_opened,
    UNIFORM(0, 10, RANDOM()) AS emails_clicked,
    UNIFORM(0, 5, RANDOM()) AS email_templates_used,
    -- Social Media (varies by day - less on weekends)
    CASE WHEN DAYOFWEEK(d.date_val) IN (1, 7) THEN 0 ELSE UNIFORM(0, 10, RANDOM()) END AS social_posts_created,
    CASE WHEN DAYOFWEEK(d.date_val) IN (1, 7) THEN 0 ELSE UNIFORM(0, 8, RANDOM()) END AS social_posts_published,
    UNIFORM(1, 5, RANDOM()) AS social_accounts_connected,
    -- Website
    UNIFORM(10, 500, RANDOM()) AS website_page_views,
    UNIFORM(0, 5, RANDOM()) AS website_leads_generated,
    UNIFORM(0, 2, RANDOM()) AS blog_posts_published,
    -- MyRepChat
    UNIFORM(0, 30, RANDOM()) AS myrepchat_messages_sent,
    UNIFORM(0, 25, RANDOM()) AS myrepchat_messages_received,
    UNIFORM(0, 5, RANDOM()) AS myrepchat_templates_used,
    -- Events & Cards
    UNIFORM(0, 2, RANDOM()) AS events_created,
    UNIFORM(0, 10, RANDOM()) AS greeting_cards_sent,
    -- General Platform
    UNIFORM(1, 10, RANDOM()) AS total_logins,
    UNIFORM(5, 120, RANDOM()) AS session_duration_minutes,
    UNIFORM(3, 15, RANDOM()) AS features_used
FROM 
    USERS u
    CROSS JOIN (
        SELECT DATEADD('day', -seq4(), CURRENT_DATE()) AS date_val 
        FROM TABLE(GENERATOR(ROWCOUNT => 90))
    ) d
WHERE 
    u.user_status = 'Active'
    AND RANDOM() < 0.7;  -- Not every user is active every day

-- Generate Customer Health Scores (weekly snapshots for last 12 weeks)
INSERT INTO CUSTOMER_HEALTH_SCORES
SELECT
    DATEADD('week', -w.week_num, DATE_TRUNC('week', CURRENT_DATE())) AS snapshot_date,
    c.customer_id,
    -- Overall health score (weighted average of components)
    ROUND(
        (usage.usage_score * 0.25) + 
        (engagement.engagement_score * 0.20) + 
        (support.support_score * 0.20) + 
        (payment.payment_score * 0.20) + 
        (expansion.expansion_score * 0.15)
    ) AS overall_health_score,
    usage.usage_score,
    engagement.engagement_score,
    support.support_score,
    payment.payment_score,
    expansion.expansion_score,
    CASE 
        WHEN ROUND((usage.usage_score * 0.25) + (engagement.engagement_score * 0.20) + (support.support_score * 0.20) + (payment.payment_score * 0.20) + (expansion.expansion_score * 0.15)) >= 80 THEN 'Low'
        WHEN ROUND((usage.usage_score * 0.25) + (engagement.engagement_score * 0.20) + (support.support_score * 0.20) + (payment.payment_score * 0.20) + (expansion.expansion_score * 0.15)) >= 60 THEN 'Medium'
        WHEN ROUND((usage.usage_score * 0.25) + (engagement.engagement_score * 0.20) + (support.support_score * 0.20) + (payment.payment_score * 0.20) + (expansion.expansion_score * 0.15)) >= 40 THEN 'High'
        ELSE 'Critical'
    END AS churn_risk,
    CASE 
        WHEN RANDOM() < 0.33 THEN 'Improving'
        WHEN RANDOM() < 0.66 THEN 'Stable'
        ELSE 'Declining'
    END AS health_trend
FROM 
    CUSTOMERS c
    CROSS JOIN (SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS week_num FROM TABLE(GENERATOR(ROWCOUNT => 12))) w
    CROSS JOIN LATERAL (SELECT UNIFORM(40, 100, RANDOM()) AS usage_score) usage
    CROSS JOIN LATERAL (SELECT UNIFORM(30, 100, RANDOM()) AS engagement_score) engagement
    CROSS JOIN LATERAL (SELECT UNIFORM(50, 100, RANDOM()) AS support_score) support
    CROSS JOIN LATERAL (SELECT UNIFORM(60, 100, RANDOM()) AS payment_score) payment
    CROSS JOIN LATERAL (SELECT UNIFORM(20, 100, RANDOM()) AS expansion_score) expansion
WHERE 
    c.account_status IN ('Active', 'Paused');

-- Generate NPS Responses
INSERT INTO NPS_RESPONSES
WITH products AS (SELECT * FROM (VALUES ('Marketing Suite'), ('Website Pro'), ('MyRepChat'), ('Do It For Me'), ('Overall Platform')) AS t(product))
SELECT
    'NPS-' || LPAD(ROW_NUMBER() OVER (ORDER BY RANDOM())::VARCHAR, 6, '0') AS response_id,
    u.customer_id,
    u.user_id,
    DATEADD('day', -UNIFORM(0, 365, RANDOM()), CURRENT_DATE()) AS survey_date,
    UNIFORM(0, 10, RANDOM()) AS nps_score,
    CASE 
        WHEN UNIFORM(0, 10, RANDOM()) <= 6 THEN 'Detractor'
        WHEN UNIFORM(0, 10, RANDOM()) <= 8 THEN 'Passive'
        ELSE 'Promoter'
    END AS nps_category,
    CASE 
        WHEN RANDOM() < 0.4 THEN NULL
        WHEN RANDOM() < 0.6 THEN 'Great product, love the templates!'
        WHEN RANDOM() < 0.7 THEN 'Support could be faster but overall good experience.'
        WHEN RANDOM() < 0.8 THEN 'Would love more customization options.'
        WHEN RANDOM() < 0.9 THEN 'The platform saves me so much time every week.'
        ELSE 'Compliance features are essential for our practice.'
    END AS feedback_text,
    p.product AS product_mentioned,
    RANDOM() < 0.2 AS follow_up_requested,
    RANDOM() < 0.8 AS follow_up_completed,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM 
    USERS u
    CROSS JOIN LATERAL (SELECT product FROM products ORDER BY RANDOM() LIMIT 1) p
WHERE 
    RANDOM() < 0.3  -- Not all users respond to NPS surveys
    AND u.is_primary_contact = TRUE;

-- Generate Support Tickets
INSERT INTO SUPPORT_TICKETS
WITH 
categories AS (SELECT * FROM (VALUES 
    ('Technical', 'Login Issues', 0.15),
    ('Technical', 'Email Delivery', 0.12),
    ('Technical', 'Integration Problems', 0.10),
    ('Technical', 'Performance Issues', 0.08),
    ('Billing', 'Invoice Question', 0.10),
    ('Billing', 'Refund Request', 0.05),
    ('Billing', 'Upgrade/Downgrade', 0.08),
    ('Feature Request', 'New Feature', 0.07),
    ('Feature Request', 'Enhancement', 0.05),
    ('Training', 'How-To Question', 0.10),
    ('Training', 'Best Practices', 0.05),
    ('Compliance', 'Content Review', 0.03),
    ('Compliance', 'Archiving Question', 0.02)
) AS t(category, subcategory, weight)),
agents AS (SELECT * FROM (VALUES 
    ('Alex Thompson'), ('Jordan Rivera'), ('Casey Morgan'), ('Taylor Kim'), ('Morgan Chen'),
    ('Riley Johnson'), ('Jamie Lee'), ('Drew Martinez'), ('Quinn Wilson'), ('Avery Davis')
) AS t(agent))
SELECT
    'TKT-' || LPAD(ROW_NUMBER() OVER (ORDER BY RANDOM())::VARCHAR, 6, '0') AS ticket_id,
    u.customer_id,
    u.user_id,
    DATEADD('hour', -UNIFORM(1, 8760, RANDOM()), CURRENT_TIMESTAMP()) AS created_date,
    CASE WHEN RANDOM() < 0.85 
        THEN DATEADD('hour', UNIFORM(1, 72, RANDOM()), DATEADD('hour', -UNIFORM(1, 8760, RANDOM()), CURRENT_TIMESTAMP()))
        ELSE NULL 
    END AS resolved_date,
    cat.category,
    cat.subcategory,
    CASE 
        WHEN RANDOM() < 0.5 THEN 'Medium'
        WHEN RANDOM() < 0.8 THEN 'Low'
        WHEN RANDOM() < 0.95 THEN 'High'
        ELSE 'Urgent'
    END AS priority,
    CASE 
        WHEN RANDOM() < 0.70 THEN 'Resolved'
        WHEN RANDOM() < 0.80 THEN 'Closed'
        WHEN RANDOM() < 0.90 THEN 'In Progress'
        WHEN RANDOM() < 0.95 THEN 'Waiting on Customer'
        ELSE 'Open'
    END AS status,
    CASE 
        WHEN RANDOM() < 0.4 THEN 'Email'
        WHEN RANDOM() < 0.7 THEN 'Chat'
        WHEN RANDOM() < 0.9 THEN 'Phone'
        ELSE 'Self-Service'
    END AS channel,
    a.agent AS assigned_agent,
    UNIFORM(0.5, 72, RANDOM()) AS resolution_time_hours,
    UNIFORM(5, 480, RANDOM()) AS first_response_time_minutes,
    UNIFORM(1, 5, RANDOM()) AS csat_score,
    RANDOM() < 0.92 AS sla_met,
    'Customer inquiry regarding ' || cat.subcategory AS ticket_summary,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM 
    USERS u
    CROSS JOIN LATERAL (SELECT category, subcategory FROM categories WHERE RANDOM() < weight ORDER BY RANDOM() LIMIT 1) cat
    CROSS JOIN LATERAL (SELECT agent FROM agents ORDER BY RANDOM() LIMIT 1) a
WHERE 
    RANDOM() < 0.4;  -- Generate tickets for ~40% of users

-- Generate Sales Leads
INSERT INTO SALES_LEADS
WITH 
sources AS (SELECT * FROM (VALUES 
    ('Website', 'Contact Form', 0.25),
    ('Website', 'Demo Request', 0.20),
    ('Webinar', '2025 Marketing Trends', 0.15),
    ('Webinar', 'Compliance Best Practices', 0.10),
    ('Referral', 'Customer Referral', 0.10),
    ('Partner', 'Broker-Dealer Partner', 0.08),
    ('Event', 'Industry Conference', 0.05),
    ('Content Download', '2025 Marketing Guide', 0.07)
) AS t(source, detail, weight)),
sdrs AS (SELECT * FROM (VALUES 
    ('Mike Chen'), ('Sarah Park'), ('Jason Williams'), ('Emily Davis'), ('Chris Martinez')
) AS t(sdr))
SELECT
    'LEAD-' || LPAD(ROW_NUMBER() OVER (ORDER BY RANDOM())::VARCHAR, 6, '0') AS lead_id,
    fp.prefix || ' ' || fs.suffix AS company_name,
    fn.fname || ' ' || ln.lname AS contact_name,
    LOWER(fn.fname) || '.' || LOWER(ln.lname) || '@example.com' AS contact_email,
    '(' || LPAD(UNIFORM(200, 999, RANDOM())::VARCHAR, 3, '0') || ') ' ||
        LPAD(UNIFORM(200, 999, RANDOM())::VARCHAR, 3, '0') || '-' ||
        LPAD(UNIFORM(1000, 9999, RANDOM())::VARCHAR, 4, '0') AS contact_phone,
    s.source AS lead_source,
    s.detail AS lead_source_detail,
    ind.industry,
    sz.size AS company_size,
    DATEADD('day', -UNIFORM(0, 365, RANDOM()), CURRENT_TIMESTAMP()) AS created_date,
    sdr.sdr AS assigned_sdr,
    CASE 
        WHEN RANDOM() < 0.20 THEN 'New'
        WHEN RANDOM() < 0.40 THEN 'Contacted'
        WHEN RANDOM() < 0.70 THEN 'Qualified'
        WHEN RANDOM() < 0.85 THEN 'Converted'
        ELSE 'Unqualified'
    END AS lead_status,
    CASE WHEN RANDOM() < 0.60 THEN DATEADD('day', UNIFORM(1, 14, RANDOM()), DATEADD('day', -UNIFORM(0, 365, RANDOM()), CURRENT_DATE())) ELSE NULL END AS mql_date,
    CASE WHEN RANDOM() < 0.40 THEN DATEADD('day', UNIFORM(7, 30, RANDOM()), DATEADD('day', -UNIFORM(0, 365, RANDOM()), CURRENT_DATE())) ELSE NULL END AS sql_date,
    CASE WHEN RANDOM() < 0.15 THEN DATEADD('day', UNIFORM(14, 60, RANDOM()), DATEADD('day', -UNIFORM(0, 365, RANDOM()), CURRENT_DATE())) ELSE NULL END AS conversion_date,
    NULL AS converted_customer_id,
    CASE WHEN RANDOM() < 0.5 THEN 'google' WHEN RANDOM() < 0.8 THEN 'linkedin' ELSE 'direct' END AS utm_source,
    CASE WHEN RANDOM() < 0.4 THEN 'cpc' WHEN RANDOM() < 0.7 THEN 'organic' ELSE 'referral' END AS utm_medium,
    CASE WHEN RANDOM() < 0.5 THEN 'brand_awareness_2025' ELSE 'lead_gen_q1' END AS utm_campaign,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM 
    (SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) AS rn FROM TABLE(GENERATOR(ROWCOUNT => 1000))) gen
    CROSS JOIN LATERAL (SELECT source, detail FROM sources WHERE RANDOM() < weight ORDER BY RANDOM() LIMIT 1) s
    CROSS JOIN LATERAL (SELECT sdr FROM sdrs ORDER BY RANDOM() LIMIT 1) sdr
    CROSS JOIN LATERAL (SELECT * FROM (VALUES ('RIA'), ('Broker-Dealer'), ('Insurance'), ('Bank')) ORDER BY RANDOM() LIMIT 1) ind(industry)
    CROSS JOIN LATERAL (SELECT * FROM (VALUES ('1-10'), ('11-50'), ('51-200'), ('200+')) ORDER BY RANDOM() LIMIT 1) sz(size)
    CROSS JOIN LATERAL (SELECT * FROM (VALUES ('Pinnacle'), ('Summit'), ('Heritage'), ('Legacy'), ('Cornerstone'), ('Beacon'), ('Horizon'), ('Sterling'), ('Meridian'), ('Capstone')) ORDER BY RANDOM() LIMIT 1) fp(prefix)
    CROSS JOIN LATERAL (SELECT * FROM (VALUES ('Financial Advisors'), ('Wealth Management'), ('Financial Group'), ('Advisory Services'), ('Capital Partners')) ORDER BY RANDOM() LIMIT 1) fs(suffix)
    CROSS JOIN LATERAL (SELECT * FROM (VALUES ('James'), ('Mary'), ('John'), ('Patricia'), ('Robert'), ('Jennifer'), ('Michael'), ('Linda'), ('William'), ('Elizabeth')) ORDER BY RANDOM() LIMIT 1) fn(fname)
    CROSS JOIN LATERAL (SELECT * FROM (VALUES ('Smith'), ('Johnson'), ('Williams'), ('Brown'), ('Jones'), ('Garcia'), ('Miller'), ('Davis')) ORDER BY RANDOM() LIMIT 1) ln(lname);

-- Generate Sales Opportunities
INSERT INTO SALES_OPPORTUNITIES
WITH 
owners AS (SELECT * FROM (VALUES 
    ('Tom Brady'), ('Jessica Williams'), ('Marcus Johnson'), ('Rachel Kim'), ('Andrew Scott')
) AS t(owner)),
stages AS (SELECT * FROM (VALUES 
    ('Discovery', 10), ('Demo', 25), ('Proposal', 50), ('Negotiation', 75), ('Closed Won', 100), ('Closed Lost', 0)
) AS t(stage, probability))
SELECT
    'OPP-' || LPAD(ROW_NUMBER() OVER (ORDER BY RANDOM())::VARCHAR, 6, '0') AS opportunity_id,
    l.lead_id,
    NULL AS customer_id,
    l.company_name || ' - ' || 
        CASE WHEN RANDOM() < 0.7 THEN 'New Business' ELSE 'Platform Upgrade' END AS opportunity_name,
    CASE WHEN RANDOM() < 0.7 THEN 'New Business' WHEN RANDOM() < 0.9 THEN 'Upsell' ELSE 'Cross-sell' END AS opportunity_type,
    st.stage,
    st.probability,
    UNIFORM(5000, 100000, RANDOM()) AS amount,
    UNIFORM(5000, 100000, RANDOM()) AS arr_value,
    l.created_date::DATE AS created_date,
    DATEADD('day', UNIFORM(30, 120, RANDOM()), l.created_date::DATE) AS close_date,
    CASE WHEN st.stage IN ('Closed Won', 'Closed Lost') 
        THEN DATEADD('day', UNIFORM(30, 90, RANDOM()), l.created_date::DATE)
        ELSE NULL 
    END AS actual_close_date,
    o.owner,
    CASE 
        WHEN RANDOM() < 0.3 THEN 'Marketing Suite'
        WHEN RANDOM() < 0.5 THEN 'Marketing Suite, Website Pro'
        WHEN RANDOM() < 0.7 THEN 'Marketing Suite, MyRepChat'
        WHEN RANDOM() < 0.9 THEN 'Full Platform Bundle'
        ELSE 'Do It For Me'
    END AS products_interested,
    CASE WHEN RANDOM() < 0.3 THEN 'Broadridge' WHEN RANDOM() < 0.5 THEN 'Snappy Kraken' WHEN RANDOM() < 0.7 THEN 'Twenty Over Ten' ELSE NULL END AS competitor,
    CASE WHEN st.stage = 'Closed Lost' THEN 
        CASE WHEN RANDOM() < 0.3 THEN 'Price too high'
             WHEN RANDOM() < 0.6 THEN 'Went with competitor'
             ELSE 'No decision made' END
        ELSE NULL 
    END AS loss_reason,
    CASE WHEN st.stage = 'Closed Won' THEN 
        CASE WHEN RANDOM() < 0.3 THEN 'Strong product fit'
             WHEN RANDOM() < 0.6 THEN 'Compliance features'
             ELSE 'Great demo experience' END
        ELSE NULL 
    END AS win_reason,
    UNIFORM(15, 120, RANDOM()) AS days_in_pipeline,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM 
    SALES_LEADS l
    CROSS JOIN LATERAL (SELECT owner FROM owners ORDER BY RANDOM() LIMIT 1) o
    CROSS JOIN LATERAL (SELECT stage, probability FROM stages ORDER BY RANDOM() LIMIT 1) st
WHERE 
    l.lead_status IN ('Qualified', 'Converted')
    AND RANDOM() < 0.8;

-- ============================================================================
-- STEP 8: Create Views for Common Reporting
-- ============================================================================

-- Move to curated schema for business-ready views
USE SCHEMA FMG_PRODUCTION.CURATED;

-- Customer 360 View
CREATE OR REPLACE VIEW V_CUSTOMER_360 AS
SELECT 
    c.customer_id,
    c.company_name,
    c.segment,
    c.industry,
    c.sub_industry,
    c.state,
    c.account_status,
    c.csm_owner,
    c.created_date AS customer_since,
    DATEDIFF('month', c.created_date, CURRENT_DATE()) AS tenure_months,
    -- Subscription metrics
    COUNT(DISTINCT s.subscription_id) AS active_subscriptions,
    COALESCE(SUM(s.mrr_amount), 0) AS total_mrr,
    COALESCE(SUM(s.arr_amount), 0) AS total_arr,
    -- User metrics
    COUNT(DISTINCT u.user_id) AS total_users,
    COUNT(DISTINCT CASE WHEN u.user_status = 'Active' THEN u.user_id END) AS active_users,
    MAX(u.last_login_date) AS last_user_login,
    -- Health metrics (most recent)
    h.overall_health_score,
    h.churn_risk,
    -- Support metrics
    COUNT(DISTINCT t.ticket_id) AS total_tickets_90d
FROM 
    FMG_PRODUCTION.RAW.CUSTOMERS c
    LEFT JOIN FMG_PRODUCTION.RAW.SUBSCRIPTIONS s ON c.customer_id = s.customer_id AND s.status = 'Active'
    LEFT JOIN FMG_PRODUCTION.RAW.USERS u ON c.customer_id = u.customer_id
    LEFT JOIN FMG_PRODUCTION.RAW.CUSTOMER_HEALTH_SCORES h ON c.customer_id = h.customer_id 
        AND h.snapshot_date = (SELECT MAX(snapshot_date) FROM FMG_PRODUCTION.RAW.CUSTOMER_HEALTH_SCORES)
    LEFT JOIN FMG_PRODUCTION.RAW.SUPPORT_TICKETS t ON c.customer_id = t.customer_id 
        AND t.created_date >= DATEADD('day', -90, CURRENT_TIMESTAMP())
GROUP BY 
    c.customer_id, c.company_name, c.segment, c.industry, c.sub_industry, 
    c.state, c.account_status, c.csm_owner, c.created_date,
    h.overall_health_score, h.churn_risk;

-- Revenue Summary View
CREATE OR REPLACE VIEW V_REVENUE_SUMMARY AS
SELECT 
    DATE_TRUNC('month', s.start_date) AS cohort_month,
    c.segment,
    c.industry,
    s.product_name,
    s.plan_tier,
    COUNT(DISTINCT s.subscription_id) AS subscription_count,
    SUM(s.mrr_amount) AS total_mrr,
    SUM(s.arr_amount) AS total_arr,
    AVG(s.mrr_amount) AS avg_mrr,
    COUNT(DISTINCT CASE WHEN s.status = 'Cancelled' THEN s.subscription_id END) AS churned_subscriptions
FROM 
    FMG_PRODUCTION.RAW.SUBSCRIPTIONS s
    JOIN FMG_PRODUCTION.RAW.CUSTOMERS c ON s.customer_id = c.customer_id
GROUP BY 
    DATE_TRUNC('month', s.start_date), c.segment, c.industry, s.product_name, s.plan_tier;

-- ============================================================================
-- STEP 9: Verify Data Generation
-- ============================================================================

SELECT 'CUSTOMERS' AS table_name, COUNT(*) AS row_count FROM FMG_PRODUCTION.RAW.CUSTOMERS
UNION ALL SELECT 'USERS', COUNT(*) FROM FMG_PRODUCTION.RAW.USERS
UNION ALL SELECT 'SUBSCRIPTIONS', COUNT(*) FROM FMG_PRODUCTION.RAW.SUBSCRIPTIONS
UNION ALL SELECT 'PLATFORM_USAGE_DAILY', COUNT(*) FROM FMG_PRODUCTION.RAW.PLATFORM_USAGE_DAILY
UNION ALL SELECT 'CUSTOMER_HEALTH_SCORES', COUNT(*) FROM FMG_PRODUCTION.RAW.CUSTOMER_HEALTH_SCORES
UNION ALL SELECT 'NPS_RESPONSES', COUNT(*) FROM FMG_PRODUCTION.RAW.NPS_RESPONSES
UNION ALL SELECT 'SUPPORT_TICKETS', COUNT(*) FROM FMG_PRODUCTION.RAW.SUPPORT_TICKETS
UNION ALL SELECT 'SALES_LEADS', COUNT(*) FROM FMG_PRODUCTION.RAW.SALES_LEADS
UNION ALL SELECT 'SALES_OPPORTUNITIES', COUNT(*) FROM FMG_PRODUCTION.RAW.SALES_OPPORTUNITIES
ORDER BY table_name;

-- ============================================================================
-- DATA GENERATION COMPLETE!
-- Next Step: Proceed to Lab 1 - Getting Started with Snowflake
-- ============================================================================

SELECT '✅ FMG Synthetic Data Generation Complete!' AS STATUS,
       CURRENT_TIMESTAMP() AS COMPLETED_AT;

