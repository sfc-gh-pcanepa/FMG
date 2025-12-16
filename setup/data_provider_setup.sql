/*=============================================================================
  FMG SUITE - DATA PROVIDER SETUP
  
  Run this in YOUR account (the data provider) to:
  1. Create all sample FMG data
  2. Create a share to deliver to the prospect's trial account
  
  After running this, add the prospect's account to the share.
=============================================================================*/

USE ROLE ACCOUNTADMIN;

-- ============================================================================
-- STEP 1: CREATE DATABASE AND SCHEMA
-- ============================================================================
CREATE DATABASE IF NOT EXISTS FMG_SAMPLE_DATA;
CREATE SCHEMA IF NOT EXISTS FMG_SAMPLE_DATA.FMG;
USE SCHEMA FMG_SAMPLE_DATA.FMG;

-- Create a warehouse for data generation
CREATE WAREHOUSE IF NOT EXISTS FMG_SETUP_WH
    WAREHOUSE_SIZE = 'XSMALL'
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

-- USERS (with PII for masking demo)
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

-- CUSTOMER_FEEDBACK (for Cortex demos)
CREATE OR REPLACE TABLE CUSTOMER_FEEDBACK (
    feedback_id VARCHAR(20),
    customer_id VARCHAR(20),
    nps_score INT,
    feedback_text VARCHAR(1000),
    submitted_date DATE
);

-- KNOWLEDGE_BASE (for Cortex Search demo)
CREATE OR REPLACE TABLE KNOWLEDGE_BASE (
    article_id VARCHAR(10),
    title VARCHAR(200),
    content VARCHAR(2000),
    category VARCHAR(50)
);

-- ============================================================================
-- STEP 3: INSERT SAMPLE DATA
-- ============================================================================

-- CUSTOMERS
INSERT INTO CUSTOMERS VALUES
    ('C001', 'Acme Financial Advisors', 'Enterprise', 'RIA', 2500.00, 85, '2022-01-15'),
    ('C002', 'Summit Wealth Management', 'Mid-Market', 'RIA', 899.00, 72, '2022-03-20'),
    ('C003', 'Peak Advisory Group', 'SMB', 'Independent RIA', 299.00, 91, '2023-06-01'),
    ('C004', 'Horizon Financial Partners', 'Enterprise', 'Broker-Dealer', 3200.00, 68, '2021-11-10'),
    ('C005', 'Cascade Investment Services', 'Mid-Market', 'RIA', 599.00, 88, '2023-01-25'),
    ('C006', 'Alpine Wealth Advisors', 'SMB', 'Independent RIA', 199.00, 95, '2023-08-15'),
    ('C007', 'Meridian Financial Group', 'Enterprise', 'Wirehouse', 4500.00, 78, '2020-05-01'),
    ('C008', 'Coastal Advisory Partners', 'Mid-Market', 'Insurance', 750.00, 82, '2022-09-10');

-- USERS
INSERT INTO USERS VALUES
    ('U001', 'C001', 'john.smith@acmefinancial.com', '(555) 123-4567', 'John Smith', 'Admin'),
    ('U002', 'C001', 'sarah.jones@acmefinancial.com', '(555) 234-5678', 'Sarah Jones', 'Advisor'),
    ('U003', 'C002', 'mike.chen@summitwm.com', '(555) 345-6789', 'Mike Chen', 'Admin'),
    ('U004', 'C003', 'lisa.park@peakadvisory.com', '(555) 456-7890', 'Lisa Park', 'Advisor'),
    ('U005', 'C004', 'david.wilson@horizonfin.com', '(555) 567-8901', 'David Wilson', 'Compliance'),
    ('U006', 'C005', 'emma.davis@cascadeinvest.com', '(555) 678-9012', 'Emma Davis', 'Admin'),
    ('U007', 'C006', 'ryan.taylor@alpinewealth.com', '(555) 789-0123', 'Ryan Taylor', 'Advisor'),
    ('U008', 'C007', 'jennifer.brown@meridianfg.com', '(555) 890-1234', 'Jennifer Brown', 'Admin');

-- SUBSCRIPTIONS
INSERT INTO SUBSCRIPTIONS VALUES
    ('S001', 'C001', 'Marketing Suite', 1500.00, 'Active', '2022-01-15'),
    ('S002', 'C001', 'Website Pro', 500.00, 'Active', '2022-01-15'),
    ('S003', 'C001', 'MyRepChat', 500.00, 'Active', '2022-06-01'),
    ('S004', 'C002', 'Marketing Suite', 599.00, 'Active', '2022-03-20'),
    ('S005', 'C002', 'MyRepChat', 300.00, 'Active', '2022-03-20'),
    ('S006', 'C003', 'Marketing Suite', 299.00, 'Active', '2023-06-01'),
    ('S007', 'C004', 'Marketing Suite', 1800.00, 'Active', '2021-11-10'),
    ('S008', 'C004', 'Website Pro', 800.00, 'Active', '2021-11-10'),
    ('S009', 'C004', 'Do It For Me', 600.00, 'Cancelled', '2021-11-10'),
    ('S010', 'C005', 'Marketing Suite', 599.00, 'Active', '2023-01-25'),
    ('S011', 'C006', 'Marketing Suite', 199.00, 'Active', '2023-08-15'),
    ('S012', 'C007', 'Marketing Suite', 2500.00, 'Active', '2020-05-01'),
    ('S013', 'C007', 'Website Pro', 1000.00, 'Active', '2020-05-01'),
    ('S014', 'C007', 'MyRepChat', 500.00, 'Active', '2020-08-01'),
    ('S015', 'C007', 'Do It For Me', 500.00, 'Active', '2021-01-01'),
    ('S016', 'C008', 'Marketing Suite', 450.00, 'Active', '2022-09-10'),
    ('S017', 'C008', 'MyRepChat', 300.00, 'Active', '2022-09-10');

-- CUSTOMER_FEEDBACK
INSERT INTO CUSTOMER_FEEDBACK VALUES
    ('F001', 'C001', 9, 'Love the email marketing tools! Our open rates have increased 40% since switching to FMG.', '2024-01-15'),
    ('F002', 'C002', 7, 'Good product overall but the social media scheduler could be more intuitive.', '2024-01-18'),
    ('F003', 'C003', 10, 'Amazing support team! They helped us set up everything in one day. Highly recommend!', '2024-02-01'),
    ('F004', 'C004', 4, 'Disappointed with the recent price increase. Considering other options.', '2024-02-10'),
    ('F005', 'C005', 8, 'The website builder is fantastic. Would love to see more templates though.', '2024-02-15'),
    ('F006', 'C001', 9, 'MyRepChat has been a game-changer for client communication. Compliance loves it!', '2024-03-01'),
    ('F007', 'C006', 10, 'Best decision we made this year. The ROI is incredible.', '2024-03-10'),
    ('F008', 'C007', 6, 'Solid platform but wish there was better integration with our CRM.', '2024-03-15'),
    ('F009', 'C008', 8, 'Great for our insurance practice. The compliance features are top-notch.', '2024-03-20');

-- KNOWLEDGE_BASE
INSERT INTO KNOWLEDGE_BASE VALUES
    ('KB001', 'How to Create Email Campaigns', 'Navigate to Marketing Tools, click Create Campaign, select a template, customize your content, choose recipients from your contact list, and schedule or send immediately. Pro tip: Use A/B testing for subject lines to optimize open rates.', 'Email'),
    ('KB002', 'Troubleshooting Email Delivery', 'If emails are not delivering, check: 1) SPF/DKIM settings in your domain, 2) Verify recipient email addresses, 3) Review bounce reports in Analytics, 4) Ensure content is not triggering spam filters. Contact support if issues persist.', 'Email'),
    ('KB003', 'Setting Up MyRepChat', 'Download the MyRepChat app from your app store. Log in with your FMG credentials. Complete the compliance acknowledgment. Start messaging clients - all conversations are automatically archived for compliance review.', 'MyRepChat'),
    ('KB004', 'Website Analytics Overview', 'Access your website analytics via Dashboard > Website > Analytics. View page views, unique visitors, session duration, lead captures, and conversion rates. Export reports for client meetings or compliance records.', 'Website'),
    ('KB005', 'Scheduling Social Media Posts', 'Go to Social Media > Create Post. Write your content or use our AI assistant. Select platforms (LinkedIn, Facebook, Twitter). Click Schedule, pick date and time. View all scheduled posts in the Calendar view.', 'Social'),
    ('KB006', 'Compliance Archive Access', 'All communications are automatically archived. Access via Compliance > Archive. Search by date, client name, or keyword. Export records for audits. Retention period is configurable based on your compliance requirements.', 'Compliance');

-- ============================================================================
-- STEP 4: CREATE THE DATA SHARE
-- ============================================================================

CREATE OR REPLACE SHARE FMG_LABS_SHARE
    COMMENT = 'FMG Sample Data for Hands-On Labs';

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

-- ============================================================================
-- STEP 5: ADD CONSUMER ACCOUNT (Run this after getting prospect's account ID)
-- ============================================================================
-- Replace XXXXXXX.YYYYYYY with the prospect's account locator
-- ALTER SHARE FMG_LABS_SHARE ADD ACCOUNTS = XXXXXXX.YYYYYYY;

-- To find the prospect's account locator, have them run:
-- SELECT CURRENT_ORGANIZATION_NAME() || '.' || CURRENT_ACCOUNT_NAME();

-- ============================================================================
-- VERIFICATION
-- ============================================================================
SELECT 'CUSTOMERS' AS table_name, COUNT(*) AS rows FROM CUSTOMERS
UNION ALL SELECT 'USERS', COUNT(*) FROM USERS
UNION ALL SELECT 'SUBSCRIPTIONS', COUNT(*) FROM SUBSCRIPTIONS
UNION ALL SELECT 'CUSTOMER_FEEDBACK', COUNT(*) FROM CUSTOMER_FEEDBACK
UNION ALL SELECT 'KNOWLEDGE_BASE', COUNT(*) FROM KNOWLEDGE_BASE;

SELECT '✅ Data Provider Setup Complete!' AS STATUS;
SELECT 'Next: Add prospect account to share with ALTER SHARE FMG_LABS_SHARE ADD ACCOUNTS = <account_locator>;' AS NEXT_STEP;

