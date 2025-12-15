/*=============================================================================
  FMG SUITE - LAB 2: GOVERNANCE + FINOPS
  Script 4: Auditing with ACCOUNT_USAGE
  
  Description: Query ACCOUNT_USAGE views for compliance, auditing, and optimization
  Prerequisites: ACCOUNTADMIN access (or SNOWFLAKE database grants)
  Duration: ~15 minutes
=============================================================================*/

-- ============================================================================
-- SECTION 1: SET CONTEXT
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE FMG_DEV_XS;

-- ============================================================================
-- SECTION 2: UNDERSTANDING ACCOUNT_USAGE
-- ============================================================================

/*
    The SNOWFLAKE.ACCOUNT_USAGE schema contains:
    
    CATEGORY                    KEY VIEWS
    ─────────────────────────────────────────────────────────────
    Query & Access              QUERY_HISTORY, ACCESS_HISTORY
    Security                    LOGIN_HISTORY, SESSIONS
    Users & Roles               USERS, ROLES, GRANTS_TO_*
    Objects                     TABLES, VIEWS, COLUMNS
    Storage                     STORAGE_USAGE, TABLE_STORAGE_METRICS
    Compute                     WAREHOUSE_METERING_HISTORY
    Serverless                  SERVERLESS_TASK_HISTORY, PIPE_USAGE_HISTORY
    Data Sharing                DATA_SHARING_USAGE
    
    Key Characteristics:
    - 365 days of history
    - ~45 minute to 3 hour data latency
    - Only ACCOUNTADMIN has default access
    - Can grant to other roles via IMPORTED PRIVILEGES
*/

-- List all views in ACCOUNT_USAGE
SHOW VIEWS IN SCHEMA SNOWFLAKE.ACCOUNT_USAGE;

-- ============================================================================
-- SECTION 3: QUERY HISTORY ANALYSIS
-- ============================================================================

-- Top 20 most expensive queries (by credits) in the last 30 days
SELECT 
    query_id,
    query_type,
    user_name,
    role_name,
    warehouse_name,
    warehouse_size,
    ROUND(total_elapsed_time / 1000, 2) AS elapsed_seconds,
    ROUND(credits_used_cloud_services, 4) AS cloud_credits,
    rows_produced,
    bytes_scanned / POWER(1024, 3) AS gb_scanned,
    LEFT(query_text, 200) AS query_preview
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
AND warehouse_name LIKE 'FMG%'
ORDER BY total_elapsed_time DESC
LIMIT 20;

-- Query volume by user and role
SELECT 
    user_name,
    role_name,
    COUNT(*) AS query_count,
    SUM(CASE WHEN execution_status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_queries,
    SUM(CASE WHEN execution_status = 'FAIL' THEN 1 ELSE 0 END) AS failed_queries,
    ROUND(AVG(total_elapsed_time) / 1000, 2) AS avg_elapsed_seconds,
    ROUND(SUM(bytes_scanned) / POWER(1024, 4), 4) AS total_tb_scanned
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
AND warehouse_name LIKE 'FMG%'
GROUP BY 1, 2
ORDER BY query_count DESC
LIMIT 20;

-- Query patterns by hour of day (for capacity planning)
SELECT 
    HOUR(start_time) AS hour_of_day,
    DAYNAME(start_time) AS day_of_week,
    COUNT(*) AS query_count,
    AVG(total_elapsed_time) / 1000 AS avg_seconds
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
AND warehouse_name LIKE 'FMG%'
GROUP BY 1, 2
ORDER BY hour_of_day, 
    CASE day_of_week 
        WHEN 'Mon' THEN 1 WHEN 'Tue' THEN 2 WHEN 'Wed' THEN 3 
        WHEN 'Thu' THEN 4 WHEN 'Fri' THEN 5 WHEN 'Sat' THEN 6 ELSE 7 
    END;

-- ============================================================================
-- SECTION 4: ACCESS HISTORY (Data Access Audit)
-- ============================================================================

/*
    ACCESS_HISTORY tracks:
    - What objects were read (direct_objects_accessed)
    - What base objects were touched (base_objects_accessed)  
    - What objects were modified (objects_modified)
    
    Essential for compliance: "Who accessed customer data?"
*/

-- Recent access to FMG tables
SELECT 
    query_start_time,
    user_name,
    direct_objects_accessed,
    base_objects_accessed,
    objects_modified
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
WHERE query_start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
AND ARRAY_SIZE(direct_objects_accessed) > 0
ORDER BY query_start_time DESC
LIMIT 50;

-- Find all access to sensitive tables (CUSTOMERS, USERS)
SELECT 
    DATE_TRUNC('day', query_start_time) AS access_date,
    user_name,
    obj.value:objectName::STRING AS object_accessed,
    COUNT(*) AS access_count
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY,
    LATERAL FLATTEN(input => direct_objects_accessed) obj
WHERE query_start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
AND obj.value:objectName::STRING IN ('FMG_PRODUCTION.RAW.CUSTOMERS', 'FMG_PRODUCTION.RAW.USERS')
GROUP BY 1, 2, 3
ORDER BY access_date DESC, access_count DESC;

-- Track who modified data (INSERT/UPDATE/DELETE)
SELECT 
    query_start_time,
    user_name,
    mod.value:objectName::STRING AS table_modified,
    mod.value:columns AS columns_modified
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY,
    LATERAL FLATTEN(input => objects_modified) mod
WHERE query_start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
AND mod.value:objectName::STRING LIKE 'FMG_PRODUCTION%'
ORDER BY query_start_time DESC
LIMIT 50;

-- ============================================================================
-- SECTION 5: LOGIN HISTORY (Security Audit)
-- ============================================================================

-- Recent login activity
SELECT 
    event_timestamp,
    user_name,
    client_ip,
    reported_client_type,
    first_authentication_factor,
    second_authentication_factor,
    is_success,
    error_code,
    error_message
FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
WHERE event_timestamp >= DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY event_timestamp DESC
LIMIT 100;

-- Failed login attempts (security concern)
SELECT 
    DATE_TRUNC('day', event_timestamp) AS login_date,
    user_name,
    client_ip,
    error_code,
    error_message,
    COUNT(*) AS failed_attempts
FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
WHERE event_timestamp >= DATEADD('day', -30, CURRENT_TIMESTAMP())
AND is_success = 'NO'
GROUP BY 1, 2, 3, 4, 5
ORDER BY failed_attempts DESC
LIMIT 20;

-- Users without MFA (security risk)
SELECT 
    l.user_name,
    MAX(l.event_timestamp) AS last_login,
    COUNT(CASE WHEN l.second_authentication_factor IS NULL THEN 1 END) AS logins_without_mfa,
    COUNT(*) AS total_logins,
    ROUND(COUNT(CASE WHEN l.second_authentication_factor IS NULL THEN 1 END) * 100.0 / COUNT(*), 2) AS pct_without_mfa
FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY l
WHERE l.event_timestamp >= DATEADD('day', -30, CURRENT_TIMESTAMP())
AND l.is_success = 'YES'
GROUP BY 1
HAVING COUNT(CASE WHEN l.second_authentication_factor IS NULL THEN 1 END) > 0
ORDER BY pct_without_mfa DESC;

-- ============================================================================
-- SECTION 6: USER & ROLE AUDITING
-- ============================================================================

-- All users and their status
SELECT 
    name AS user_name,
    created_on,
    login_name,
    email,
    default_role,
    default_warehouse,
    disabled,
    last_success_login,
    DATEDIFF('day', last_success_login, CURRENT_TIMESTAMP()) AS days_since_login
FROM SNOWFLAKE.ACCOUNT_USAGE.USERS
WHERE deleted_on IS NULL
ORDER BY days_since_login DESC;

-- Inactive users (no login in 90+ days)
SELECT 
    name AS user_name,
    email,
    default_role,
    last_success_login,
    DATEDIFF('day', last_success_login, CURRENT_TIMESTAMP()) AS days_inactive
FROM SNOWFLAKE.ACCOUNT_USAGE.USERS
WHERE deleted_on IS NULL
AND last_success_login < DATEADD('day', -90, CURRENT_TIMESTAMP())
ORDER BY days_inactive DESC;

-- Role membership audit
SELECT 
    grantee_name AS user_or_role,
    role AS granted_role,
    granted_by,
    created_on
FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_USERS
WHERE deleted_on IS NULL
AND granted_role LIKE 'FMG%'
ORDER BY grantee_name, granted_role;

-- Privilege escalation audit (who has ACCOUNTADMIN?)
SELECT 
    grantee_name,
    role,
    granted_by,
    created_on
FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_USERS
WHERE role = 'ACCOUNTADMIN'
AND deleted_on IS NULL;

-- ============================================================================
-- SECTION 7: STORAGE AUDITING
-- ============================================================================

-- Overall storage usage trend
SELECT 
    usage_date,
    ROUND(storage_bytes / POWER(1024, 4), 4) AS storage_tb,
    ROUND(stage_bytes / POWER(1024, 4), 4) AS stage_tb,
    ROUND(failsafe_bytes / POWER(1024, 4), 4) AS failsafe_tb,
    ROUND((storage_bytes + stage_bytes + failsafe_bytes) / POWER(1024, 4), 4) AS total_tb
FROM SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE
WHERE usage_date >= DATEADD('day', -90, CURRENT_DATE())
ORDER BY usage_date DESC;

-- Storage by database
SELECT 
    table_catalog AS database_name,
    ROUND(SUM(active_bytes) / POWER(1024, 3), 2) AS active_gb,
    ROUND(SUM(time_travel_bytes) / POWER(1024, 3), 2) AS time_travel_gb,
    ROUND(SUM(failsafe_bytes) / POWER(1024, 3), 2) AS failsafe_gb,
    ROUND(SUM(active_bytes + time_travel_bytes + failsafe_bytes) / POWER(1024, 3), 2) AS total_gb
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
WHERE table_catalog LIKE 'FMG%'
GROUP BY 1
ORDER BY total_gb DESC;

-- Largest tables by storage
SELECT 
    table_catalog AS database_name,
    table_schema,
    table_name,
    ROUND(active_bytes / POWER(1024, 3), 4) AS active_gb,
    ROUND(time_travel_bytes / POWER(1024, 3), 4) AS time_travel_gb,
    row_count
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
WHERE table_catalog LIKE 'FMG%'
AND active_bytes > 0
ORDER BY active_bytes DESC
LIMIT 20;

-- ============================================================================
-- SECTION 8: CREATE AUDIT VIEWS FOR FMG
-- ============================================================================

USE DATABASE FMG_PRODUCTION;
USE SCHEMA GOVERNANCE;

-- Daily security audit summary
CREATE OR REPLACE VIEW V_DAILY_SECURITY_AUDIT AS
WITH login_stats AS (
    SELECT 
        DATE_TRUNC('day', event_timestamp) AS audit_date,
        COUNT(*) AS total_logins,
        COUNT(CASE WHEN is_success = 'YES' THEN 1 END) AS successful_logins,
        COUNT(CASE WHEN is_success = 'NO' THEN 1 END) AS failed_logins,
        COUNT(DISTINCT user_name) AS unique_users,
        COUNT(DISTINCT client_ip) AS unique_ips
    FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
    WHERE event_timestamp >= DATEADD('day', -30, CURRENT_TIMESTAMP())
    GROUP BY 1
),
query_stats AS (
    SELECT 
        DATE_TRUNC('day', start_time) AS audit_date,
        COUNT(*) AS total_queries,
        COUNT(DISTINCT user_name) AS querying_users,
        SUM(bytes_scanned) / POWER(1024, 4) AS tb_scanned
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
    AND warehouse_name LIKE 'FMG%'
    GROUP BY 1
)
SELECT 
    COALESCE(l.audit_date, q.audit_date) AS audit_date,
    l.total_logins,
    l.successful_logins,
    l.failed_logins,
    l.unique_users AS users_logged_in,
    l.unique_ips,
    q.total_queries,
    q.querying_users,
    ROUND(q.tb_scanned, 4) AS tb_scanned
FROM login_stats l
FULL OUTER JOIN query_stats q ON l.audit_date = q.audit_date
ORDER BY audit_date DESC;

-- PII access audit trail
CREATE OR REPLACE VIEW V_PII_ACCESS_AUDIT AS
SELECT 
    DATE_TRUNC('day', query_start_time) AS access_date,
    user_name,
    obj.value:objectName::STRING AS table_accessed,
    COUNT(*) AS access_count,
    'Review for compliance' AS audit_note
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY,
    LATERAL FLATTEN(input => direct_objects_accessed) obj
WHERE query_start_time >= DATEADD('day', -90, CURRENT_TIMESTAMP())
AND (
    obj.value:objectName::STRING LIKE '%USERS%'
    OR obj.value:objectName::STRING LIKE '%CUSTOMER%'
    OR obj.value:objectName::STRING LIKE '%PII%'
)
GROUP BY 1, 2, 3
ORDER BY access_date DESC, access_count DESC;

-- ============================================================================
-- SECTION 9: GRANT AUDIT ACCESS TO COMPLIANCE ROLE
-- ============================================================================

-- Grant compliance team access to audit data
USE ROLE ACCOUNTADMIN;

-- Create a role for auditors if not exists
CREATE ROLE IF NOT EXISTS FMG_AUDITOR
    COMMENT = 'Role for internal and external auditors';

-- Grant access to SNOWFLAKE database for auditing
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE FMG_AUDITOR;

-- Grant access to governance views
GRANT USAGE ON DATABASE FMG_PRODUCTION TO ROLE FMG_AUDITOR;
GRANT USAGE ON SCHEMA FMG_PRODUCTION.GOVERNANCE TO ROLE FMG_AUDITOR;
GRANT SELECT ON ALL VIEWS IN SCHEMA FMG_PRODUCTION.GOVERNANCE TO ROLE FMG_AUDITOR;

-- ============================================================================
-- SECTION 10: COMPLIANCE CHECKLIST QUERIES
-- ============================================================================

-- SOC 2 / Compliance checklist queries

-- 1. Are all users using MFA?
SELECT 'MFA Compliance' AS check_name,
    COUNT(CASE WHEN second_authentication_factor IS NOT NULL THEN 1 END) AS with_mfa,
    COUNT(CASE WHEN second_authentication_factor IS NULL THEN 1 END) AS without_mfa,
    ROUND(COUNT(CASE WHEN second_authentication_factor IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) AS mfa_pct
FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
WHERE event_timestamp >= DATEADD('day', -30, CURRENT_TIMESTAMP())
AND is_success = 'YES';

-- 2. Are there any shared credentials? (same IP, multiple users)
SELECT 'Shared Credential Risk' AS check_name,
    client_ip,
    LISTAGG(DISTINCT user_name, ', ') AS users_from_ip,
    COUNT(DISTINCT user_name) AS user_count
FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
WHERE event_timestamp >= DATEADD('day', -7, CURRENT_TIMESTAMP())
AND is_success = 'YES'
GROUP BY client_ip
HAVING COUNT(DISTINCT user_name) > 1
ORDER BY user_count DESC;

-- 3. Privilege review (users with high-privilege roles)
SELECT 'High Privilege Users' AS check_name,
    grantee_name,
    role
FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_USERS
WHERE role IN ('ACCOUNTADMIN', 'SECURITYADMIN', 'SYSADMIN')
AND deleted_on IS NULL;

-- 4. Data access outside business hours
SELECT 'Off-Hours Access' AS check_name,
    DATE_TRUNC('day', query_start_time) AS access_date,
    user_name,
    COUNT(*) AS query_count
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
AND (HOUR(start_time) < 6 OR HOUR(start_time) > 22)  -- Before 6 AM or after 10 PM
AND warehouse_name LIKE 'FMG%'
GROUP BY 1, 2
ORDER BY query_count DESC;

-- ============================================================================
-- SCRIPT COMPLETE!
-- ============================================================================

SELECT '✅ Audit Setup Complete!' AS STATUS,
       'Review V_DAILY_SECURITY_AUDIT for daily monitoring' AS NEXT_STEP,
       CURRENT_TIMESTAMP() AS COMPLETED_AT;

