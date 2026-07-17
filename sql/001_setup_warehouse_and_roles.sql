-- ============================================================
-- Warehouse
-- ============================================================
CREATE WAREHOUSE IF NOT EXISTS trading_pipeline_wh
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- ============================================================
-- Database + schemas (Medallion, schema-level)
-- ============================================================
CREATE DATABASE IF NOT EXISTS trading_pipeline;

CREATE SCHEMA IF NOT EXISTS trading_pipeline.bronze;
CREATE SCHEMA IF NOT EXISTS trading_pipeline.silver;
CREATE SCHEMA IF NOT EXISTS trading_pipeline.gold;

-- ============================================================
-- Roles
-- ============================================================
CREATE ROLE IF NOT EXISTS loader_role;
CREATE ROLE IF NOT EXISTS transformer_role;

-- Grant roles to ACCOUNTADMIN so they're manageable (standard practice —
-- otherwise a role with no grantee above it becomes orphaned)
GRANT ROLE loader_role TO ROLE SYSADMIN;
GRANT ROLE transformer_role TO ROLE SYSADMIN;

-- ============================================================
-- Warehouse usage — both roles need to run queries
-- ============================================================
GRANT USAGE ON WAREHOUSE trading_pipeline_wh TO ROLE loader_role;
GRANT USAGE ON WAREHOUSE trading_pipeline_wh TO ROLE transformer_role;

-- ============================================================
-- Loader role — write-only into Bronze, no Silver/Gold access
-- ============================================================
GRANT USAGE ON DATABASE trading_pipeline TO ROLE loader_role;
GRANT USAGE ON SCHEMA trading_pipeline.bronze TO ROLE loader_role;

GRANT CREATE TABLE ON SCHEMA trading_pipeline.bronze TO ROLE loader_role;
GRANT CREATE STAGE ON SCHEMA trading_pipeline.bronze TO ROLE loader_role;
GRANT CREATE FILE FORMAT ON SCHEMA trading_pipeline.bronze TO ROLE loader_role;

-- Covers tables the loader creates now, and any created later
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA trading_pipeline.bronze TO ROLE loader_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA trading_pipeline.bronze TO ROLE loader_role;

-- ============================================================
-- Transformer role — read Bronze, read/write Silver + Gold
-- ============================================================
GRANT USAGE ON DATABASE trading_pipeline TO ROLE transformer_role;
GRANT USAGE ON SCHEMA trading_pipeline.bronze TO ROLE transformer_role;
GRANT USAGE ON SCHEMA trading_pipeline.silver TO ROLE transformer_role;
GRANT USAGE ON SCHEMA trading_pipeline.gold TO ROLE transformer_role;

-- Read-only on Bronze — dbt should never mutate raw landed data
GRANT SELECT ON ALL TABLES IN SCHEMA trading_pipeline.bronze TO ROLE transformer_role;
GRANT SELECT ON FUTURE TABLES IN SCHEMA trading_pipeline.bronze TO ROLE transformer_role;

-- Full read/write on Silver + Gold — dbt creates and materializes models here
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA trading_pipeline.silver TO ROLE transformer_role;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA trading_pipeline.gold TO ROLE transformer_role;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA trading_pipeline.silver TO ROLE transformer_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA trading_pipeline.silver TO ROLE transformer_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA trading_pipeline.gold TO ROLE transformer_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA trading_pipeline.gold TO ROLE transformer_role;

-- ============================================================
-- SYSADMIN — read access across all schemas for manual browsing
-- (ACCOUNTADMIN is left untouched, reserved for account-level ops)
-- ============================================================
GRANT USAGE ON WAREHOUSE trading_pipeline_wh TO ROLE SYSADMIN;
GRANT USAGE ON DATABASE trading_pipeline TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA trading_pipeline.bronze TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA trading_pipeline.silver TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA trading_pipeline.gold TO ROLE SYSADMIN;

GRANT SELECT ON ALL TABLES IN SCHEMA trading_pipeline.bronze TO ROLE SYSADMIN;
GRANT SELECT ON FUTURE TABLES IN SCHEMA trading_pipeline.bronze TO ROLE SYSADMIN;
GRANT SELECT ON ALL TABLES IN SCHEMA trading_pipeline.silver TO ROLE SYSADMIN;
GRANT SELECT ON FUTURE TABLES IN SCHEMA trading_pipeline.silver TO ROLE SYSADMIN;
GRANT SELECT ON ALL TABLES IN SCHEMA trading_pipeline.gold TO ROLE SYSADMIN;
GRANT SELECT ON FUTURE TABLES IN SCHEMA trading_pipeline.gold TO ROLE SYSADMIN;

-- ============================================================
-- Assign roles to your user
-- Replace <your_username> with your actual Snowflake login name
-- ============================================================
GRANT ROLE loader_role TO USER "ERNEST";
GRANT ROLE transformer_role TO USER "ERNEST";
-- SYSADMIN is typically already granted to the trial account's default user,
-- but grant explicitly in case it isn't:
GRANT ROLE SYSADMIN TO USER "ERNEST";