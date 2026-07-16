CREATE ROLE IF NOT EXISTS fivetran_role;
GRANT ROLE fivetran_role TO ROLE SYSADMIN;

GRANT USAGE ON WAREHOUSE trading_pipeline_wh TO ROLE fivetran_role;
GRANT USAGE ON DATABASE trading_pipeline TO ROLE fivetran_role;
GRANT USAGE ON SCHEMA trading_pipeline.bronze TO ROLE fivetran_role;

GRANT CREATE TABLE ON SCHEMA trading_pipeline.bronze TO ROLE fivetran_role;
GRANT CREATE TEMPORARY TABLE ON SCHEMA trading_pipeline.bronze TO ROLE fivetran_role;
GRANT CREATE STAGE ON SCHEMA trading_pipeline.bronze TO ROLE fivetran_role;
GRANT CREATE FILE FORMAT ON SCHEMA trading_pipeline.bronze TO ROLE fivetran_role;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA trading_pipeline.bronze TO ROLE fivetran_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA trading_pipeline.bronze TO ROLE fivetran_role;
