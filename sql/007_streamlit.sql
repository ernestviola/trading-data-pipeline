CREATE ROLE IF NOT EXISTS streamlit_role;
GRANT ROLE streamlit_role TO ROLE SYSADMIN;

GRANT USAGE ON WAREHOUSE trading_pipeline_wh TO ROLE streamlit_role;
GRANT USAGE ON DATABASE trading_pipeline TO ROLE streamlit_role;
GRANT USAGE ON SCHEMA trading_pipeline.gold TO ROLE streamlit_role;

GRANT SELECT ON ALL TABLES IN SCHEMA trading_pipeline.gold TO ROLE streamlit_role;
GRANT SELECT ON ALL VIEWS IN SCHEMA trading_pipeline.gold TO ROLE streamlit_role;
GRANT SELECT ON FUTURE TABLES IN SCHEMA trading_pipeline.gold TO ROLE streamlit_role;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA trading_pipeline.gold TO ROLE streamlit_role;

CREATE USER IF NOT EXISTS streamlit_svc
  TYPE = SERVICE
  DEFAULT_ROLE = streamlit_role
  DEFAULT_WAREHOUSE = trading_pipeline_wh;

GRANT ROLE streamlit_role TO USER streamlit_svc;