USE ROLE loader_role;
USE WAREHOUSE trading_pipeline_wh;
USE DATABASE trading_pipeline;
USE SCHEMA bronze;

-- Named file format — CSV with header row (matches Alpaca/loader script output)
CREATE FILE FORMAT IF NOT EXISTS csv_with_header
    TYPE = 'CSV'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NULL');

-- Shared internal stage — both raw_prices and raw_trades CSVs load through here
CREATE STAGE IF NOT EXISTS bronze_load_stage
    FILE_FORMAT = csv_with_header;