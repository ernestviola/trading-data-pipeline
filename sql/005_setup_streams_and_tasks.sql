{# 
WARNING: this stream is an example only. DATA may be incorrect due to the way raw_trades is created.

This stream is append only from raw_trades, but raw_trades recalcs all data based on input values which may change.

There is no way to tell which rows belongs to which stream.

 #}


-- Phase 7: Snowflake Streams & Tasks for incremental processing.
--
-- This is a deliberately standalone path, not wired into Airflow/dbt. It
-- demonstrates the native Snowflake CDC pattern: a Stream tracks new rows
-- landed in Bronze since it was last read, and a Task fires a stored
-- procedure that MERGEs only those changed rows into a Silver-equivalent
-- target - no re-scan of the full table, unlike a dbt run.
--
-- silver.stg_trades_streaming intentionally mirrors dbt's stg_trades model
-- (dbt/models/staging/stg_trades.sql) in shape, but is NOT a dbt model and
-- is not referenced by any dbt model - keeps the two paths (dbt-batch vs.
-- Snowflake-native-incremental) cleanly separate for comparison.
--
-- Run as a role with ownership/grant privileges on bronze + silver schemas
-- (e.g. SYSADMIN), except the final EXECUTE TASK grant, which requires
-- ACCOUNTADMIN.

-- ── Grants (one-time) ────────────────────────────────────────────────────
-- transformer_role already has read-only on Bronze and read/write on
-- Silver/Gold (see README RBAC notes) - just needs the DDL privileges to
-- create these objects, plus account-level permission to actually run a
-- scheduled task (not just CALL it manually).

GRANT CREATE STREAM ON SCHEMA bronze TO ROLE transformer_role;
GRANT CREATE PROCEDURE ON SCHEMA silver TO ROLE transformer_role;
GRANT CREATE TASK ON SCHEMA bronze TO ROLE transformer_role;

-- Run as ACCOUNTADMIN:
-- GRANT EXECUTE TASK ON ACCOUNT TO ROLE transformer_role;

USE ROLE transformer_role;

-- ── Stream ───────────────────────────────────────────────────────────────
-- raw_trades only ever receives inserts (COPY INTO + MERGE with
-- WHEN NOT MATCHED THEN INSERT, no UPDATE clause - see README) - an
-- APPEND_ONLY stream is the right fit: cheaper than a standard stream,
-- since it doesn't need to track update/delete row versions it will never
-- see.
CREATE STREAM IF NOT EXISTS bronze.raw_trades_stream
    ON TABLE bronze.raw_trades
    APPEND_ONLY = TRUE;

-- ── Target table ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS silver.stg_trades_streaming (
    ticker TEXT,
    trade_date DATE,
    side TEXT,
    quantity FLOAT,
    price FLOAT,
    strategy_used TEXT,
    signal_strength FLOAT
);

-- ── Stored procedure ─────────────────────────────────────────────────────
-- MERGE, not INSERT: main.py/the DAG full-replace raw_trades on a config
-- change (delete_target_where_sql then re-insert - see README), which an
-- append-only stream sees as brand-new rows, not updates. Without a MERGE
-- keyed on the natural key, a full-replace upstream would silently
-- duplicate rows here instead of overwriting them.
CREATE OR REPLACE PROCEDURE silver.sp_process_new_trades()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    MERGE INTO silver.stg_trades_streaming AS target
    USING (
        SELECT
            ticker,
            "date"::date AS trade_date,
            side,
            quantity,
            price,
            strategy_used,
            signal_strength
        FROM bronze.raw_trades_stream
    ) AS source
    ON target.ticker = source.ticker
       AND target.strategy_used = source.strategy_used
       AND target.trade_date = source.trade_date
    WHEN MATCHED THEN UPDATE SET
        side = source.side,
        quantity = source.quantity,
        price = source.price,
        signal_strength = source.signal_strength
    WHEN NOT MATCHED THEN INSERT (
        ticker, trade_date, side, quantity, price, strategy_used, signal_strength
    ) VALUES (
        source.ticker, source.trade_date, source.side, source.quantity,
        source.price, source.strategy_used, source.signal_strength
    );

    RETURN 'Merged ' || SQLROWCOUNT || ' row(s) from raw_trades_stream';
END;
$$;

-- ── Task ─────────────────────────────────────────────────────────────────
-- WHEN SYSTEM$STREAM_HAS_DATA is what makes this event-driven rather than
-- blind polling: the task still evaluates on the SCHEDULE cadence, but
-- skips the CALL (no warehouse spin-up, no compute cost) whenever the
-- stream is empty. Replace <WAREHOUSE_NAME> with your warehouse - a
-- user-managed warehouse task, not serverless, to reuse the compute you
-- already have rather than provision a second billing path.
CREATE OR REPLACE TASK bronze.process_new_trades_task
    WAREHOUSE = TRADING_PIPELINE_WH
    SCHEDULE = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('bronze.raw_trades_stream')
AS
    CALL silver.sp_process_new_trades();

-- Tasks are created SUSPENDED by default - must be explicitly resumed.
ALTER TASK bronze.process_new_trades_task RESUME;

-- ── Manual verification (optional) ──────────────────────────────────────
-- SELECT SYSTEM$STREAM_HAS_DATA('bronze.raw_trades_stream');
-- CALL silver.sp_process_new_trades();
-- SELECT * FROM silver.stg_trades_streaming ORDER BY trade_date DESC LIMIT 20;
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
--     TASK_NAME => 'process_new_trades_task'
-- )) ORDER BY scheduled_time DESC LIMIT 10;