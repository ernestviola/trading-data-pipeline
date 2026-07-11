from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator
from gather_historicals import gather_historicals
from load_to_database import load_csv_to_snowflake
from strategies import STRATEGIES
from strategies.configs import MeanReversionConfig
from utils.snowflake_connection import snowflake_connection

default_args = {"retries": 3, "retry_delay": timedelta(minutes=5)}

TICKERS = ["AAPL"]

STRATEGY = "mean_reversion"

WINDOW = 20
STARTING_CASH = 10000
BASE_POSITION_SIZE = 500
Z_THRESHOLD = 1.5
MAX_MULTIPLIER = 3
SHARES_HELD = 0


def pull_prices(**context):
    start = context["data_interval_start"]
    end = context["data_interval_end"]

    for ticker in TICKERS:
        csv_path = gather_historicals(ticker, start, end)
        load_csv_to_snowflake(
            "raw_prices",
            'on target.symbol = source.symbol AND target."timestamp" = source."timestamp"',
            csv_path,
        )


def compute_trades(ticker: str, **context):
    # Dynamically mapped per ticker (see .expand() below) - each mapped
    # instance only reads, no shared state, so this is safe to parallelize
    # unlike load_trades below. transformer_role: same reasoning as
    # new_trades_landed - Gold read/write + Bronze read-only.
    #
    # cash_on_hand/shares_held are scoped to (ticker, STRATEGY), not just
    # ticker - cash_position/holdings_scd2 are partitioned by
    # (strategy_used, ticker), so each combination has its own independent
    # balance. The prior single-connection generate_trades() only filtered
    # shares_held by ticker and didn't filter cash_on_hand at all - masked
    # by TICKERS having exactly one entry and STRATEGY being a single
    # hardcoded value; fixed here since it stops being safe once either
    # generalizes (see README's "run all strategies for every ticker" item).
    conn = snowflake_connection(role="transformer_role")
    try:
        cs = conn.cursor()

        cs.execute(
            """
            select coalesce(
                (select cash_after from gold.cash_position
                 where is_current = true and ticker = %s and strategy_used = %s),
                %s
            )
            """,
            (ticker, STRATEGY, STARTING_CASH),
        )
        cash_on_hand = cs.fetchone()[0]

        cs.execute(
            """
            select coalesce(
                (select shares_held from gold.holdings_scd2
                 where is_current = true and ticker = %s and strategy_used = %s),
                %s
            )
            """,
            (ticker, STRATEGY, SHARES_HELD),
        )
        shares_held = cs.fetchone()[0]
    finally:
        conn.close()

    strategy_fn, _ = STRATEGIES[STRATEGY]
    config = MeanReversionConfig(
        window=WINDOW,
        buy_strength_threshold=Z_THRESHOLD,
        sell_strength_threshold=Z_THRESHOLD,
    )
    csv_path = strategy_fn(
        ticker,
        cash_on_hand,
        BASE_POSITION_SIZE,
        MAX_MULTIPLIER,
        shares_held,
        strategy_used=STRATEGY,
        config=config,
    )

    # ticker travels with the path in the XCom payload - load_trades needs
    # it for delete_target_where_sql and can't recover it from the CSV path
    # alone. Path isn't JSON-serializable for XCom, so stringify it.
    return {"ticker": ticker, "csv_path": str(csv_path)}


def load_trades(**context):
    # Single sequential task, not mapped - all mapped compute_trades
    # instances write through the same raw_trades_staging table, and
    # Snowflake's concurrent-DML conflict detection works at the
    # micro-partition level, not exact row level, so truly concurrent
    # loads against that shared staging table can still collide even with
    # scoped DELETEs. loader_role: same write path as before.
    results = context["ti"].xcom_pull(task_ids="compute_trades")
    for result in results:
        load_csv_to_snowflake(
            "raw_trades",
            "on target.ticker = source.ticker and target.strategy_used = source.strategy_used "
            'and target."date" = source."date"',
            Path(result["csv_path"]),
            # load_trades recomputes this strategy's entire trade history
            # every run (same underlying strategy functions as main.py) -
            # replace rows outright rather than only inserting missing
            # dates, so a config change actually takes effect. Mirrors
            # main.py's load_csv_to_snowflake() call.
            delete_target_where_sql="WHERE strategy_used = %s AND ticker = %s",
            delete_target_params=(STRATEGY, result["ticker"]),
        )


def new_trades_landed(**context) -> bool:
    # raw_trades is Bronze; transformer_role has read-only access there, so
    # reuse that role rather than opening a third role for one query.
    conn = snowflake_connection(role="transformer_role")
    try:
        cs = conn.cursor()
        cs.execute(
            'select count(*) from bronze.raw_trades where "date" >= %s and "date" < %s',
            (context["data_interval_start"], context["data_interval_end"]),
        )
        count = cs.fetchone()[0]
        return count > 0
    finally:
        conn.close()


with DAG(
    dag_id="trading_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
) as dag:
    pull_prices_task = PythonOperator(
        task_id="pull_prices", python_callable=pull_prices
    )

    compute_trades_task = PythonOperator.partial(
        task_id="compute_trades", python_callable=compute_trades
    ).expand(op_kwargs=[{"ticker": ticker} for ticker in TICKERS])

    load_trades_task = PythonOperator(
        task_id="load_trades", python_callable=load_trades
    )

    check_new_trades = ShortCircuitOperator(
        task_id="check_new_trades", python_callable=new_trades_landed
    )

    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command=(
            "/opt/dbt_venv/bin/dbt run "
            "--project-dir /opt/airflow/dbt "
            "--profiles-dir /opt/airflow/dbt"
        ),
    )

    (
        pull_prices_task
        >> compute_trades_task
        >> load_trades_task
        >> check_new_trades
        >> run_dbt
    )
