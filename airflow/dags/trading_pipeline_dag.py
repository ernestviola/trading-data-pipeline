from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator
from gather_historicals import gather_historicals
from load_to_database import load_csv_to_postgres
from strategies import STRATEGIES
from strategies.configs import MeanReversionConfig
from utils.postgres_connection import postgres_connection

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
        load_csv_to_postgres(
            "raw_prices",
            "on target.symbol = source.symbol AND target.timestamp = source.timestamp",
            csv_path,
        )


def generate_trades(**context):
    conn = postgres_connection()
    try:
        cs = conn.cursor()

        cs.execute(
            "select coalesce((select cash_after from dbt_dev.cash_position where is_current = true), %s)",
            (STARTING_CASH,),
        )
        cash_on_hand = cs.fetchone()[0]

        strategy_fn, _ = STRATEGIES[STRATEGY]
        config = MeanReversionConfig(
            window=WINDOW,
            buy_strength_threshold=Z_THRESHOLD,
            sell_strength_threshold=Z_THRESHOLD,
        )
        for ticker in TICKERS:
            cs.execute(
                """
                select coalesce(
                    (select shares_held from dbt_dev.holdings_scd2 where is_current = true and ticker = %s),
                    %s
                )
                """,
                (ticker, SHARES_HELD),
            )
            shares_held = cs.fetchone()[0]

            csv_path = strategy_fn(
                ticker,
                cash_on_hand,
                BASE_POSITION_SIZE,
                MAX_MULTIPLIER,
                shares_held,
                strategy_used=STRATEGY,
                config=config,
            )
            load_csv_to_postgres(
                "raw_trades",
                "on target.ticker = source.ticker and target.strategy_used = source.strategy_used "
                "and target.date = source.date",
                csv_path,
                # generate_trades() recomputes this strategy's entire trade
                # history every run (same underlying strategy functions as
                # main.py) - replace rows outright rather than only
                # inserting missing dates, so a config change actually takes
                # effect. Mirrors main.py's load_csv_to_snowflake() call;
                # assumes load_csv_to_postgres eventually gets the same
                # delete_target_where_sql/delete_target_params support once
                # this DAG's Postgres->Snowflake migration happens.
                delete_target_where_sql="WHERE strategy_used = %s AND ticker = %s",
                delete_target_params=(STRATEGY, ticker),
            )
    finally:
        conn.close()


def new_trades_landed(**context) -> bool:
    conn = postgres_connection()
    try:
        cs = conn.cursor()
        cs.execute(
            "select count(*) from raw_trades where date >= %s and date < %s",
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

    generate_trades_task = PythonOperator(
        task_id="generate_trades", python_callable=generate_trades
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

    pull_prices_task >> generate_trades_task >> check_new_trades >> run_dbt
