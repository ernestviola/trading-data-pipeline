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
from main import step_2

default_args = {"retries": 3, "retry_delay": timedelta(minutes=5)}

TICKERS = ["AAPL"]
STARTING_CASH = 10000
BASE_POSITION_SIZE = 500
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


def compute_and_load_trades():
    step_2(TICKERS, STARTING_CASH, BASE_POSITION_SIZE, MAX_MULTIPLIER, SHARES_HELD)


def new_trades_landed(**context) -> bool:
    # raw_trades is Bronze; transformer_role has read-only access there, so
    # reuse that role rather than opening a third role for one query.
    conn = snowflake_connection(role="transformer_role")
    try:
        cs = conn.cursor()
        cs.execute(
            'select count(*) from bronze.raw_prices where "timestamp" >= %s and "timestamp" < %s',
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

    compute_and_load_trades_task = PythonOperator(
        task_id="compute_and_load_trades", python_callable=compute_and_load_trades
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

    (pull_prices_task >> compute_and_load_trades_task >> check_new_trades >> run_dbt)
