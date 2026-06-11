from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="uniswap_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 6, 1),
    schedule="@daily",
    catchup=False,
    tags=["defi", "uniswap"],
) as dag:

    extract_task = BashOperator(
        task_id="extract_swaps",
        bash_command="""
        cd /opt/airflow &&
        python scripts/extract_uniswap_swaps.py
        """,
    )

    transform_task = BashOperator(
        task_id="transform_swaps",
        bash_command="""
        cd /opt/airflow &&
        python scripts/transform_uniswap_swaps.py
        """,
    )

    load_task = BashOperator(
        task_id="load_to_postgres",
        bash_command="""
        cd /opt/airflow &&
        python scripts/load_swaps_to_postgres.py
        """,
    )

    extract_task >> transform_task >> load_task