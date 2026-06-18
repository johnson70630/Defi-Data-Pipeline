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

    upload_raw_to_s3_task = BashOperator(
    task_id="upload_raw_to_s3",
    bash_command="""
    cd /opt/airflow &&
    python scripts/upload_raw_to_s3.py
    """,
    )

    transform_task = BashOperator(
        task_id="transform_swaps",
        bash_command="""
        cd /opt/airflow &&
        python scripts/transform_uniswap_swaps.py
        """,
    )

    # load_task = BashOperator(
    #     task_id="load_to_postgres",
    #     bash_command="""
    #     cd /opt/airflow &&
    #     python scripts/load_swaps_to_postgres.py
    #     """,
    # )

    load_snowflake_task = BashOperator(
    task_id="load_to_snowflake",
    bash_command="""
    cd /opt/airflow &&
    python scripts/load_swaps_to_snowflake.py
    """,
    )

    dbt_run_task = BashOperator(
        task_id="dbt_run",
        bash_command="""
        cd /opt/airflow/defi_dbt &&
        dbt run
        """,
    )

    dbt_test_task = BashOperator(
        task_id="dbt_test",
        bash_command="""
        cd /opt/airflow/defi_dbt &&
        dbt test
        """,
    )

    extract_task >> upload_raw_to_s3_task >> transform_task >> load_snowflake_task >> dbt_run_task >> dbt_test_task