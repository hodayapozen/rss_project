from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

PROJECT_DIR = "/opt/airflow/scripts"

default_args = {
    "owner": "hodaya",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="rss_etl_pipeline",
    default_args=default_args,
    description="ETL pipeline for RSS feeds: extract → transform/load",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["rss", "etl", "naya_project"],
) as dag:

    extract_rss_to_s3 = BashOperator(
        task_id="extract_rss_to_s3",
        bash_command=f"""
        cd {PROJECT_DIR} &&
        python3 get_xml_upload_s3.py
        """,
    )

    process_and_load_to_mysql = BashOperator(
        task_id="process_and_load_to_mysql",
        bash_command=f"""
        cd {PROJECT_DIR} &&
        python3 process_raw_data_s3.py
        """,
    )

    extract_rss_to_s3 >> process_and_load_to_mysql

