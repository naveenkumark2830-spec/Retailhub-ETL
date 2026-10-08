from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


default_args = {
    "owner": "retailhub",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="retailhub_silver",
    default_args=default_args,
    description="RetailHub Bronze to Silver pipeline",
    schedule="*/10 * * * *",
    start_date=datetime(2026, 9, 8),
    catchup=False,
    max_active_runs=1,
    tags=["retailhub", "silver"],
) as dag:

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command="""
        docker run --rm \
          --env-file /opt/retailhub/.env \
          -e BRONZE_PATH=/opt/retailhub/data/bronze \
          -e SILVER_PATH=/opt/retailhub/data/silver \
          -e QUARANTINE_PATH=/opt/retailhub/data/quarantine/events \
          -v /home/naveen/RetailHub-Spark/data:/opt/retailhub/data \
          retailhub-spark-batch:4.2.0 \
          spark-submit \
          /opt/retailhub/src/silver/bronze_to_silver.py
        """,
    )