from datetime import datetime, timedelta
import time

import boto3

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.python import PythonOperator


# ============================================================
# AWS CONFIGURATION
# ============================================================

AWS_REGION = "ap-south-1"

BATCH_INSTANCE_ID = "i-075044a1c7a02e50f"

SSM_DOCUMENT = "AWS-RunShellScript"


# ============================================================
# DEFAULT AIRFLOW SETTINGS
# ============================================================

default_args = {
    "owner": "retailhub",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


# ============================================================
# SILVER SPARK COMMAND
# ============================================================


def run_silver_on_batch():

    ssm = boto3.client(
        "ssm",
        region_name=AWS_REGION,
    )

    command = r"""
docker run --rm \
  --env-file /opt/retailhub-batch/.env \
  -e BRONZE_PATH=s3a://retailhub-bronze-252bda \
  -e SILVER_PATH=s3a://retailhub-silver-252bda \
  -e QUARANTINE_PATH=s3a://retailhub-quarantine-252bda/events \
  naveen9200/retailhub-spark-batch:4.2.0 \
  spark-submit \
  /opt/retailhub/src/silver/bronze_to_silver.py
"""

    response = ssm.send_command(
        InstanceIds=[BATCH_INSTANCE_ID],
        DocumentName=SSM_DOCUMENT,
        Parameters={
            "commands": [command]
        },
        Comment="RetailHub Silver Spark Job",
    )

    command_id = response["Command"]["CommandId"]

    print(f"SSM Command ID: {command_id}")

    # --------------------------------------------------------
    # Wait for SSM invocation
    # --------------------------------------------------------

    while True:

        try:

            result = ssm.get_command_invocation(
                CommandId=command_id,
                InstanceId=BATCH_INSTANCE_ID,
            )

            status = result["Status"]

            print(f"SSM Status: {status}")

            if status in [
                "Success",
                "Failed",
                "Cancelled",
                "TimedOut",
                "Undeliverable",
                "Terminated",
            ]:
                break

        except ssm.exceptions.InvocationDoesNotExist:

            print(
                "SSM invocation not available yet. "
                "Waiting 5 seconds..."
            )

        time.sleep(5)

    # --------------------------------------------------------
    # Print Spark output
    # --------------------------------------------------------

    stdout = result.get("StandardOutputContent", "")
    stderr = result.get("StandardErrorContent", "")

    if stdout:
        print("========== SPARK OUTPUT ==========")
        print(stdout)

    if stderr:
        print("========== SPARK ERROR ==========")
        print(stderr)

    # --------------------------------------------------------
    # Fail Airflow if Spark failed
    # --------------------------------------------------------

    if status != "Success":

        raise AirflowException(
            f"RetailHub Silver Spark job failed. "
            f"SSM status: {status}. "
            f"Error: {stderr}"
        )

    print(
        "RetailHub Silver Spark job completed successfully."
    )



# ============================================================
# AIRFLOW DAG
# ============================================================

with DAG(
    dag_id="retailhub_silver",

    default_args=default_args,

    description="RetailHub Bronze to Silver pipeline",

    schedule="*/10 * * * *",

    start_date=datetime(2026, 9, 8),

    catchup=False,

    max_active_runs=1,

    tags=[
        "retailhub",
        "silver",
        "aws",
        "ssm",
        "spark",
    ],

) as dag:

    bronze_to_silver = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=run_silver_on_batch,
    )