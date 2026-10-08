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
# DEFAULT ARGUMENTS
# ============================================================

default_args = {
    "owner": "retailhub",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


# ============================================================
# RUN GOLD SPARK JOB ON BATCH EC2
# ============================================================

def run_gold_on_batch(script):

    ssm = boto3.client(
        "ssm",
        region_name=AWS_REGION,
    )

    # --------------------------------------------------------
    # Docker command
    #
    # All RDS variables are loaded from:
    # /opt/retailhub-batch/.env
    #
    # Therefore dim_customer.py also receives:
    # MYSQL_HOST
    # MYSQL_PORT
    # MYSQL_DATABASE
    # MYSQL_USER
    # MYSQL_PASSWORD
    # --------------------------------------------------------

    command = f"""
docker run --rm \
  --env-file /opt/retailhub-batch/.env \
  -e SILVER_PATH=s3a://retailhub-silver-252bda \
  -e GOLD_PATH=s3a://retailhub-gold-252bda \
  naveen9200/retailhub-spark-batch:4.2.0 \
  spark-submit \
  /opt/retailhub/src/gold/{script}
"""

    # --------------------------------------------------------
    # Send command to Batch EC2
    # --------------------------------------------------------

    response = ssm.send_command(
        InstanceIds=[BATCH_INSTANCE_ID],
        DocumentName=SSM_DOCUMENT,
        Parameters={
            "commands": [command]
        },
        Comment=f"RetailHub Gold Spark Job - {script}",
    )

    command_id = response["Command"]["CommandId"]

    print("=" * 70)
    print("RETAILHUB GOLD JOB")
    print("=" * 70)
    print(f"Script      : {script}")
    print(f"SSM Command : {command_id}")
    print("=" * 70)

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

            # ------------------------------------------------
            # Terminal states
            # ------------------------------------------------

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
    # Capture output
    # --------------------------------------------------------

    stdout = result.get(
        "StandardOutputContent",
        "",
    )

    stderr = result.get(
        "StandardErrorContent",
        "",
    )

    # --------------------------------------------------------
    # Print Spark output
    # --------------------------------------------------------

    if stdout:

        print("=" * 70)
        print("SPARK OUTPUT")
        print("=" * 70)

        print(stdout)

    # --------------------------------------------------------
    # Print Spark errors
    # --------------------------------------------------------

    if stderr:

        print("=" * 70)
        print("SPARK ERROR")
        print("=" * 70)

        print(stderr)

    # --------------------------------------------------------
    # Fail Airflow task if SSM/Spark failed
    # --------------------------------------------------------

    if status != "Success":

        raise AirflowException(
            f"""
RetailHub Gold Spark job failed.

Script:
{script}

SSM Status:
{status}

Error:
{stderr}
"""
        )

    # --------------------------------------------------------
    # Success
    # --------------------------------------------------------

    print("=" * 70)
    print(f"SUCCESS: {script}")
    print("=" * 70)


# ============================================================
# GOLD DAG
# ============================================================

with DAG(

    dag_id="retailhub_gold",

    default_args=default_args,

    description="RetailHub Silver to Gold pipeline",

    schedule="*/15 * * * *",

    start_date=datetime(
        2026,
        9,
        8,
    ),

    catchup=False,

    max_active_runs=1,

    # Only 3 Spark jobs at a time
    max_active_tasks=3,

    tags=[
        "retailhub",
        "gold",
        "aws",
        "ssm",
        "spark",
    ],

) as dag:

    # ========================================================
    # FACT TABLES
    # ========================================================

    fact_orders = PythonOperator(
        task_id="fact_orders",

        python_callable=run_gold_on_batch,

        op_kwargs={
            "script": "fact_orders.py",
        },
    )

    fact_order_items = PythonOperator(
        task_id="fact_order_items",

        python_callable=run_gold_on_batch,

        op_kwargs={
            "script": "fact_order_items.py",
        },
    )

    fact_payments = PythonOperator(
        task_id="fact_payments",

        python_callable=run_gold_on_batch,

        op_kwargs={
            "script": "fact_payments.py",
        },
    )

    fact_delivery = PythonOperator(
        task_id="fact_delivery",

        python_callable=run_gold_on_batch,

        op_kwargs={
            "script": "fact_delivery.py",
        },
    )

    fact_product_interactions = PythonOperator(
        task_id="fact_product_interactions",

        python_callable=run_gold_on_batch,

        op_kwargs={
            "script": "fact_product_interactions.py",
        },
    )

    # ========================================================
    # DIMENSION TABLES
    # ========================================================

    dim_customer = PythonOperator(
        task_id="dim_customer",

        python_callable=run_gold_on_batch,

        op_kwargs={
            "script": "dim_customer.py",
        },
    )

    dim_product = PythonOperator(
        task_id="dim_product",

        python_callable=run_gold_on_batch,

        op_kwargs={
            "script": "dim_product.py",
        },
    )

    dim_category = PythonOperator(
        task_id="dim_category",

        python_callable=run_gold_on_batch,

        op_kwargs={
            "script": "dim_category.py",
        },
    )

    dim_date = PythonOperator(
        task_id="dim_date",

        python_callable=run_gold_on_batch,

        op_kwargs={
            "script": "dim_date.py",
        },
    )