from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


default_args = {
    "owner": "retailhub",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def spark_task(task_id, script):
    return BashOperator(
        task_id=task_id,
        bash_command=f"""
        docker run --rm \
          --env-file /opt/retailhub/.env \
          -e SILVER_PATH=/opt/retailhub/data/silver \
          -e GOLD_PATH=/opt/retailhub/data/gold \
          -e MYSQL_HOST=host.docker.internal \
          -e MYSQL_PORT=3306 \
          -v /home/naveen/RetailHub-Spark/data:/opt/retailhub/data \
          retailhub-spark-batch:4.2.0 \
          spark-submit \
          /opt/retailhub/src/gold/{script}
        """,
    )


with DAG(
    dag_id="retailhub_gold",
    default_args=default_args,
    description="RetailHub Silver to Gold pipeline",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 9, 8),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=3,
    tags=["retailhub", "gold"],
) as dag:

    fact_orders = spark_task("fact_orders", "fact_orders.py")
    fact_order_items = spark_task("fact_order_items", "fact_order_items.py")
    fact_payments = spark_task("fact_payments", "fact_payments.py")
    fact_delivery = spark_task("fact_delivery", "fact_delivery.py")
    fact_product_interactions = spark_task(
        "fact_product_interactions",
        "fact_product_interactions.py",
    )

    dim_customer = spark_task("dim_customer", "dim_customer.py")
    dim_product = spark_task("dim_product", "dim_product.py")
    dim_category = spark_task("dim_category", "dim_category.py")
    dim_date = spark_task("dim_date", "dim_date.py")