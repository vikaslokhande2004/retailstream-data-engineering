from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import psycopg2


DB_CONFIG = {
    "host": "host.docker.internal",
    "port": 5432,
    "database": "retail_dw",
    "user": "retailadmin",
    "password": "retailstream123",
}


def check_database():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("SELECT version();")
    version = cursor.fetchone()[0]

    print("PostgreSQL connection successful")
    print(f"Database version: {version}")

    cursor.close()
    conn.close()


def validate_staging():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    tables = [
        "categories",
        "customers",
        "order_items",
        "orders",
        "payments",
        "products",
        "returns",
    ]

    for table in tables:
        cursor.execute(
            f"SELECT COUNT(*) FROM staging.{table};"
        )

        count = cursor.fetchone()[0]

        print(f"staging.{table}: {count:,} rows")

        if count == 0:
            raise ValueError(f"staging.{table} is empty")

    cursor.close()
    conn.close()


def validate_analytics():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    tables = [
        "dim_category",
        "dim_customer",
        "dim_product",
        "dim_date",
        "fact_orders",
        "fact_sales",
        "fact_payments",
        "fact_returns",
    ]

    for table in tables:
        cursor.execute(
            f"SELECT COUNT(*) FROM analytics.{table};"
        )

        count = cursor.fetchone()[0]

        print(f"analytics.{table}: {count:,} rows")

        if count == 0:
            raise ValueError(f"analytics.{table} is empty")

    cursor.close()
    conn.close()


with DAG(
    dag_id="retailstream_pipeline",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=["retailstream", "data-engineering"],
) as dag:

    check_database_task = PythonOperator(
        task_id="check_database",
        python_callable=check_database,
    )

    validate_staging_task = PythonOperator(
        task_id="validate_staging",
        python_callable=validate_staging,
    )

    dbt_run_task = BashOperator(
        task_id="dbt_run",
        bash_command="""
        cd /opt/airflow/dbt/retailstream_dbt
        dbt run --profiles-dir /home/airflow/.dbt
        """,
    )

    dbt_test_task = BashOperator(
        task_id="dbt_test",
        bash_command="""
        cd /opt/airflow/dbt/retailstream_dbt
        dbt test --profiles-dir /home/airflow/.dbt
        """,
    )

    validate_analytics_task = PythonOperator(
        task_id="validate_analytics",
        python_callable=validate_analytics,
    )

    (
        check_database_task
        >> validate_staging_task
        >> dbt_run_task
        >> dbt_test_task
        >> validate_analytics_task
    )