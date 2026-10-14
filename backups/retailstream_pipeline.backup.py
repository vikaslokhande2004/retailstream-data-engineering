
from datetime import datetime

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


DB_CONFIG = {
    "host": "host.docker.internal",
    "port": 5432,
    "database": "retail_dw",
    "user": "retailadmin",
    "password": "retailstream123",
}

PIPELINE_IMAGE = "retailstream-pipeline:latest"

# Docker Desktop host path confirmed from the existing data bind mount.
DATA_SOURCE = (
    "/run/desktop/mnt/host/d/retailstream-data-engineering/data"
)

PIPELINE_ENV = {
    "RUSTFS_ENDPOINT": "http://host.docker.internal:19000",
    "POSTGRES_HOST": "host.docker.internal",
    "POSTGRES_PORT": "5432",
    "POSTGRES_DB": "retail_dw",
    "POSTGRES_USER": "retailadmin",
    "POSTGRES_PASSWORD": "retailstream123",
}


def check_database():
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT version();")
            print("PostgreSQL connection successful")
            print(cursor.fetchone()[0])


def validate_staging():
    tables = [
        "categories",
        "customers",
        "order_items",
        "orders",
        "payments",
        "products",
        "returns",
    ]

    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cursor:
            for table in tables:
                cursor.execute(
                    f"SELECT COUNT(*) FROM staging.{table};"
                )
                count = cursor.fetchone()[0]
                print(f"staging.{table}: {count:,} rows")

                if count == 0:
                    raise ValueError(
                        f"staging.{table} is empty"
                    )


def validate_analytics():
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

    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cursor:
            for table in tables:
                cursor.execute(
                    f"SELECT COUNT(*) FROM analytics.{table};"
                )
                count = cursor.fetchone()[0]
                print(f"analytics.{table}: {count:,} rows")

                if count == 0:
                    raise ValueError(
                        f"analytics.{table} is empty"
                    )


def create_pipeline_task(task_id, script_name):
    return DockerOperator(
        task_id=task_id,
        image=PIPELINE_IMAGE,
        command=["python", f"scripts/{script_name}"],
        docker_url="unix://var/run/docker.sock",
        working_dir="/opt/retailstream",
        mounts=[
            Mount(
                source=DATA_SOURCE,
                target="/opt/retailstream/data",
                type="bind",
            )
        ],
        environment=PIPELINE_ENV,
        mount_tmp_dir=False,
        auto_remove="success",
    )


with DAG(
    dag_id="retailstream_pipeline",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=["retailstream", "data-engineering", "pyspark", "dbt"],
) as dag:

    check_database_task = PythonOperator(
        task_id="check_database",
        python_callable=check_database,
    )

    ingest_bronze_task = create_pipeline_task(
        "ingest_bronze",
        "ingest_bronze.py",
    )

    transform_silver_task = create_pipeline_task(
        "transform_silver",
        "transform_silver.py",
    )

    validate_silver_task = create_pipeline_task(
        "validate_silver",
        "validate_silver.py",
    )

    load_postgres_task = create_pipeline_task(
        "load_silver_postgres",
        "load_silver_postgres.py",
    )

    validate_staging_task = PythonOperator(
        task_id="validate_staging",
        python_callable=validate_staging,
    )

    dbt_run_task = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd /opt/airflow/dbt/retailstream_dbt && "
            "dbt run --profiles-dir /home/airflow/.dbt"
        ),
    )

    dbt_test_task = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /opt/airflow/dbt/retailstream_dbt && "
            "dbt test --profiles-dir /home/airflow/.dbt"
        ),
    )

    validate_analytics_task = PythonOperator(
        task_id="validate_analytics",
        python_callable=validate_analytics,
    )

    (
        check_database_task
        >> ingest_bronze_task
        >> transform_silver_task
        >> validate_silver_task
        >> load_postgres_task
        >> validate_staging_task
        >> dbt_run_task
        >> dbt_test_task
        >> validate_analytics_task
    )
