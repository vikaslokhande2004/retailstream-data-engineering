
import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

SILVER_PATH = Path("data/silver")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "retail_dw")
POSTGRES_USER = os.getenv("POSTGRES_USER", "retailadmin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "retailstream123")

JDBC_URL = (
    f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

JDBC_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}

spark = (
    SparkSession.builder
    .appName("RetailStream-Silver-to-PostgreSQL")
    .master("local[*]")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.8")
    .config("spark.driver.extraJavaOptions", "-Duser.timezone=Asia/Kolkata")
    .config("spark.executor.extraJavaOptions", "-Duser.timezone=Asia/Kolkata")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


def load_table(table_path):
    table_name = table_path.name

    print("\n" + "=" * 70)
    print(f"LOADING TABLE: {table_name}")
    print("=" * 70)

    df = spark.read.parquet(str(table_path))
    row_count = df.count()

    print(f"Rows: {row_count:,}")
    print(f"Columns: {len(df.columns)}")

    temp_table = f"_load_{table_name}_temp"

    # Load into a temporary-purpose table without touching the target table.
    (
        df.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", f'staging."{temp_table}"')
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )

    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )

    try:
        with conn:
            with conn.cursor() as cur:
                # Preserve the existing table so dependent dbt views survive.
                cur.execute(
                    f'TRUNCATE TABLE staging."{table_name}"'
                )
                cur.execute(
                    f'INSERT INTO staging."{table_name}" '
                    f'SELECT * FROM staging."{temp_table}"'
                )
                cur.execute(
                    f'DROP TABLE staging."{temp_table}"'
                )

    finally:
        conn.close()

    print(
        f"Successfully loaded {row_count:,} rows "
        f"into staging.{table_name}"
    )


try:
    for table_path in sorted(SILVER_PATH.iterdir()):
        if table_path.is_dir():
            load_table(table_path)

    print("\n" + "=" * 70)
    print("SILVER TO POSTGRESQL LOAD COMPLETED")
    print("=" * 70)

finally:
    spark.stop()
