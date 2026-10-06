import os
from pathlib import Path

from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()

SILVER_PATH = Path("data/silver")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "retail_dw")
POSTGRES_USER = os.getenv("POSTGRES_USER", "retailadmin")
POSTGRES_PASSWORD = os.getenv(
    "POSTGRES_PASSWORD",
    "retailstream123"
)

JDBC_URL = (
    f"jdbc:postgresql://"
    f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
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
    .config(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.7.8"
    )
    .config(
        "spark.driver.extraJavaOptions",
        "-Duser.timezone=Asia/Kolkata"
    )
    .config(
        "spark.executor.extraJavaOptions",
        "-Duser.timezone=Asia/Kolkata"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

def load_table(table_path: Path):
    table_name = table_path.name

    print("\n" + "=" * 70)
    print(f"LOADING TABLE: {table_name}")
    print("=" * 70)

    df = spark.read.parquet(str(table_path))

    row_count = df.count()

    print(f"Rows: {row_count}")
    print(f"Columns: {len(df.columns)}")

    (
            df.write
            .mode("overwrite")
            .jdbc(
                url=JDBC_URL,
                table=f"staging.{table_name}",
                properties=JDBC_PROPERTIES
            )
        )

    print(
            f"Successfully loaded staging.{table_name}"
        )

for table_path in sorted(SILVER_PATH.iterdir()):
    if table_path.is_dir():
        load_table(table_path)

print("\n" + "=" * 70)
print("SILVER TO POSTGRESQL LOAD COMPLETED")
print("=" * 70)

spark.stop()