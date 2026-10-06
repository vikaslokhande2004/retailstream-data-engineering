from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim

BRONZE_DIR = Path("data/bronze")
SILVER_DIR = Path("data/silver")


spark = (
    SparkSession.builder
    .appName("RetailStreamSilverTransformation")
    .master("local[*]")
    .getOrCreate()
)

def clean_string_columns(df):
    # Trim whitespace from string columns.

    for filed in df.schema.fields:
        if filed.dataType.simpleString() == "string":
            df = df.withColumn(
                filed.name,
                trim(col(filed.name))
            )
    return df

def process_table(table_name):
    input_path = BRONZE_DIR / table_name
    output_path = SILVER_DIR / table_name

    print("\n" + "=" * 70)
    print(f"PROCESSING TABLE: {table_name}")
    print("=" * 70)

    df = spark.read.parquet(str(input_path))

    input_count = df.count()
    
    print(f"Input rows: {input_count:,}")

    # 1. Remove completely duplicate rows
    df = df.dropDuplicates()

    after_duplicate_count = df.count()

    print(f"Duplicates removed: "f"{input_count - after_duplicate_count:,}")

    # 2. Trim strig columns
    df = clean_string_columns(df)

    # 3. Remove completely empty records
    df = df.dropna(how="all")

    # 4. Write Silver data
    (
        df.write
        .mode("overwrite")
        .parquet(str(output_path))
    )

    final_count = df.count()

    print(f"Silver rows: {final_count:,}")
    print(f"Output: {output_path}")

def main():

    print("=" * 70)
    print("RETAILSTREAM SILVER TRANSFORMATION")
    print("=" * 70)

    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    tables = sorted(
        path.name
        for path in BRONZE_DIR.iterdir()
        if path.is_dir()
    )

    print(f"Tables found: {len(tables)}")

    for table_name in tables:
        process_table(table_name)

    spark.stop()

    print("\n" + "=" * 70)
    print("SILVER TRANSFORMATION COMPLETED")
    print("=" * 70)

if __name__ == "__main__":
    main()