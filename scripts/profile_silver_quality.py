from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, trim

SILVER_PATH = Path("data/silver")

spark = (
    SparkSession.builder
    .appName("RetailStream-Silver-Quality-Profile")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

def profile_table(table_path: Path):
    table_name = table_path.name

    print("\n" + "=" * 80)
    print(f"TABLE: {table_name}")
    print("=" * 80)

    df = spark.read.parquet(str(table_path))

    total_rows = df.count()

    print(f"\nTotal rows: {total_rows}")
    print(f"Total columns: {len(df.columns)}")

    print("\n--- Schema ---")
    df.printSchema()

    print("\n--- Columns ---")
    for column_name in df.columns:
        print(column_name)

    print("\n--- Null Counts ---")

    null_exprs = [
        count(
            when(
                col(column_name).isNull()
                | (trim(col(column_name).cast("string")) == ""),
                column_name,
            )
        ).alias(column_name)
        for column_name in df.columns
    ]

    null_counts = df.select(null_exprs).collect()[0].asDict()

    for column_name, null_count in null_counts.items():
        print(f"{column_name}: {null_count}")

    print("\n--- Duplicate Rows ---")

    duplicate_count = total_rows - df.dropDuplicates().count()

    print(f"Duplicate rows: {duplicate_count}")

    print("\n--- Sample Data ---")
    df.show(5, truncate=False)

for table_path in sorted(SILVER_PATH.iterdir()):
    if table_path.is_dir():
        profile_table(table_path)

print("\n" + "=" * 80)
print("SILVER QUALITY PROFILING COMPLETED")
print("=" * 80)

spark.stop()