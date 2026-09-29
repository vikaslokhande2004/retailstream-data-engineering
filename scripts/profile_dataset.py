from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

RAW_DIR = Path("data/raw")

spark = (
    SparkSession.builder
    .appName("RetailStreamDatasetProfiling")
    .master("local[*]")
    .getOrCreate()
)

print("=" * 80)
print("RETAILSTREAM DATASET PROFILING")
print("=" * 80)

for file in sorted(RAW_DIR.glob("*.parquet")):
    table_name = file.stem

    print("\n" + "=" * 80)
    print(f"TABLE: {table_name}")
    print("=" * 80)

    df = spark.read.parquet(str(file))

    # Basic information
    print(f"Rows    : {df.count()}")
    print(f"Columns : {len(df.columns)}")

    print("\nColumn names:")
    print(df.columns)

    print("\nSchema:")
    df.printSchema()

    # Null counts
    null_counts = df.select([
        count(
            when(col(c).isNull(), c)
        ).alias(c)
        for c in df.columns
    ]).collect()[0].asDict()

    print("\nNull counts:")
    has_nulls = False

    for column_name, null_count in null_counts.items():
        if null_count > 0:
            print(f"  {column_name}: {null_count}")
            has_nulls = True

    if not has_nulls:
        print("  No null values found.")

    # Duplicate count
    duplicate_count = df.count() - df.dropDuplicates().count()

    print(f"\nDuplicate rows: {duplicate_count}")

    # Sample data
    print("\nSample records:")
    df.show(5,truncate=False)

print("\n" + "=" * 80)
print("PROFILING COMPLETED")
print("=" * 80)

spark.stop()