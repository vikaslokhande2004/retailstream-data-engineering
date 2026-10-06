from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

RAW_DIR = Path("data/raw")

spark = (
    SparkSession.builder
    .appName("RetailStreamRelationshipProfiling")
    .master("local[*]")
    .getOrCreate()
)

tables = {}

for file in sorted(RAW_DIR.glob("*.parquet")):
    table_name = file.stem
    tables[table_name] = spark.read.parquet(str(file))

# print(tables)
print("=" * 80)
print("RETAILSTREAM TABLE RELATIONSHIP PROFILING")
print("=" * 80)

for name, df in tables.items():
    print(f"\n{name}")
    print("-" * 40)
    print("Columns:")
    for column_name in df.columns:
        print(f"  {column_name}")

print("\n" + "=" * 80)
print("COMMON KEY COLUMNS")
print("=" * 80)

all_columns = {}

for table_name, df in tables.items():
    for column_name in df.columns:
        all_columns.setdefault(column_name, []).append(table_name)

for column_name, table_names in sorted(all_columns.items()):
    if len(table_names) > 1:
        print(f"{column_name}: {', '.join(table_names)}")

# print(all_columns)

print("\n" + "=" * 80)
print("PRIMARY KEY CANDIDATE ANALYSIS")
print("=" * 80)

for table_name, df in tables.items():
    print(f"\nTABLE: {table_name}")

    for column_name in df.columns:
        if column_name.lower().endswith("_id") or column_name.lower() == "id":
            total = df.count()
            distinct = df.select(column_name).distinct().count()
            nulls = df.filter(col(column_name).isNull()).count()

            print(
                            f"  {column_name}: "
                            f"rows={total}, "
                            f"distinct={distinct}, "
                            f"nulls={nulls}"
                        )

print("\n" + "=" * 80)
print("RELATIONSHIP PROFILING COMPLETED")
print("=" * 80)

spark.stop()