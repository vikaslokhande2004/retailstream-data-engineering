from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, lit, current_timestamp

SILVER_PATH = Path("data/silver")
QUALITY_PATH = Path("data/gold/data_quality")
# Columns where NULL values are valid business data
OPTIONAL_NULL_COLUMNS = {
    "categories": ["parent_category_id"],
    "customers": ["loyalty_tier"],
    "orders": ["shipped_at", "coupon_code"],
    "payments": ["failure_reason"],
    "products": ["discontinued_at"],
}

spark = (
    SparkSession.builder.appName("RetailStream-Silver-Data-Quality")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

results = []


def add_result(table, check, status, total, failed):
    results.append((table, check, status, total, failed))


def validate_table(table_path):

    table_name = table_path.name

    print("\n" + "=" * 80)
    print(f"VALIDATING: {table_name}")
    print("=" * 80)

    df = spark.read.parquet(str(table_path))

    total_rows = df.count()

    print(f"Total rows: {total_rows}")

    # ---------------------------------------------------------
    # CHECK 1: Empty table
    # ---------------------------------------------------------

    if total_rows == 0:
        add_result(table_name, "table_not_empty", "FAIL", total_rows, 0)
        print("FAIL: Table is empty")
    else:
        add_result(table_name, "table_not_empty", "PASS", total_rows, 0)
        print("PASS: Table contains records")

    # ---------------------------------------------------------
    # CHECK 2: Duplicate rows
    # ---------------------------------------------------------

    distinct_rows = df.dropDuplicates().count()
    duplicate_rows = total_rows - distinct_rows

    if duplicate_rows > 0:
        status = "FAIL"
        print(f"FAIL: Duplicate rows = {duplicate_rows}")
    else:
        status = "PASS"
        print("PASS: No dupliacte rows")

    add_result(table_name, "duplicate_rows", status, total_rows, duplicate_rows)

    # ---------------------------------------------------------
    # CHECK 3: Null values
    # ---------------------------------------------------------

    optional_columns = OPTIONAL_NULL_COLUMNS.get(table_name, [])

    for column_name in df.columns:
        null_count = df.filter(col(column_name).isNull()).count()

        if null_count > 0 and column_name not in optional_columns:
            status = "FAIL"
            print(f"FAIL: {column_name} has {null_count} null values")
        else:
            status = "PASS"

            if null_count > 0:
                print(f"PASS: {column_name} has " f"{null_count} allowed null values")

        add_result(
            table_name, f"null_check:{column_name}", status, total_rows, null_count
        )
    # ---------------------------------------------------------
    # CHECK 4: Blank string values
    # ---------------------------------------------------------

    string_columns = [
        field.name
        for field in df.schema.fields
        if field.dataType.simpleString() == "string"
    ]

    for column_name in string_columns:
        black_count = df.filter(
            col(column_name).isNotNull() & (col(column_name) == "")
        ).count()

        if black_count > 0:
            status = "FAIL"
            print(f"FAIL: {column_name} has " f"{black_count} blank values")
        else:
            status = "PASS"

        add_result(
            table_name, f"blank_check:{column_name}", status, total_rows, black_count
        )


for table_path in sorted(SILVER_PATH.iterdir()):
    if table_path.is_dir():
        validate_table(table_path)

# -------------------------------------------------------------
# CREATE DATA QUALITY AUDIT DATAFRAME
# -------------------------------------------------------------

result_schema = [
    "table_name",
    "check_name",
    "status",
    "total_records",
    "failed_records",
]

quality_df = spark.createDataFrame(results, result_schema)

quality_df = quality_df.withColumn("run_timestamp", current_timestamp())

# -------------------------------------------------------------
# WRITE QUALITY RESULTS
# -------------------------------------------------------------

QUALITY_PATH.mkdir(parents=True, exist_ok=True)

quality_df.write.mode("overwrite").parquet(str(QUALITY_PATH))


# -------------------------------------------------------------
# DISPLAY SUMMARY
# -------------------------------------------------------------

print("\n" + "=" * 80)
print("DATA QUALITY SUMMARY")
print("=" * 80)

quality_df.groupBy("status").count().show()

quality_df.orderBy("table_name", "check_name").show(100, truncate=False)

print("\n" + "=" * 80)
print("SILVER DATA QUALITY VALIDATION COMPLETED")
print("=" * 80)


failed_count = quality_df.filter(col("status") == "FAIL").count()

if failed_count > 0:
    print(f"VALIDATION FAILED: {failed_count} failed checks")
    spark.stop()
    raise RuntimeError(
        f"Silver data quality validation failed: "
        f"{failed_count} checks failed."
    )

spark.stop()
print("All Silver data quality checks passed.")