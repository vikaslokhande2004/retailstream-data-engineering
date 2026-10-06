from pathlib import Path
import boto3
from pyspark.sql import SparkSession

ENDPOINT = "http://localhost:19000"
ACCESS_KEY = "retailadmin"
SECRET_KEY = "retailstream123"
BUCKET = "retailstream"

BRONZE_DIR = Path("data/bronze")

def download_raw_files():
    # Download raw objects from RustFS to a temporary local directory.

    temp_dir = Path("data/.raw_from_rustfs")
    temp_dir.mkdir(parents=True,exist_ok=True)

    s3 = boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name="us-east-1",
    )

    response = s3.list_objects_v2(
        Bucket=BUCKET,
        Prefix="raw/",
    )

    objects = response.get("Contents", [])

    if not objects:
        raise RuntimeError("No objects found under retailstream/raw/")

    downloaded_files = []

    for obj in objects:
        key = obj["Key"]

        if not key.endswith(".parquet"):
            continue

        filename = Path(key).name
        local_path = temp_dir / filename

        print(f"Downloading: {key}")

        s3.download_file(
            BUCKET,
            key,
            str(local_path),
        )

        downloaded_files.append(local_path)

    return downloaded_files

def main():

    print("=" * 70)
    print("RETAILSTREAM BRONZE INGESTION")
    print("=" * 70)

    BRONZE_DIR.mkdir(parents=True,exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("RetailStreamBronzeIngestion")
        .master("local[*]")
        .getOrCreate()
    )

    raw_files = download_raw_files()

    print(f"\nFound {len(raw_files)} raw Parquet files.")

    for file in sorted(raw_files):
        table_name = file.stem

        print("\n" + "-" * 70)
        print(f"Processing: {table_name}")
        print("-" * 70)

        df = spark.read.parquet(str(file))

        row_count = df.count()

        print(f"Input rows: {row_count:,}")

        output_path = BRONZE_DIR / table_name

        (
            df.write
            .mode("overwrite")
            .parquet(str(output_path))
        )

        print(f"Bronze output: {output_path}")

    spark.stop()

    print("\n" + "=" * 70)
    print("BRONZE INGESTION COMPLETED")
    print("=" * 70)

if __name__ == "__main__":
    main()