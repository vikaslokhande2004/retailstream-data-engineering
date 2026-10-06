from pathlib import Path
import boto3

ENDPOINT = "http://localhost:19000"
ACCESS_KEY = "retailadmin"
SECRET_KEY = "retailstream123"
BUCKET = "retailstream"

RAW_DIR = Path("data/raw")

s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    region_name="us-east-1",
)

files = sorted(RAW_DIR.glob("*.parquet"))

if not files:
    raise FileNotFoundError("No Parquet files found in data/raw")

print("=" * 70)
print("UPLOADING RETAILSTREAM RAW DATA")
print("=" * 70)

for file in files:

    object_key = f"raw/{file.name}"

    print(f"Uploading: {file}")

    s3.upload_file(
        str(file),
        BUCKET,
        object_key,
    )

    print(f"Uploaded: s3://{BUCKET}/{object_key}")

print("\n" + "=" * 70)
print(f"Uploaded {len(files)} files successfully")
print("=" * 70)
