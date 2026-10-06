import boto3
from botocore.exceptions import ClientError

ENDPOINT = "http://localhost:19000"
ACCESS_KEY = "retailadmin"
SECRET_KEY = "retailstream123"
BUCKET = "retailstream"

s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    region_name="us-east-1",
)

try:
    s3.create_bucket(Bucket=BUCKET)
    print(f"Bucket created successfully: {BUCKET}")

except ClientError as e:
    error_code = e.response["Error"]["Code"]

    if error_code in ["BucketAlreadyOwnedByYou", "BucketAlreadyExists"]:
        print(f"Bucket already exists: {BUCKET}")
    else:
        raise

response = s3.list_buckets()

print("\nAvailable buckets:")
for bucket in response["Buckets"]:
    print(f" - {bucket['Name']}")
