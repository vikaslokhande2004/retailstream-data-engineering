import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:19000",
    aws_access_key_id="retailadmin",
    aws_secret_access_key="retailstream123",
    region_name="us-east-1",
)

response = s3.list_objects_v2(
    Bucket="retailstream",
    Prefix="raw/"
)

print("=" * 70)
print("RETAILSTREAM RAW DATA")
print("=" * 70)

objects = response.get("Contents", [])

for obj in objects:
    print(f"{obj['Key']:40} {obj['Size']:,} bytes")

print("=" * 70)
print(f"Total objects: {len(objects)}")
