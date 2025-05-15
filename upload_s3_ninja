import pandas as pd
import io
import boto3

# Load CSV into a DataFrame
csv_file_path = "server3.csv"  # Local CSV file
df = pd.read_csv(csv_file_path)

# Convert DataFrame to Parquet format (in memory)
parquet_buffer = io.BytesIO()
df.to_parquet(parquet_buffer, engine="pyarrow", index=False)

# Configure S3 resource
s3 = boto3.resource(
    "s3",
    region_name="us-east-1",
    endpoint_url="http://localhost:9444",
    aws_access_key_id="***",
    aws_secret_access_key="***",
)

# Define S3 bucket and key (file path)
bucket_name = "hpe-local-telemetry"
parquet_s3_key = "refined/SlowPowerMeter/date=2025-04-24/pcid=pci1/acid=aci1/device_id=068519-402+8899068519702532/4.parquet"

# Upload Parquet file to S3
s3.Object(bucket_name, parquet_s3_key).put(Body=parquet_buffer.getvalue())

print(f"Parquet file uploaded successfully to s3://{bucket_name}/{parquet_s3_key}")
