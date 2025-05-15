import boto3
import io
import os
import time
import pandas as pd
import hashlib # Used here for generating dummy data, not strictly for S3 upload itself
import pyarrow.parquet as pq

S3_BUCKET_NAME = ""
S3_OBJECT_KEY = ""
AWS_REGION = "us-west-2" # e.g., "us-west-2", "eu-central-1"

#s3_client = boto3.client('s3', region_name=AWS_REGION, aws_access_key_id="***", aws_secret_access_key="***")
session = boto3.Session(profile_name='default')
s3_client = session.client('s3')
affected=[]

def get_server_details(metric_data_df: pd.DataFrame) -> pd.DataFrame:
    server_details_cols = [
        "device_id",
        "platform_customer_id",
        "application_customer_id",
        "model",
        "server_name",
        "server_generation",
        "processor_vendor",
        "location_id",
        "location_name",
        "location_country",
        "location_state",
        "location_city",
        "co2_factor",
        "energy_cost_factor",
        "sustainability_factor_source",
    ]
    new_inventory_columns = [
        "cpu_inventory",
        "memory_inventory",
        "pcie_devices_count",
        "socket_count",
    ]
    server_details_cols.extend(new_inventory_columns)
    server_details_df = metric_data_df[
        metric_data_df.columns.intersection(server_details_cols)
    ]
    if not server_details_df.empty:
        server_details_df = server_details_df.drop_duplicates(ignore_index=True)
    return server_details_df

try:
    session = boto3.Session(profile_name='default')
    s3_client = session.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix='refined/SlowPowerMeter/date=2025-05-15/')
    df = pd.DataFrame()
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                # print(f"Object Key: {obj['Key']}, Size: {obj['Size']} bytes, Last Modified: {obj['LastModified']}")
                response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=obj['Key'])
                parquet_file = io.BytesIO(response['Body'].read())
                tmp_df = pd.read_parquet(parquet_file)
                pcie_check = ('pcie_devices_count' not in tmp_df.columns) or (tmp_df['pcie_devices_count'] != 0).any()
                opted_in_exists = 'report_type' in tmp_df.columns and (tmp_df['report_type'] == 'OPTED_IN').any()
                if opted_in_exists:
                   df = pd.concat([df, tmp_df])

    print(df)


except Exception as e:
    print(f"Error listing objects in bucket '{bucket_name}' using profile '{profile_name}': {e}")

'''
try:
    response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=S3_OBJECT_KEY)
    file_content = response['Body'].read().decode('utf-8')
    print(file_content)
except Exception as e:
    print(f"Error reading file from S3: {e}")

'''
print(affected)
