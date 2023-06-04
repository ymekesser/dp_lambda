import boto3
import os
import json


def write_json(data, dst_path):
    print(f"Write JSON data to {dst_path}")

    s3 = boto3.resource("s3")
    s3.Object(os.environ["S3_BUCKET"], dst_path).put(Body=json.dumps(data))


def copy_file(src_path, dst_path):
    print(f"Copying file from {src_path} to {dst_path}")

    bucket_name = os.environ["S3_BUCKET"]

    copy_source = {"Bucket": bucket_name, "Key": src_path}

    s3 = boto3.resource("s3")
    s3.meta.client.copy_object(
        CopySource=copy_source,
        Bucket=bucket_name,  # Destination bucket
        Key=dst_path,  # Destination path/filename
    )
