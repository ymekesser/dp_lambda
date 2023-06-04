import boto3
import os
from pathlib import Path

def lambda_handler(event, context):
    src_file = os.environ["SOURCE_HDB_RESALE_PRICES"]
    dst_file = os.environ["LOCATION_STAGING"] + "/" + Path(src_file).name

    bucket_name = 'mas-thesis-datapipeline-platform'

    copy_source = {"Bucket": bucket_name, "Key": src_file}

    s3 = boto3.resource("s3")
    s3.meta.client.copy_object(
            CopySource=copy_source,
            Bucket=bucket_name,  # Destination bucket
            Key=dst_file,  # Destination path/filename
        )
