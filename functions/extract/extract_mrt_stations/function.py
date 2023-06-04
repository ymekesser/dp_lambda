import os
from pathlib import Path

from s3_io import copy_file, join_path


def lambda_handler(event, context):
    src_file = os.environ["SOURCE_MRT_STATIONS"]
    dst_file = join_path(os.environ["LOCATION_STAGING"], Path(src_file).name)

    bucket_name = os.environ["S3_BUCKET"]

    print(f"Extracting {src_file} to {dst_file} on S3 Bucket {bucket_name}")

    copy_file(src_file, dst_file)
