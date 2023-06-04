import boto3
import pandas as pd
import io
import os
from pathlib import Path


def lambda_handler(event, context):
    bucket_name = os.environ["S3_BUCKET"]
    src_file = os.environ["LOCATION_STAGING"] + "/" + Path(os.environ["S3_BUCKET"]).name
    dst_file = (
        os.environ["LOCATION_STORAGE"]
        + "/"
        + os.environ["STORAGE_FILE_HDB_RESALE_PRICES"]
    )
    s3 = boto3.resource("s3")

    obj = s3.Object(bucket_name, src_file)
    df = pd.read_csv(io.BytesIO(obj.get()["Body"].read()))

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.Object(bucket_name, dst_file).put(Body=csv_buffer.getvalue())

    return {"message": "Successfully loaded to S3 storage"}
