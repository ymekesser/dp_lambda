import boto3
import pandas as pd
import io
import os


def lambda_handler(event, context):
    src_file = "dp-lambda/staging/resale-flat-prices-based-on-registration-date-from-jan-2017-onwards.csv"
    dst_file = "dp-lambda/storage/resale_flat_prices.csv"
    s3 = boto3.resource("s3")

    obj = s3.Object(os.environ["S3_BUCKET"], src_file)
    df = pd.read_csv(io.BytesIO(obj.get()["Body"].read()))

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.Object(os.environ["S3_BUCKET"], dst_file).put(Body=csv_buffer.getvalue())

    return {"message": "Successfully loaded to S3 storage"}
