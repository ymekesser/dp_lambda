import boto3
import pandas as pd
import io


def lambda_handler(event, context):
    bucket_name = 'mas-thesis-datapipeline-platform'
    src_file = "dp-plain-python/staging/resale-flat-prices-based-on-registration-date-from-jan-2017-onwards.csv"
    dst_file = "dp-plain-python/storage/resale_flat_prices.csv"
    s3 = boto3.resource("s3")

    obj = s3.Object(bucket_name, src_file)
    df = pd.read_csv(io.BytesIO(obj.get()['Body'].read()))

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.Object(bucket_name, dst_file).put(Body=csv_buffer.getvalue())

    return {
        'message': 'Successfully loaded to S3 storage'
    }