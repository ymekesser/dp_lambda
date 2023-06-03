import boto3

def lambda_handler(event, context):
    src_file = 'source_data/resale-flat-prices-based-on-registration-date-from-jan-2017-onwards.csv'
    dst_file = 'dp-lambda/staging/resale-flat-prices-based-on-registration-date-from-jan-2017-onwards.csv'

    bucket_name = 'mas-thesis-datapipeline-platform'

    copy_source = {"Bucket": bucket_name, "Key": src_file}

    s3 = boto3.resource("s3")
    s3.meta.client.copy_object(
            CopySource=copy_source,
            Bucket=bucket_name,  # Destination bucket
            Key=dst_file,  # Destination path/filename
        )