import boto3
import os
import json
import pandas as pd
import io
import posixpath


def join_path(*paths):
    return posixpath.join(*[str(path) for path in paths])


def write_json(data, dst_path):
    print(f"Write JSON data to {dst_path}")

    bytes = json.dumps(data).encode("utf-8")

    s3 = boto3.resource("s3")
    s3.Object(os.environ["S3_BUCKET"], dst_path).put(Body=bytes)


def read_json(src_path):
    print(f"Read JSON data from {src_path}")

    s3 = boto3.resource("s3")
    obj = s3.Object(os.environ["S3_BUCKET"], s3)
    data = obj.get()["Body"].read().decode("utf-8")
    return json.loads(data)


def write_dataframe(df, dst_path):
    print(f"Write dataframe to {dst_path}")

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    s3 = boto3.resource("s3")
    s3.Object(os.environ["S3_BUCKET"], dst_path).put(Body=csv_buffer.getvalue())


def read_dataframe(src_path):
    print(f"Read dataframe from {src_path}")

    s3 = boto3.resource("s3")
    obj = s3.Object(os.environ["S3_BUCKET"], src_path)
    df = pd.read_csv(io.BytesIO(obj.get()["Body"].read()))

    return df


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
