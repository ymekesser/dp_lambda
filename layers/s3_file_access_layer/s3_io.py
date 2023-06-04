import boto3
import os
import json


def write_json(data, dst_path):
    s3 = boto3.resource("s3")
    s3.Object(os.environ["S3_BUCKET"], dst_path).put(Body=json.dumps(data))
