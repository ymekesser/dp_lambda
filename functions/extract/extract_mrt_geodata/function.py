import boto3
import requests
import json
import os

query = """
[out:json];
area["ISO3166-1"="SG"][admin_level=2];
node(area)["subway"="yes"];
out;
"""


def lambda_handler(event, context):
    endpoint = os.environ["API_OVERPASS"]
    response = requests.get(endpoint, params={"data": query})

    print(f"Querying overpass ({endpoint}). Query: {query}")

    response = requests.get(endpoint, params={"data": query})

    if response.status_code != 200:
        error_msg = (
            f"Overpass API returned {response.status_code} status code: {response.text}"
        )
        print(error_msg)
        raise requests.HTTPError(error_msg)

    data = response.json()

    s3 = boto3.resource("s3")
    s3.Object(os.environ["S3_BUCKET"], "dp-lambda/staging/mrt_geodata.json").put(
        Body=json.dumps(data)
    )
