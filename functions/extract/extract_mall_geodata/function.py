import boto3
import requests
import json

query = """
[out:json];
area["ISO3166-1"="SG"][admin_level=2]->.sg;
(
  node["shop"="mall"](area.sg);
  way["shop"="mall"](area.sg);
  relation["shop"="mall"](area.sg);
);
out center;
"""

overpass_endpoint = "https://overpass-api.de/api/interpreter"

def lambda_handler(event, context):
    response = requests.get(overpass_endpoint, params={"data": query})

    if response.status_code != 200:
        error_msg = (
            f"Overpass API returned {response.status_code} status code: {response.text}"
        )
        raise requests.HTTPError(error_msg)
    
    data = response.json()
    
    bucket_name = 'mas-thesis-datapipeline-platform'

    s3 = boto3.resource("s3")
    s3.Object(bucket_name, "mrt_geodata.json").put(Body=json.dumps(data))