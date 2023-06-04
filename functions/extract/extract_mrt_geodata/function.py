import requests
import os

from s3_io import join_path, write_json

query = """
[out:json];
area["ISO3166-1"="SG"][admin_level=2];
node(area)["subway"="yes"];
out;
"""


def lambda_handler(event, context):
    endpoint = os.environ["API_OVERPASS"]
    dst_file = join_path(
        os.environ["LOCATION_STAGING"], os.environ["STORAGE_FILE_MRT_GEODATA"]
    )

    print(f"Querying overpass ({endpoint}). Query: {query}")

    response = requests.get(endpoint, params={"data": query})

    if response.status_code != 200:
        error_msg = (
            f"Overpass API returned {response.status_code} status code: {response.text}"
        )
        print(error_msg)
        raise requests.HTTPError(error_msg)
    print(f"Overpass query successful")

    data = response.json()

    write_json(
        data,
        dst_file,
    )
