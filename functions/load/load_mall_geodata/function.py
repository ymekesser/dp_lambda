import os
from s3_io import read_json, write_dataframe, join_path
import pandas as pd


def lambda_handler(event, context):
    src_file = join_path(
        os.environ["LOCATION_STAGING"],
        os.environ["STORAGE_FILE_MALL_GEODATA"],
    )
    dst_file = join_path(
        os.environ["LOCATION_STORAGE"], os.environ["STORAGE_FILE_MALL_GEODATA"]
    )
    data = read_json(src_file)

    data_dict = pd.json_normalize(data, record_path=["elements"])
    df = pd.DataFrame.from_dict(data_dict, orient="columns")

    write_dataframe(df, dst_file)

    return {"message": "Successfully loaded to S3 storage"}
