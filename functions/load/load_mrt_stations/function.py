import os
from pathlib import Path
from s3_io import read_dataframe, write_dataframe, join_path


def lambda_handler(event, context):
    src_file = join_path(
        os.environ["LOCATION_STAGING"],
        Path(os.environ["SOURCE_MRT_STATIONS"]).name,
    )
    dst_file = join_path(
        os.environ["LOCATION_STORAGE"], os.environ["STORAGE_FILE_MRT_STATIONS"]
    )
    df = read_dataframe(src_file)

    write_dataframe(df, dst_file)

    return {"message": "Successfully loaded to S3 storage"}
