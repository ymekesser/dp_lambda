import os
import pandas as pd

from s3_io import read_dataframe, write_dataframe, join_path
from pandas_util import select_columns


def lambda_handler(event, context):
    src_file = join_path(
        os.environ["LOCATION_STORAGE"], os.environ["STORAGE_FILE_MALL_GEODATA"]
    )
    dst_file = join_path(
        os.environ["LOCATION_TRANSFORMED_ANALYTICS"],
        os.environ["STORAGE_FILE_MALL_GEODATA"],
    )

    mall_geodata = read_dataframe(src_file)

    mall_geodata = _remove_nameless_malls(mall_geodata)
    mall_geodata = _remove_duplicate_malls(mall_geodata)
    df_mall_geodata = select_columns(
        df_mall_geodata,
        {
            "tags.name": "name",
            "latitude": "latitude",
            "longitude": "longitude",
        },
    )

    write_dataframe(mall_geodata, dst_file)

    return event


def _remove_nameless_malls(df_mall_geodata: pd.DataFrame) -> pd.DataFrame:
    df_mall_geodata = df_mall_geodata.dropna(subset=["name"])

    return df_mall_geodata


def _remove_duplicate_malls(df_mall_geodata: pd.DataFrame) -> pd.DataFrame:
    # Some malls, e.g. Mustafa Centre, appear multiple times.
    # The locations are clustered together, so we take the mean of lat/long
    df_mall_geodata = df_mall_geodata.groupby("name", as_index=False).mean()

    return df_mall_geodata
