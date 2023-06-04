import os
import pandas as pd

from s3_io import read_dataframe, write_dataframe, join_path
from pandas_util import coalesce_colums, select_columns


def lambda_handler(event, context):
    src_file = join_path(
        os.environ["LOCATION_STORAGE"], os.environ["STORAGE_FILE_MALL_GEODATA"]
    )
    dst_file = join_path(
        os.environ["LOCATION_TRANSFORMED_ANALYTICS"],
        os.environ["STORAGE_FILE_MALL_GEODATA"],
    )

    mall_geodata = read_dataframe(src_file)

    # Depending on the type of element, the lat/long are in different columns
    mall_geodata = coalesce_colums(mall_geodata, ["lat", "center.lat"], "latitude")
    mall_geodata = coalesce_colums(mall_geodata, ["lon", "center.lon"], "longitude")
    mall_geodata = select_columns(
        mall_geodata,
        {
            "tags.name": "name",
            "latitude": "latitude",
            "longitude": "longitude",
        },
    )
    mall_geodata = _remove_nameless_malls(mall_geodata)
    mall_geodata = _remove_duplicate_malls(mall_geodata)

    write_dataframe(mall_geodata, dst_file)

    return event


def _remove_nameless_malls(mall_geodata: pd.DataFrame) -> pd.DataFrame:
    mall_geodata = mall_geodata.dropna(subset=["name"])

    return mall_geodata


def _remove_duplicate_malls(mall_geodata: pd.DataFrame) -> pd.DataFrame:
    # Some malls, e.g. Mustafa Centre, appear multiple times.
    # The locations are clustered together, so we take the mean of lat/long
    mall_geodata = mall_geodata.groupby("name", as_index=False).mean()

    return mall_geodata
