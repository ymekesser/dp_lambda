import os
import pandas as pd

from s3_io import read_dataframe, write_dataframe, join_path
from pandas_util import select_columns


def lambda_handler(event, context):
    mrt_stations_src_file = join_path(
        os.environ["LOCATION_STORAGE"], os.environ["STORAGE_FILE_MRT_STATIONS"]
    )
    mrt_geodata_src_file = join_path(
        os.environ["LOCATION_STORAGE"], os.environ["STORAGE_FILE_MRT_GEODATA"]
    )
    dst_file = join_path(
        os.environ["LOCATION_TRANSFORMED_ANALYTICS"],
        os.environ["STORAGE_FILE_HDB_ADDRESS_GEODATA"],
    )

    mrt_stations = read_dataframe(mrt_stations_src_file)

    mrt_stations = select_columns(
        mrt_stations,
        {"Name": "name", "Code": "code", "Opening": "opening"},
    )

    mrt_stations = _remove_duplicate_stations(mrt_stations)
    mrt_stations = _remove_planned_stations(mrt_stations)
    mrt_stations = _get_number_of_lines(mrt_stations)

    mrt_geodata = read_dataframe(mrt_geodata_src_file)

    mrt_geodata = select_columns(
        mrt_geodata,
        {
            "tags.name": "name",
            "lat": "latitude",
            "lon": "longitude",
        },
    )

    # Some stations appear as multiple nodes on the map,
    # They are clustered together, so we take the mean
    mrt_geodata = mrt_geodata.groupby("name").mean()

    # Join with geodata
    result = pd.merge(mrt_stations, mrt_geodata, how="inner", on="name")

    write_dataframe(result, dst_file)

    return event


def _remove_duplicate_stations(mrt_stations):
    # For interchanges the station is listed once per line.
    # we only care about the individual stations
    mrt_stations = mrt_stations.drop_duplicates(subset=["name"])

    return mrt_stations


def _get_number_of_lines(mrt_stations: pd.DataFrame) -> pd.DataFrame:
    # For interchanges with other lines, a station has multiple codes, separated by space.
    # E.g. Jurong East servicing both North-South and East-West lines has the code "NS1 EW24"
    mrt_stations["No of Lines"] = (
        mrt_stations["code"].str.replace("", "").str.split().apply(len)
    )

    return mrt_stations


def _remove_planned_stations(mrt_stations: pd.DataFrame) -> pd.DataFrame:
    # Format Opening as datetime
    # Remove all rows in mrt_stations where the "Opening" column is not a date.
    # Some planned stations have e.g. mid-2034 as an opening date
    mrt_stations["opening"] = pd.to_datetime(mrt_stations["opening"], errors="coerce")
    mrt_stations = mrt_stations.dropna(subset=["opening"])

    # We ignore stations which open in the future
    cutoff_date = pd.Timestamp("2023-05-01")
    mask = mrt_stations["opening"] > cutoff_date
    mrt_stations = mrt_stations.drop(mrt_stations[mask].index)

    return mrt_stations
