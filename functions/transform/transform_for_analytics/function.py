import os
import pandas as pd
from math import radians

from scipy.spatial import cKDTree

from s3_io import read_dataframe, write_dataframe, join_path


def lambda_handler(event, context):
    hdb_resale_prices = read_dataframe(
        join_path(
            os.environ["LOCATION_TRANSFORMED_ANALYTICS"],
            os.environ["STORAGE_FILE_HDB_RESALE_PRICES"],
        )
    )
    mrt_stations = read_dataframe(
        join_path(
            os.environ["LOCATION_TRANSFORMED_ANALYTICS"],
            os.environ["STORAGE_FILE_MRT_STATIONS"],
        )
    )
    mall_geodata = read_dataframe(
        join_path(
            os.environ["LOCATION_TRANSFORMED_ANALYTICS"],
            os.environ["STORAGE_FILE_MALL_GEODATA"],
        )
    )
    hdb_address_geodata = read_dataframe(
        join_path(
            os.environ["LOCATION_TRANSFORMED_ANALYTICS"],
            os.environ["STORAGE_FILE_HDB_ADDRESS_GEODATA"],
        )
    )

    feature_set = pd.merge(
        hdb_resale_prices,
        hdb_address_geodata,
        how="left",
        on=["block", "street_name"],
    )

    missing_latitude_count = feature_set["latitude"].isnull().sum()
    print(
        f"Number of rows with missing address location: {missing_latitude_count} out of {feature_set.shape[0]}"
    )
    feature_set = feature_set.dropna(subset=["latitude"])

    feature_set = _add_closest_mrt(feature_set, mrt_stations)
    feature_set = _add_closest_mall(feature_set, mall_geodata)
    feature_set = _add_distance_to_cbd(feature_set)

    dst_file = join_path(
        os.environ["LOCATION_TRANSFORMED_ANALYTICS"],
        os.environ["ANALYTICS_FILE_FEATURE_SET"],
    )

    write_dataframe(feature_set, dst_file)

    return event


def _add_closest_mrt(df_feature_set, df_mrt_stations):
    df_feature_set = _find_closest_location(
        df_feature_set, df_mrt_stations, "closest_mrt"
    )
    return df_feature_set


def _add_closest_mall(df_feature_set, df_mall_geodata):
    df_feature_set = _find_closest_location(
        df_feature_set, df_mall_geodata, "closest_mall"
    )

    return df_feature_set


def _add_distance_to_cbd(df_feature_set):
    cbd_location = pd.DataFrame.from_dict(
        {
            "latitude": [1.280602347559877],
            "longitude": [103.85040609311484],
            "name": "CBD",
        }
    )

    df_feature_set = _find_closest_location(df_feature_set, cbd_location, "cbd")

    return df_feature_set


def _find_closest_location(
    df_feature_set: pd.DataFrame, df_locations: pd.DataFrame, location_type: str
) -> pd.DataFrame:
    EARTH_RADIUS = 6371

    # Convert latitude and longitude to radians
    df_feature_set["latitude_rad"] = df_feature_set["latitude"].apply(radians)
    df_feature_set["longitude_rad"] = df_feature_set["longitude"].apply(radians)
    df_locations["latitude_rad"] = df_locations["latitude"].apply(radians)
    df_locations["longitude_rad"] = df_locations["longitude"].apply(radians)

    # Create a KDTree for the location dataframe
    location_tree = cKDTree(df_locations[["latitude_rad", "longitude_rad"]])

    # Query the KDTree for each point in the points dataframe
    distances, indices = location_tree.query(
        df_feature_set[["latitude_rad", "longitude_rad"]], k=1
    )

    # Get the closest entry from the location dataframe
    df_feature_set[f"{location_type}"] = df_locations.loc[indices, "name"].values  # type: ignore
    df_feature_set[f"distance_to_{location_type}"] = distances * EARTH_RADIUS * 1000

    # Drop the intermediate columns
    df_feature_set.drop(["latitude_rad", "longitude_rad"], axis=1, inplace=True)

    return df_feature_set
