import boto3
import os
import posixpath
import io
import pandas as pd
from math import radians

from scipy.spatial import cKDTree


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
            os.environ["STORAGE_FILE_MRT_GEODATA"],
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


def _add_closest_mrt(feature_set, mrt_stations):
    feature_set = _find_closest_location(feature_set, mrt_stations, "closest_mrt")
    return feature_set


def _add_closest_mall(feature_set, mall_geodata):
    feature_set = _find_closest_location(feature_set, mall_geodata, "closest_mall")

    return feature_set


def _add_distance_to_cbd(feature_set):
    cbd_location = pd.DataFrame.from_dict(
        {
            "latitude": [1.280602347559877],
            "longitude": [103.85040609311484],
            "name": "CBD",
        }
    )

    feature_set = _find_closest_location(feature_set, cbd_location, "cbd")

    return feature_set


def _find_closest_location(
    feature_set: pd.DataFrame, locations: pd.DataFrame, location_type: str
) -> pd.DataFrame:
    EARTH_RADIUS = 6371

    # Convert latitude and longitude to radians
    feature_set["latitude_rad"] = feature_set["latitude"].apply(radians)
    feature_set["longitude_rad"] = feature_set["longitude"].apply(radians)
    locations["latitude_rad"] = locations["latitude"].apply(radians)
    locations["longitude_rad"] = locations["longitude"].apply(radians)

    # Create a KDTree for the location dataframe
    location_tree = cKDTree(locations[["latitude_rad", "longitude_rad"]])

    # Query the KDTree for each point in the points dataframe
    distances, indices = location_tree.query(
        feature_set[["latitude_rad", "longitude_rad"]], k=1
    )

    # Get the closest entry from the location dataframe
    feature_set[f"{location_type}"] = locations.loc[indices, "name"].values  # type: ignore
    feature_set[f"distance_to_{location_type}"] = distances * EARTH_RADIUS * 1000

    # Drop the intermediate columns
    feature_set.drop(["latitude_rad", "longitude_rad"], axis=1, inplace=True)

    return feature_set


def join_path(*paths) -> str:
    return posixpath.join(*[str(path) for path in paths])


def read_dataframe(src_path: str) -> pd.DataFrame:
    print(f"Read dataframe from {src_path}")

    s3 = boto3.resource("s3")
    obj = s3.Object(os.environ["S3_BUCKET"], src_path)
    df = pd.read_csv(io.BytesIO(obj.get()["Body"].read()))

    return df


def write_dataframe(df: pd.DataFrame, dst_path: str) -> None:
    print(f"Write dataframe to {dst_path}")

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    s3 = boto3.resource("s3")
    s3.Object(os.environ["S3_BUCKET"], dst_path).put(Body=csv_buffer.getvalue())
