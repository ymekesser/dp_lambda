import os
import pandas as pd

from s3_io import read_dataframe, write_dataframe, join_path


def lambda_handler(event, context):
    src_file = join_path(
        os.environ["LOCATION_STORAGE"], os.environ["STORAGE_FILE_HDB_ADDRESS_GEODATA"]
    )
    dst_file = join_path(
        os.environ["LOCATION_TRANSFORMED_ANALYTICS"],
        os.environ["STORAGE_FILE_HDB_ADDRESS_GEODATA"],
    )

    hdb_address_geodata = read_dataframe(src_file)

    hdb_address_geodata = _remove_duplicates(hdb_address_geodata)
    hdb_address_geodata = _remove_untrusted_entries(hdb_address_geodata)

    hdb_address_geodata = hdb_address_geodata[
        "block",
        "street_name",
        "latitude",
        "longitude",
        "postal_code",
        "confidence",
        "type",
    ]

    write_dataframe(hdb_address_geodata, dst_file)

    return event


def _remove_untrusted_entries(hdb_address_geodata: pd.DataFrame) -> pd.DataFrame:
    # The geolocation service used didn't work perfectly, some results are wrong
    # Remove those with low confidence or wrong type
    low_confidence = hdb_address_geodata["confidence"] < 1
    wrong_type = hdb_address_geodata["type"] != "address"
    hdb_address_geodata = hdb_address_geodata.drop(
        hdb_address_geodata[low_confidence & wrong_type].index
    )

    return hdb_address_geodata


def _remove_duplicates(hdb_address_geodata: pd.DataFrame) -> pd.DataFrame:
    # There might be some duplicates
    hdb_address_geodata = hdb_address_geodata.drop_duplicates(
        subset=["block", "street_name"]
    )

    return hdb_address_geodata
