import os
from pathlib import Path

from s3_io import copy_file, join_path


def lambda_handler(event, context):
    src_file = os.environ["SOURCE_HDB_RESALE_PRICES"]
    dst_file = join_path(os.environ["LOCATION_STAGING"], Path(src_file).name)

    copy_file(src_file, dst_file)

    return event
