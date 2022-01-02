import os

from pipeline import config
from pipeline.helpers.zip_file_io import download_zip_file

DATA_FOLDER = config.params["DATA_FOLDER"]
FILE_URL = config.params["POSTAL_LOCATIONS_URL"]
FILE_NAME = config.params["POSTAL_LOCATIONS_FILE_NAME"]
FILE_TAG = config.params["POSTAL_LOCATIONS_FILE_TAG"]


def ingest_postal_locations(**context: dict) -> None:
    """Ingest postal location zip file and extract to file store"""

    # download and extract file
    download_zip_file(FILE_URL, FILE_NAME, DATA_FOLDER, is_insecure=True)

    # rename file with tag and timestamp
    os.rename(
        f"{DATA_FOLDER}/{FILE_NAME}",
        f"{DATA_FOLDER}/{FILE_TAG}-{context.get('ts_nodash')}.csv",
    )
