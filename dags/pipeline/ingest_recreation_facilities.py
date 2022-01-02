import requests
from pipeline import config

DATA_FOLDER = config.params["DATA_FOLDER"]
FILE_URL = config.params["RECREATION_FACILITIES_URL"]
FILE_TAG = config.params["RECREATION_FACILITIES_FILE_TAG"]


def ingest_recreation_facilities(**context: dict) -> None:
    """Ingest recreation facilities xml and save to file store"""

    full_file_name = f"{DATA_FOLDER}/{FILE_TAG}-{context.get('ts_nodash')}.xml"

    response = requests.get(FILE_URL)
    response.raise_for_status()

    with open(full_file_name, "w") as f:
        f.write(response.text)
