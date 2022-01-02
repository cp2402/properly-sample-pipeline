import json

import pandas as pd
import xmltodict
from pipeline import config
from sqlalchemy import create_engine
from sqlalchemy.sql import text

DB_ENGINE = config.params["DB_ENGINE"]
DB_SCHEMA = config.params["DB_SCHEMA"]
DB_SRC_TABLE = "src_recreation_facilities"

DATA_FOLDER = config.params["DATA_FOLDER"]
FILE_TAG = config.params["RECREATION_FACILITIES_FILE_TAG"]


def process_recreation_facilities(data: dict) -> pd.DataFrame:
    """Process and clean recreation facilities data"""

    remap_columns = {
        "PostalCode": "postal_code",
        "FacilityID": "facility_id",
        "FacilityType": "facility_type",
        "FacilityName": "facility_name",
        "FacilityDisplayName": "facility_display_name",
        "LocationID": "location_id",
        "LocationName": "location_name",
        "Address": "location_address",
    }

    # map xml data to dataframe
    df = pd.DataFrame(data["Locations"]["Location"])

    # consolidate facilities column - can consists of dict and lists
    json_struct = json.loads(df.to_json(orient="records"))
    for item in json_struct:
        item["Facilities"] = (
            [item["Facilities"]["Facility"]]
            if type(item["Facilities"]["Facility"]) == dict
            else item["Facilities"]["Facility"]
        )

    # flatten nested facilities lists
    flat_df = pd.json_normalize(
        json_struct,
        record_path=["Facilities"],
        meta=["LocationID", "Address", "LocationName", "PostalCode"],
        sep="_",
    )

    flat_df = flat_df.dropna()
    flat_df = flat_df.drop_duplicates(subset="FacilityID")
    flat_df = flat_df.rename(columns=remap_columns)

    return flat_df


def load_recreation_facilities(**context: dict) -> None:
    """Processes recreation facilities data and performs upsert load to raw table"""

    extract_tag = context.get("ts_nodash")

    source_file = f"{DATA_FOLDER}/{FILE_TAG}-{extract_tag}.xml"

    with open(source_file, "r") as myfile:
        data = xmltodict.parse(myfile.read(), dict_constructor=dict)

        processed_df = process_recreation_facilities(data)
        processed_df["extract_tag"] = extract_tag

        # upsert - delete where existing based on tag, then append all source records
        # may need to convert to table truncation if dataset needs to be fully refreshed
        # instead of incrementally loaded (depends on source use case)
        engine = create_engine(DB_ENGINE)
        engine.execute(
            text(
                f"DELETE FROM {DB_SRC_TABLE} WHERE extract_tag = '{extract_tag}'"
            ).execution_options(autocommit=True)
        )
        processed_df.to_sql(
            DB_SRC_TABLE,
            engine,
            schema=DB_SCHEMA,
            if_exists="append",
            index=False,
            chunksize=1000,
        )
