import ast

import pandas as pd
from pipeline import config
from sqlalchemy import create_engine
from sqlalchemy.sql import text

DB_ENGINE = config.params["DB_ENGINE"]
DB_SCHEMA = config.params["DB_SCHEMA"]
DB_SRC_TABLE = "src_recreation_facilities"

DATA_FOLDER = config.params["DATA_FOLDER"]
FILE_TAG = config.params["RECREATION_FACILITIES_FILE_TAG"]


def extract_coordinates(row):
    row_dict = ast.literal_eval(row)
    coordinates = row_dict['coordinates'][0]
    latitude = coordinates[1]
    longitude = coordinates[0]
    return pd.Series({'latitude': latitude, 'longitude': longitude})


def process_recreation_facilities(df: pd.DataFrame) -> pd.DataFrame:
    """Process and clean recreation facilities data"""

    remap_columns = {
        "ASSET_ID": "facility_id",
        "TYPE": "facility_type",
        "ASSET_NAME": "facility_name",
        "LOCATIONID": "location_id",
        "ADDRESS": "location_address",
    }

    # drop unused columns
    df.drop(columns=['_id', 'AMENITIES', 'PHONE', "URL"], inplace=True)
    df[['latitude', 'longitude']] = df['geometry'].apply(lambda x: extract_coordinates(x))
    df.drop(columns=['geometry'], inplace=True)
    df = df.dropna()
    df = df.drop_duplicates(subset="LOCATIONID")
    df = df.rename(columns=remap_columns)

    return df


def load_recreation_facilities(**context: dict) -> None:
    """Processes recreation facilities data and performs upsert load to raw table"""

    extract_tag = context.get("ts_nodash")

    source_file = f"{DATA_FOLDER}/{FILE_TAG}-{extract_tag}.csv"
    
    df = pd.read_csv(source_file)
    df["extract_tag"] = extract_tag

    processed_df = process_recreation_facilities(df)

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
