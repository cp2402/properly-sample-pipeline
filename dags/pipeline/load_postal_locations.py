import pandas as pd
from pipeline import config
from sqlalchemy import create_engine
from sqlalchemy.sql import text

DB_ENGINE = config.params["DB_ENGINE"]
DB_SCHEMA = config.params["DB_SCHEMA"]
DB_SRC_TABLE = "src_postal_locations"

DATA_FOLDER = config.params["DATA_FOLDER"]
FILE_TAG = config.params["POSTAL_LOCATIONS_FILE_TAG"]


def process_postal_locations(df: pd.DataFrame) -> pd.DataFrame:
    """Process and clean postal locations data"""

    # filter for Ontario records
    df = df.dropna()
    df.columns = df.columns.str.lower()
    filtered_df = df[(df["province_abbr"] == "ON")]

    return filtered_df


def load_postal_locations(**context: dict) -> None:
    """Processes postal locations data and performs upsert load to raw table"""

    extract_tag = context.get("ts_nodash")

    source_file = f"{DATA_FOLDER}/{FILE_TAG}-{extract_tag}.csv"
    df = pd.read_csv(source_file)
    df["extract_tag"] = extract_tag

    processed_df = process_postal_locations(df)

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
