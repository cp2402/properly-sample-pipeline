import pandas as pd
from pipeline import config
from sqlalchemy import create_engine
from sqlalchemy.sql import text

DB_ENGINE = config.params["DB_ENGINE"]
DB_SCHEMA = config.params["DB_SCHEMA"]
DB_SRC_TABLE = "src_property_sales"

DATA_FOLDER = config.params["DATA_FOLDER"]
FILE_TAG = config.params["PROPERTY_SALES_FILE_TAG"]


def process_property_sales(df: pd.DataFrame) -> pd.DataFrame:
    """Process and clean property sales data"""

    remap_columns = {
        "Address": "property_address",
        "AreaName": "area_name",
        "Price ($)": "price",
        "lat": "latitude",
        "lng": "longitude",
    }

    # drop first column
    df.drop(columns=df.columns[0], axis=1, inplace=True)
    df = df.dropna()
    df = df.drop_duplicates()
    df = df.rename(columns=remap_columns)

    # filter for Toronto records
    filtered_df = df[df["property_address"].str.contains("Toronto, ON")]

    return filtered_df


def load_property_sales(**context: dict) -> None:
    """Processes property sales data and performs upsert load to raw table"""

    extract_tag = context.get("ts_nodash")

    source_file = f"{DATA_FOLDER}/{FILE_TAG}-{extract_tag}.csv"
    df = pd.read_csv(source_file, index_col=None)
    df["extract_tag"] = extract_tag

    processed_df = process_property_sales(df)

    # upsert - delete where existing based on tag, then append all source records
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
