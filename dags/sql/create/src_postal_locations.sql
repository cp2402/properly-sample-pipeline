CREATE TABLE IF NOT EXISTS src_postal_locations (
    postal_code TEXT,
    city TEXT,
    province_abbr TEXT,
    time_zone INTEGER,
    latitude REAL,
    longitude REAL,
    extract_tag TEXT
)
