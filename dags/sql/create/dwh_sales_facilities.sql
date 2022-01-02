CREATE TABLE IF NOT EXISTS dwh_sales_facilities (
    property_address TEXT,
    sales_price INTEGER,
    sport_field_nearby INTEGER,
    outdoor_pool_nearby INTEGER,
    outdoor_rink_nearby INTEGER,
    outdoor_track_nearby INTEGER,
    sport_field_proximity DOUBLE PRECISION,
    outdoor_pool_proximity DOUBLE PRECISION,
    outdoor_rink_proximity DOUBLE PRECISION,
    outdoor_track_proxmity DOUBLE PRECISION
)
