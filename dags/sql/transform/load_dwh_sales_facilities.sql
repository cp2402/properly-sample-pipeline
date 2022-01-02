INSERT INTO dwh_sales_facilities (
    property_address,
    sales_price,
    sport_field_nearby,
    outdoor_pool_nearby,
    outdoor_rink_nearby,
    outdoor_track_nearby,
    sport_field_proximity,
    outdoor_pool_proximity,
    outdoor_rink_proximity,
    outdoor_track_proxmity
)
SELECT
    src_property_sales.property_address,
    src_property_sales.price AS sales_price,
    COALESCE(dwh_facilities_count.sport_field_count, 0) AS sport_field_nearby,
    COALESCE(dwh_facilities_count.outdoor_pool_count, 0) AS outdoor_pool_nearby,
    COALESCE(dwh_facilities_count.outdoor_rink_count, 0) AS outdoor_rink_nearby,
    COALESCE(
        dwh_facilities_count.outdoor_track_count, 0
    ) AS outdoor_track_nearby,
    dwh_facilities_closest.sport_field_proximity,
    dwh_facilities_closest.outdoor_pool_proximity,
    dwh_facilities_closest.outdoor_rink_proximity,
    dwh_facilities_closest.outdoor_track_proxmity
FROM
    src_property_sales
LEFT JOIN
    dwh_facilities_count
    ON
        src_property_sales.property_address = dwh_facilities_count.property_address
LEFT JOIN
    dwh_facilities_closest
    ON
        src_property_sales.property_address = dwh_facilities_closest.property_address
