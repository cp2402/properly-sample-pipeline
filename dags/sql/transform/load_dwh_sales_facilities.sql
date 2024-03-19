INSERT INTO dwh_sales_facilities (
    property_address,
    sales_price,
    park_nearby,
    community_centre_nearby,
    park_proximity,
    community_centre_proxmity
)
SELECT
    src_property_sales.property_address,
    src_property_sales.price AS sales_price,
    COALESCE(dwh_facilities_count.park_count, 0) AS park_nearby,
    COALESCE(dwh_facilities_count.community_centre_count, 0) AS community_centre_nearby,
    dwh_facilities_closest.park_proximity,
    dwh_facilities_closest.community_centre_proximity
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
