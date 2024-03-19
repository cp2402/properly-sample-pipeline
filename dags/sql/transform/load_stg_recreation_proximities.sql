INSERT INTO
stg_recreation_proximities (
    property_address,
    facility_location,
    facility_category,
    facility_proximity
)
SELECT DISTINCT
    src_property_sales.property_address,
    stg_recreation_locations.facility_name AS facility_location,
    stg_recreation_locations.facility_type AS facility_category,
    2 * 6371 * asin(
        sqrt(
            (
                sin(
                    radians(
                        (
                            stg_recreation_locations.latitude - src_property_sales.latitude
                        ) / 2
                    )
                )
            ) ^ 2 + cos(
                radians( src_property_sales.latitude)
            ) * cos(
                radians(stg_recreation_locations.latitude)
            ) * (
                sin(
                    radians(
                        (
                            stg_recreation_locations.longitude - src_property_sales.longitude
                        ) / 2
                    )
                )
            ) ^ 2
        )
    ) AS facility_proximity
FROM
    src_property_sales
CROSS JOIN
    stg_recreation_locations
