CREATE OR REPLACE VIEW dwh_facilities_count AS
SELECT
    property_address,
    MIN(
        CASE
            WHEN ( facility_category = 'Sport Field' ) THEN facility_count
        END
    ) AS sport_field_count,
    MIN(
        CASE
            WHEN ( facility_category = 'Outdoor Pool' ) THEN facility_count
        END
    ) AS outdoor_pool_count,
    MIN(
        CASE
            WHEN ( facility_category = 'Outdoor Rink' ) THEN facility_count
        END
    ) AS outdoor_rink_count,
    MIN(
        CASE
            WHEN ( facility_category = 'Outdoor Track' ) THEN facility_count
        END
    ) AS outdoor_track_count
FROM
    (
        SELECT
            property_address,
            facility_category,
            COUNT(*) AS facility_count
        FROM
            stg_recreation_proximities
        WHERE
            facility_proximity < 1
        GROUP BY
            property_address,
            facility_category
    ) facilities_count
GROUP BY
    property_address
