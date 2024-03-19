CREATE OR REPLACE VIEW dwh_facilities_count AS
SELECT
    property_address,
    MIN(
        CASE
            WHEN ( facility_category = 'Park' ) THEN facility_count
        END
    ) AS park_count,
    MIN(
        CASE
            WHEN ( facility_category = 'Community Centre' ) THEN facility_count
        END
    ) AS community_centre_count
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
