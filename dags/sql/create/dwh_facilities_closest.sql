CREATE OR REPLACE VIEW dwh_facilities_closest AS
SELECT
    property_address,
    MIN(
        CASE
            WHEN ( facility_category = 'Park' ) THEN facility_proximity
        END
    ) AS park_proximity,
    MIN(
        CASE
            WHEN ( facility_category = 'Community Centre' ) THEN facility_proximity
        END
    ) AS community_centre_proximity
FROM
    stg_recreation_proximities
GROUP BY
    property_address
