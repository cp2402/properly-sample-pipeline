CREATE OR REPLACE VIEW dwh_facilities_closest AS
SELECT
    property_address,
    MIN(
        CASE
            WHEN ( facility_category = 'Sport Field' ) THEN facility_proximity
        END
    ) AS sport_field_proximity,
    MIN(
        CASE
            WHEN ( facility_category = 'Outdoor Pool' ) THEN facility_proximity
        END
    ) AS outdoor_pool_proximity,
    MIN(
        CASE
            WHEN ( facility_category = 'Outdoor Rink' ) THEN facility_proximity
        END
    ) AS outdoor_rink_proximity,
    MIN(
        CASE
            WHEN ( facility_category = 'Outdoor Track' ) THEN facility_proximity
        END
    ) AS outdoor_track_proxmity
FROM
    stg_recreation_proximities
GROUP BY
    property_address
ORDER BY
    property_address
