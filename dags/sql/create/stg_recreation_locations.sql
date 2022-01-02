CREATE OR REPLACE VIEW stg_recreation_locations AS
SELECT
    src_recreation_facilities.facility_id,
    src_recreation_facilities.facility_type,
    src_recreation_facilities.facility_name,
    src_recreation_facilities.facility_display_name,
    src_recreation_facilities.location_id,
    src_recreation_facilities.location_address,
    src_recreation_facilities.location_name,
    src_recreation_facilities.postal_code,
    src_postal_locations.latitude,
    src_postal_locations.longitude
FROM
    src_recreation_facilities
INNER JOIN
    src_postal_locations ON
        src_recreation_facilities.postal_code = src_postal_locations.postal_code
