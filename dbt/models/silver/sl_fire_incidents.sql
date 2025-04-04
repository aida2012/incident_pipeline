WITH load_dates AS (
    SELECT prev_data_as_of
    FROM incidents.metadata.load_tracker
    WHERE model_name = 'fire_incidents'
), base AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY (incident_number,exposure_number)
               ORDER BY data_as_of DESC
           ) AS row_num
    FROM {{ ref('br_fire_incidents') }}
)
    SELECT 
        md5(concat(incident_number, '-', exposure_number)) AS fire_incident_id,
        incident_number,
        CAST(exposure_number AS INTEGER) AS exposure_number,
        address,
        CAST(incident_date AS TIMESTAMP) AS incident_date,
        TO_CHAR(incident_date::DATE, 'YYYYMMDD')::INTEGER AS date_id,
        call_number,
        CAST(alarm_dttm AS TIMESTAMP) AS alarm_dttm,
        CAST(arrival_dttm AS TIMESTAMP) AS arrival_dttm,
        CAST(close_dttm AS TIMESTAMP) AS close_dttm,
        city,
        zipcode,
        battalion,
        station_area,
        box,
        CAST(suppression_units AS INTEGER) AS suppression_units,
        CAST(suppression_personnel AS INTEGER) AS suppression_personnel,
        CAST(ems_units  AS INTEGER) AS ems_units,
        CAST(ems_personnel  AS INTEGER) AS ems_personnel,
        CAST(other_units AS INTEGER) AS other_units,
        CAST(other_personnel  AS INTEGER) AS other_personnel,
        first_unit_on_scene,
        CAST(estimated_property_loss AS NUMERIC(15,2)) AS estimated_property_loss,
        CAST(estimated_contents_loss AS NUMERIC(15,2)) AS estimated_contents_loss,
        CAST(fire_fatalities  AS INTEGER) AS fire_fatalities,
        CAST(fire_injuries AS INTEGER) AS fire_injuries,
        CAST(civilian_fatalities AS INTEGER) AS civilian_fatalities,
        CAST(civilian_injuries AS INTEGER) AS civilian_injuries,
        CAST(number_of_alarms AS INTEGER) AS number_of_alarms,
        primary_situation,
        mutual_aid,
        action_taken_primary,
        action_taken_secondary,
        action_taken_other,
        detector_alerted_occupants,
        property_use,
        area_of_fire_origin,
        ignition_cause,
        ignition_factor_primary,
        ignition_factor_secondary,
        heat_source,
        item_first_ignited,
        human_factors_associated_with_ignition,
        structure_type,
        structure_status,
        CAST(floor_of_fire_origin AS INTEGER) AS floor_of_fire_origin,
        fire_spread,
        no_flame_spread,
        CAST(num_floors_min_damage AS INTEGER) AS num_floors_min_damage,
        CAST(num_floors_significant_damage AS INTEGER) AS num_floors_significant_damage,
        CAST(num_floors_heavy_damage AS INTEGER) AS num_floors_heavy_damage,
        CAST(num_floors_extreme_damage AS INTEGER) AS num_floors_extreme_damage,
        detectors_present,
        detector_type,
        detector_operation,
        detector_effectiveness,
        detector_failure_reason,
        automatic_extinguishing_system_present,
        automatic_extinguishing_system_type,
        automatic_extinguishing_system_performance,
        automatic_extinguishing_system_failure_reason,
        CAST(num_sprinkler_heads_operating AS INTEGER) AS num_sprinkler_heads_operating,
        supervisor_district,
        neighborhood_district,
        ST_SetSRID(ST_GeomFromText(point), 4326) AS point,
        CAST(data_as_of AS TIMESTAMP) AS data_as_of,
        CAST(data_loaded_at AS TIMESTAMP) AS data_loaded_at,
        CURRENT_TIMESTAMP AS insert_timestamp
    FROM base,load_dates
    WHERE 
        row_num = 1 AND
        battalion IN ('B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B09', 'B10')  
        AND CAST(number_of_alarms AS INTEGER) BETWEEN 1 AND 5  
        AND incident_number IS NOT NULL 
        AND exposure_number IS NOT NULL
        AND ((CAST(data_as_of AS TIMESTAMP)>prev_data_as_of and  prev_data_as_of is not NULL) OR  (prev_data_as_of is NULL))





 

