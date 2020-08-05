-- # -- # Updating `whalesafe_v2.ais_data` -- # --
-- # Benioff Ocean Initiative: 2020-07-30
-- #                    __       __
-- #                     '.'--.--'.-'
-- #       .,_------.___,   \' r'
-- #       ', '-._a      '-' .'
-- #        '.    '-'Y \._  /
-- #          '--;____'--.'-,
-- #Sean Goral/..'Ben Best''\ Callie Steffen ''' Morgan Visalli # --

-- # -- # Step 0: DECLARE newest timestamp.
-- # IF STARTING FROM SCRATCH, USE FIRST DECLARE STATEMENT AND COMMENT SECOND DECLARE STATEMENT BELOW:
-- DECLARE
-- new_ais_ts DEFAULT
--  (SELECT SAFE_CAST('2017-01-01 00:00:00 UTC' AS TIMESTAMP));

-- # -- # IF UPDATING: DECLARE newest AIS timestamp from `whalesafe_v2.ais_data` table as 'new_ais_ts'
DECLARE
    new_ais_ts DEFAULT(
            SELECT
                MAX(timestamp)
                        FROM `whalesafe_v2.ais_data`
                        WHERE
                DATE(timestamp) > DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY) -- # query last 28 days for max timestamp
                        LIMIT 1);

-- # -- # Step 1: Make temporary table `temp_ais_data` for incoming GFW AIS data
    CREATE TEMPORARY TABLE `temp_ais_data` (
            mmsi INT64,
                timestamp TIMESTAMP,
                lon FLOAT64,
                lat FLOAT64,
                speed_knots NUMERIC,
                implied_speed_knots NUMERIC,
                source STRING,
                region STRING
);

-- # -- # Step 2: Insert GFW AIS data (with a timestamp > than the max tiestamp in the existing `ais_data` table) into the `temp_ais_data` table
    INSERT INTO `temp_ais_data`
    SELECT
        SAFE_CAST (ais.ssvid AS INT64) AS mmsi, -- # CAST ssvid to NUMERIC and rename AS mmsi
        ais.timestamp,
        ais.lon,
        ais.lat,
        SAFE_CAST (ais.speed_knots AS NUMERIC) AS speed_knots, -- # CAST speed_knots to NUMERIC
        SAFE_CAST (ais.implied_speed_knots AS NUMERIC) AS implied_speed_knots, --# CAST implied_speed_knots to NUMERIC
        ais.source,
    CASE WHEN 
    lat >= (33.290)     -- # 33.2998838
        AND lat <= (34.5739)    -- # 34.5736988
        AND lon >= (- 125.013)   -- # -121.0392169
        AND lon <= (- 117.460)  -- # -117.4701519 
    THEN 'sc' -- Southern CA Region.
    WHEN
    lat > (34.5739)     -- # 33.2998838
        AND lat <= (35.557)    -- # 34.5736988
        AND lon >= (- 125.013)   -- # -121.0392169
        AND lon <= (- 117.460)  -- # -117.4701519 
    THEN 'cc' -- Central Coast CA Region.
    WHEN
    lat > (35.557)     -- # 33.2998838
        AND lat <= (39.032)    -- # 34.5736988
        AND lon >= (- 125.013)   -- # -121.0392169
        AND lon <= (- 117.460)  -- # -117.4701519 
    THEN 'sf' -- San Francisco Region
    ELSE 'other'
    END AS region
    FROM
-- -- # Old pipeline, switch to one below. -- -- # Querying GFW AIS pipeline. Requires permissions. 
    `gfw_research.ais_pipeline_dummy_2019` AS ais
WHERE
    date > new_ais_ts
-- -- # New Pipeline
-- 		`gfw_research.ais_pipeline_dummy_2020` AS ais 
-- 	WHERE
-- 		_PARTITIONDATE = DATE(new_ais_ts) 
-- # New GWF pipeline uses '_PARTITIONDATE' as partitioning column.
-- # Important for keeping query costs as cheap as possible.
-- # Bounding box for waters off CA coast.
    AND lat >= (33.285)     -- # 33.2998838
    AND lat <= (39.032)    -- # 34.5736988
    AND lon <= (- 117.455)  -- # -117.4701519 
        AND lon >= (- 125.013)   -- # -121.0392169
;

-- # -- Step 3: Create empty partitioned ad clustered table, for GFW AIS DATA if not already existing.
-- # -- `whalesafe_v2.ais_data` table
    CREATE TABLE IF NOT EXISTS `whalesafe_v2.ais_data` (
            mmsi INT64,
                timestamp TIMESTAMP,
                lon FLOAT64,
                lat FLOAT64,
                speed_knots NUMERIC,
                implied_speed_knots NUMERIC,
                source STRING,
                region STRING
)
    PARTITION BY DATE(timestamp) CLUSTER BY
        mmsi, region OPTIONS (description = "partitioned by day, clustered by (mmsi, region)", require_partition_filter = TRUE);

-- # -- # Step 4: Insert everything from `temp_ais_data` table into partitioned, clustered table,
-- # -- # `whalesafe_v2.ais_data` table
    INSERT INTO `whalesafe_v2.ais_data`
    SELECT
        *
    FROM
        `temp_ais_data`;

-- # -- # Step 5: Create whalesafe_v2 timestamp log table if not already existing.
    CREATE TABLE IF NOT EXISTS 
    `whalesafe_v2.whalesafe_timestamp_log` (
        newest_timestamp TIMESTAMP,
                        date_accessed TIMESTAMP,
                        table_name STRING,
                        query_exec STRING
);

-- # -- # Step 6: Insert 'new_ais_ts', the new timestamp in `ais_data` from BEFORE querying gfw pipeline.
    INSERT INTO `whalesafe_v2.whalesafe_timestamp_log`
    SELECT
        new_ais_ts AS newest_timestamp,
        CURRENT_TIMESTAMP() AS date_accessed,
        'ais_data' AS table_name,
    'query_start' AS query_exec;

-- # -- # Step 7: Insert 'new_ais_ts', the new timestamp in `ais_data` from AFTER querying gfw pipeline
    INSERT INTO `whalesafe_v2.whalesafe_timestamp_log`
    SELECT
        (
                                    SELECT
        MAX(timestamp)
          FROM
        `whalesafe_v2.ais_data`
          WHERE
        DATE(timestamp) > DATE_SUB(DATE(new_ais_ts), INTERVAL 21 DAY)
          LIMIT 1) AS newest_timestamp,
        CURRENT_TIMESTAMP() AS date_accessed,
        'ais_data' AS table_name,
    'query_end' AS query_exec;
