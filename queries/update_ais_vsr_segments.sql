-- # -- Updating `whalesafe_v2.ais_vsr_segments` -- # --
-- # Benioff Ocean Initiative: 2020-07-30

-- # Step 0: Declare 'new_seg_vsr_ts'
-- # If starting from scratch, USE DECLARE STATEMENT BELOW, AND COMMENT THE SECOND DECLARE: 
-- DECLARE
-- 	new_vsr_seg_ts DEFAULT(
-- 		SELECT
-- 			(SAFE_CAST ('1990-01-01 00:00:00 UTC' AS TIMESTAMP)));

-- # -- If UPDATING, USE DECLARE STATEMENT BELOW, AND COMMENT OFF DECLARE STATEMENT ABOVE:
-- # -- Since VSR season is not always active, this statement can return NULL if the new VSR polygon and date range has not been added to the `whalesafe_v2.vsr_zones` table. SEE GFW AIS METHODOLOGY FOR DETAILS.  
    DECLARE
        new_vsr_seg_ts DEFAULT(
        SELECT
         MAX(timestamp_end)
        FROM `whalesafe_v2.ais_vsr_segments`
        WHERE
         DATE(timestamp) > DATE_SUB(CURRENT_DATE(), INTERVAL 8 MONTH)
        LIMIT 1);

-- # -- Step 1: Create temporary table to hold new vsr segments data.
    CREATE TEMPORARY TABLE `temp_ais_vsr_segments` (
            timestamp TIMESTAMP,
                date DATE,
                mmsi INT64,
                num INT64,
                timestamp_beg TIMESTAMP,
                timestamp_end TIMESTAMP,
                speed_knots NUMERIC,
                implied_speed_knots NUMERIC,
                calculated_knots NUMERIC,
                distance_km NUMERIC,
                distance_nm NUMERIC,
                segment_time_minutes FLOAT64,
                lon FLOAT64,
                lat FLOAT64,
                source STRING,
                region STRING,
                gt NUMERIC,
                point GEOGRAPHY,
                linestring GEOGRAPHY,
                final_speed_knots NUMERIC,
                touches_coast BOOL,
                speed_bin_num INT64,
                implied_speed_bin_num INT64,
                calculated_speed_bin_num INT64,
                final_speed_bin_num INT64,
                vsr_category STRING,
                vsr_region STRING,
                vsr_linestring GEOGRAPHY
);

-- # -- Step 2: INSERT new segments data (greater than 'new_vsr_seg_ts') that intersects VSR Zones into `temp_ais_vsr_segments`
    INSERT INTO `temp_ais_vsr_segments`
    SELECT
        s.*,
        z.vsr_category,
        z.region AS vsr_region,
        -- # Spatial intersection case statement, when linestring is covered by VSR zone, return linestring
    -- # Otherwise, intersect the linestring with the VSR zone polygon.
        CASE WHEN ST_CoveredBy (s.linestring, z.geog) THEN
            s.linestring
        ELSE
            ST_Intersection(s.linestring, z.geog)
        END AS vsr_linestring
        FROM
            `whalesafe_v2.ais_segments` AS s 
            INNER JOIN `whalesafe_v2.vsr_zones` AS z 
        ON ST_Intersects(s.linestring, z.geog)
        WHERE
            s.timestamp <= z.datetime_end
            AND s.timestamp >= z.datetime_beg
            AND s.timestamp > new_vsr_seg_ts;
    -- # Querying gfw_segments and inner joining with vsr_zones 
    -- # Based on whether the segment timestamp is within a certain year's VSR SEASON
    -- # DECLARED new_vsr_seg_ts to filter gfw_segments for only new data using timestamp (partitioning column)

-- # -- Step 3: Create partitioned and clustered `whalesafe_v2.ais_vsr_segments` table if not already existing
    CREATE TABLE IF NOT EXISTS `whalesafe_v2.ais_vsr_segments` (
            timestamp TIMESTAMP,
                date DATE,
                mmsi INT64,
                num INT64,
                timestamp_beg TIMESTAMP,
                timestamp_end TIMESTAMP,
                speed_knots NUMERIC,
                implied_speed_knots NUMERIC,
                calculated_knots NUMERIC,
                distance_km NUMERIC,
                distance_nm NUMERIC,
                segment_time_minutes FLOAT64,
                lon FLOAT64,
                lat FLOAT64,
                source STRING,
                region STRING,
                gt NUMERIC,
                point GEOGRAPHY,
                linestring GEOGRAPHY,
                final_speed_knots NUMERIC,
                touches_coast BOOL,
                speed_bin_num INT64,
                implied_speed_bin_num INT64,
                calculated_speed_bin_num INT64,
                final_speed_bin_num INT64,
                vsr_category STRING,
                vsr_region STRING,
                vsr_linestring GEOGRAPHY
    )
    PARTITION BY DATE(timestamp) CLUSTER BY
        mmsi, vsr_region, linestring, vsr_linestring OPTIONS (description = "partitioned by day, clustered by (mmsi, linestring, vsr_linestring)", 
        require_partition_filter = TRUE);

-- # -- Step 4: Insert everything from temp table into the clustered, partitioned table, `ais_vsr_gegments`
    INSERT INTO `whalesafe_v2.ais_vsr_segments`
    SELECT
        *
    FROM
        temp_ais_vsr_segments;

-- # -- Step 5: Make whalesafe_v2 timestamp log table if not already existing.
    CREATE TABLE IF NOT EXISTS 
    `whalesafe_v2.whalesafe_timestamp_log` (
        newest_timestamp TIMESTAMP,
        date_accessed TIMESTAMP,
        table_name STRING,
        query_exec STRING
        );

-- # -- Step 6: Insert 'new_vsr_seg_ts', the new timestamp from `ais_vsr_segments` BEFORE querying ais_segments
    INSERT INTO `whalesafe_v2.whalesafe_timestamp_log`
    SELECT
        new_vsr_seg_ts                   AS newest_timestamp,
        CURRENT_TIMESTAMP() AS date_accessed,
        'ais_vsr_segments'             AS table_name,
        'query_start'                       AS query_exec;

-- # -- Step 7: Insert 'new_vsr_seg_ts', the new timestamp from `ais_vsr_segments` AFTER querying ais_segments
    INSERT INTO `whalesafe_v2.whalesafe_timestamp_log`
    SELECT
        (
        SELECT
            MAX(timestamp_end)
                    FROM
            `whalesafe_v2.ais_vsr_segments`
                    WHERE
            DATE(timestamp) > DATE_SUB(DATE(new_vsr_seg_ts), INTERVAL 8 MONTH)
                    LIMIT 1)                   AS newest_timestamp,
        CURRENT_TIMESTAMP() AS date_accessed,
        'ais_vsr_segments'             AS table_name,
       'query_end'                          AS query_exec;