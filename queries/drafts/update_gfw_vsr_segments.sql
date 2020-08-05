-- # Step 0: Declare 'new_seg_vsr_ts'
-- # If starting from scratch, USE DECLARE STATEMENT BELOW: 
-- DECLARE
-- 	new_vsr_seg_ts DEFAULT(
-- 		SELECT
-- 			(SAFE_CAST ('1990-01-01 00:00:00 UTC' AS TIMESTAMP)));

        -- # If UPDATING, USE DECLARE STATEMENT BELOW, AND COMMENT OFF DECLARE STATEMENT ABOVE:
        -- # Since VSR season is not always active, this statement can return NULL if the new VSR polygon and date range has not been added to the `benioff_datasets.vsr_zones` table. SEE GFW AIS METHODOLOGY FOR DETAILS.  
        DECLARE
                new_vsr_seg_ts DEFAULT(
                        SELECT
                                CASE WHEN (
                                        SELECT
                                                MAX(timestamp)
                                                FROM `benioff-ocean-initiative.whalesafe.gfw_vsr_segments`
                                        WHERE
                                                DATE(timestamp) > DATE_SUB(CURRENT_DATE(), INTERVAL 8 MONTH)
                                        LIMIT 1) IS NULL THEN
                                        '2020-01-01'
                                ELSE
                                        (
                                                SELECT
                                                        MAX(timestamp)
                                                        FROM `benioff-ocean-initiative.whalesafe.gfw_vsr_segments`
                                                WHERE
                                                        DATE(timestamp) > DATE_SUB(CURRENT_DATE(), INTERVAL 8 MONTH)
                                                LIMIT 1)
                                END);

        -- # Step 1: Create temporary table to hold new vsr segments data.
        CREATE TEMPORARY TABLE `temp_gfw_vsr_segments` (
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
                lead_lon FLOAT64,
                lead_lat FLOAT64,
                source STRING,
                gt NUMERIC,
                point GEOGRAPHY,
                linestring GEOGRAPHY,
                final_speed_knots NUMERIC,
                speed_bin_num INT64,
                implied_speed_bin_num INT64,
                calculated_speed_bin_num INT64,
                final_speed_bin_num INT64,
                vsr_category STRING,
                vsr_linestring GEOGRAPHY
);

        -- # Step 2: INSERT new segments data (greater than 'new_seg_ts') that intersects VSR Zones into `temp_gfw_ihs_vsr_segments`
        INSERT INTO `temp_gfw_vsr_segments`
        SELECT
                s.*,
                z.vsr_category,
                # Spatial intersection case statement
                CASE WHEN ST_CoveredBy (s.linestring,
                        z.geog) THEN
                        s.linestring
                ELSE
                        ST_Intersection(s.linestring, z.geog)
                END AS vsr_linestring
        FROM
                `benioff-ocean-initiative.whalesafe.gfw_segments` AS s
                INNER JOIN `benioff-ocean-initiative.benioff_datasets.vsr_zones` AS z ON ST_Intersects(s.linestring, z.geog)
        WHERE
                s.timestamp <= z.datetime_end
                AND s.timestamp >= z.datetime_beg
                AND s.timestamp > new_vsr_seg_ts
                AND s.timestamp <= (
                    SELECT
                        MAX((datetime_end))
                                FROM
                        `benioff-ocean-initiative.benioff_datasets.vsr_zones`
                                LIMIT 1);

        -- 	# Step 3: Create partitioned and clustered `whalesafe_ais.gfw_ihs_vsr_segments` table if not already existing
        CREATE TABLE IF NOT EXISTS `benioff-ocean-initiative.whalesafe.gfw_vsr_segments` (
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
                lead_lon FLOAT64,
                lead_lat FLOAT64,
                source STRING,
                gt NUMERIC,
                point GEOGRAPHY,
                linestring GEOGRAPHY,
                final_speed_knots NUMERIC,
                speed_bin_num INT64,
                implied_speed_bin_num INT64,
                calculated_speed_bin_num INT64,
                final_speed_bin_num INT64,
                vsr_category STRING,
                vsr_linestring GEOGRAPHY
)
        PARTITION BY DATE(timestamp) CLUSTER BY
                        mmsi, linestring, vsr_linestring OPTIONS (description = "partitioned by day, clustered by (mmsi, linestring, vsr_linestring)", require_partition_filter = TRUE);

                -- # Step 4: Insert everything from `temp_gfw_ihs_vsr_gegments` into the clustered, partitioned table, `gfw_ihs_vsr_gegments`
                INSERT INTO `benioff-ocean-initiative.whalesafe.gfw_vsr_segments`
                SELECT
                        *
                FROM
                        temp_gfw_vsr_segments;

                --  Step 5: Make timestamp log if not already existing.
                CREATE TABLE IF NOT EXISTS `benioff-ocean-initiative.whalesafe.timestamp_log` (
                        newest_timestamp TIMESTAMP,
                        date_accessed TIMESTAMP,
                        table_name STRING);
                        
                        -- # Step 6: Insert the previous/starting newest `whalesafe.gfw_vsr_segments` timestamp, new_vsr_seg_ts, into `whalesafe.timestamp_log`
                        INSERT INTO `benioff-ocean-initiative.whalesafe.timestamp_log`
                        SELECT
                                new_vsr_seg_ts AS newest_timestamp,
                                CURRENT_TIMESTAMP() AS date_accessed,
                                'gfw_vsr_segments' AS table_name;

                        -- # Step 7: Insert updated newest `whalesafe.gfw_vsr_segments` timestamp into `whalesafe.timestamp_log`
                        INSERT INTO `benioff-ocean-initiative.whalesafe.timestamp_log`
                        SELECT
                                (
                                        SELECT
                                                MAX(timestamp)
                                        FROM
                                                `benioff-ocean-initiative.whalesafe.gfw_vsr_segments`
                                        WHERE
                                                DATE(timestamp) > DATE_SUB(CURRENT_DATE(), INTERVAL 8 MONTH)
                                        LIMIT 1) AS newest_timestamp,
                                CURRENT_TIMESTAMP() AS date_accessed,
                                'gfw_vsr_segments' AS table_name;