--  # Step 0:
--  # IF STARTING FROM SCRATCH, USE DECLARE STATEMENT BELOW:
-- DECLARE
-- 	new_seg_ts DEFAULT(
-- 		SELECT
-- 			SAFE_CAST ('1990-01-01 00:00:00' AS TIMESTAMP));

--  # IF UPDATING `whalesafe.gfw_segments`, USE DECLARE STATEMENT BELOW:
DECLARE
        new_seg_ts DEFAULT(
                SELECT
                        MAX(timestamp_end)
                        FROM `benioff-ocean-initiative.whalesafe.gfw_segments`
                WHERE
                        DATE(timestamp) > DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
                LIMIT 1);

        -- # Step 1: Create temporary table to hold data to be inserted into `whalesafe.gfw_segments` table
        CREATE TEMPORARY TABLE `temp_gfw_segments` (
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
                final_speed_bin_num INT64);

        -- # Step 2: Insert data from `whalesafe.gfw_data` that has a timestamp greater than the new_seg_ts, declared above.
        INSERT INTO `temp_gfw_segments`
        SELECT
                *,
                CASE WHEN speed_knots = 0 THEN
                        0
                WHEN speed_knots > 0
                        AND speed_knots <= 10 THEN
                        1
                WHEN speed_knots > 10
                        AND speed_knots <= 12 THEN
                        2
                WHEN speed_knots > 12
                        AND speed_knots <= 15 THEN
                        3
                WHEN speed_knots > 15
                        AND speed_knots <= 50 THEN
                        4
                ELSE
                        5
                END AS speed_bin_num,
                CASE WHEN implied_speed_knots = 0 THEN
                        0
                WHEN implied_speed_knots > 0
                        AND implied_speed_knots <= 10 THEN
                        1
                WHEN implied_speed_knots > 10
                        AND implied_speed_knots <= 12 THEN
                        2
                WHEN implied_speed_knots > 12
                        AND implied_speed_knots <= 15 THEN
                        3
                WHEN implied_speed_knots > 15
                        AND implied_speed_knots <= 50 THEN
                        4
                ELSE
                        5
                END AS implied_speed_bin_num,
                CASE WHEN calculated_knots = 0 THEN
                        0
                WHEN calculated_knots > 0
                        AND calculated_knots <= 10 THEN
                        1
                WHEN calculated_knots > 10
                        AND calculated_knots <= 12 THEN
                        2
                WHEN calculated_knots > 12
                        AND calculated_knots <= 15 THEN
                        3
                WHEN calculated_knots > 15
                        AND calculated_knots <= 50 THEN
                        4
                ELSE
                        5
                END AS calculated_speed_bin_num,
                CASE WHEN final_speed_knots = 0 THEN
                        0
                WHEN final_speed_knots > 0
                        AND final_speed_knots <= 10 THEN
                        1
                WHEN final_speed_knots > 10
                        AND final_speed_knots <= 12 THEN
                        2
                WHEN final_speed_knots > 12
                        AND final_speed_knots <= 15 THEN
                        3
                WHEN final_speed_knots > 15
                        AND final_speed_knots <= 50 THEN
                        4
                ELSE
                        5
                END AS final_speed_bin_num
        FROM (
                SELECT
                        *,
                        CASE WHEN calculated_knots BETWEEN 0.000001 AND 50 THEN
                                calculated_knots
                        WHEN implied_speed_knots BETWEEN 0.000001 AND 50 THEN
                                implied_speed_knots
                        WHEN speed_knots BETWEEN 0.000001 AND 50 THEN
                                speed_knots
                        ELSE
                                NULL
                        END AS final_speed_knots
                FROM (
                        SELECT
                                timestamp,
                                DATE(timestamp) AS date,
                                mmsi,
                                num,
                                SAFE_CAST (t1 AS TIMESTAMP) AS timestamp_beg,
                                SAFE_CAST (t2 AS TIMESTAMP) AS timestamp_end,
                                speed_knots,
                                implied_speed_knots,
                                -- # If the time elapsed between points is greater than 0 milliseconds, then calculate the speed in knots, otherwise return 0 knots
                                SAFE_CAST (
                                        CASE WHEN (TIMESTAMP_DIFF (TIMESTAMP(t2),
                                                        TIMESTAMP(t1),
                                                        MILLISECOND)) > 0 THEN
                                                ROUND(((SAFE_CAST (st_distance(geom2, geom) / 1000 AS NUMERIC) / (TIMESTAMP_DIFF (TIMESTAMP(t2), TIMESTAMP(t1), MILLISECOND) / 3600000)) * 0.539957), 4)
                                        ELSE
                                                NULL
                                        END AS numeric) AS calculated_knots,
                                -- # Get distance in kilometers
                                ROUND(SAFE_CAST (st_distance(geom2, geom) / 1000 AS NUMERIC), 5) AS distance_km,
                                ROUND(SAFE_CAST (st_distance(geom2, geom) * 0.000539957 AS NUMERIC), 5) AS distance_nm,
                                -- # Get time elapsed between points in minutes
                                ROUND((TIMESTAMP_DIFF (TIMESTAMP(t2), TIMESTAMP(t1), SECOND) / 60), 2) AS segment_time_minutes,
                                lon,
                                lat,
                                SAFE_CAST (lead_lon AS FLOAT64) AS lead_lon,
                                SAFE_CAST (lead_lat AS FLOAT64) AS lead_lat,
                                # AIS message source
                                source,
                                gt,
                                -- # Point Geography
                                geom AS point,
                                -- # Linestring Geography
                                ST_MAKELINE (geom,
                                        geom2) AS linestring,
                                -- # Subquery that partitions by mmsi and orders by timestamp in order to construct linestrings
                        FROM (
                                SELECT
                                        timestamp,
                                        ais.mmsi,
                                        --   # Lead,1 speeds as segment speeds, Lead,2 speeds as lead segment speeds, given speeds as lag speeds
                                        SAFE_CAST ((speed_knots) AS NUMERIC) AS lag_speed_knots,
                                        SAFE_CAST ((implied_speed_knots) AS NUMERIC) AS lag_implied_speed_knots,
                                        SAFE_CAST (LEAD((speed_knots), 1) OVER w AS NUMERIC) AS speed_knots,
                                                SAFE_CAST (LEAD((implied_speed_knots), 1) OVER w AS NUMERIC) AS implied_speed_knots,
                                                SAFE_CAST (LEAD((speed_knots), 2) OVER w AS NUMERIC) AS lead_speed_knots,
                                                SAFE_CAST (LEAD((implied_speed_knots), 2) OVER w AS NUMERIC) AS lead_implied_speed_knots,
                                                lon,
                                                lat,
                                                source,
                                                mmsi_list.gt,
                                                row_number() OVER w AS num,
                                                        LEAD(lon) OVER w AS lead_lon,
                                                                LEAD(lat) OVER w AS lead_lat,
                                                                        ST_GeogPoint (lon,
                                                                        lat) AS geom,
                                                                LEAD(ST_GeogPoint (lon, lat)) OVER w AS geom2,
                                                                        LEAD(STRING (timestamp), 0) OVER w AS t1,
                                                                                LEAD(STRING (timestamp), 1) OVER w AS t2,
                                                                                FROM
                                                                                        `benioff-ocean-initiative.whalesafe.gfw_data` AS ais
                                                                                LEFT JOIN `benioff-ocean-initiative.benioff_datasets.mmsi_list` AS mmsi_list ON ais.mmsi = mmsi_list.mmsi
                                                                        WHERE
                                                                                timestamp > new_seg_ts
                                                                                AND mmsi_list.gt >= 300 WINDOW w AS (PARTITION BY ais.mmsi ORDER BY timestamp)) AS q
                                                        WHERE
                                                                geom2 IS NOT NULL
                                                                AND
                                                                -- # Time difference must be greater that 120 minutes
                                                                TIMESTAMP_DIFF (TIMESTAMP(t2), TIMESTAMP(t1), MINUTE) <= 120
                                                        AND
                                                        -- Distance of segment must be BETWEEN 0.001 meters AND 222240 meters (120 nautical miles)
                                                        st_distance(geom2, geom) BETWEEN 0.0001 AND 222240
                                                        AND
                                                        -- # Vessel must be greater than or equal to 300 gross tonage
                                                        gt >= 300));

        -- # Step 3: Create `whalesafe_ais.gfw_ihs_segments` table if not already exists
        CREATE TABLE IF NOT EXISTS `benioff-ocean-initiative.whalesafe.gfw_segments` (
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
                final_speed_bin_num INT64
)
        PARTITION BY DATE(timestamp) CLUSTER BY
                        mmsi, point, linestring OPTIONS (description = "partitioned by day, clustered by (mmsi, point, linestring)", require_partition_filter = TRUE);

                -- # Step 4: Insert data from temporary table `whalesafe.gfw_segments` into BigQuery table
                INSERT INTO `benioff-ocean-initiative.whalesafe.gfw_segments`
                SELECT
                        *
                FROM
                        `temp_gfw_segments`;

                --  Step 5: Create timestamp log if not already existing.
                CREATE TABLE IF NOT EXISTS `benioff-ocean-initiative.whalesafe.timestamp_log` (
                        newest_timestamp TIMESTAMP,
                        date_accessed TIMESTAMP,
                        table_name STRING);

                        -- # Step 6: Insert previous newest timestamp, new_seg_ts, into log
                        INSERT INTO `benioff-ocean-initiative.whalesafe.timestamp_log`
                        SELECT
                                new_seg_ts AS newest_timestamp,
                                CURRENT_TIMESTAMP() AS date_accessed,
                                'gfw_segments' AS table_name;

                        -- # Step 7: Insert the current newest timestamp into log
                        INSERT INTO `benioff-ocean-initiative.whalesafe.timestamp_log`
                        SELECT
                                (
                                        SELECT
                                                MAX(timestamp_end)
                                        FROM
                                                `benioff-ocean-initiative.whalesafe.gfw_segments`
                                        WHERE
                                                DATE(timestamp) > DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
                                        LIMIT 1) AS newest_timestamp,
                                CURRENT_TIMESTAMP() AS date_accessed,
                                'gfw_segments' AS table_name;