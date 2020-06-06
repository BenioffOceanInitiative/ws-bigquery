--  # Step 0:
--  # IF STARTING FROM SCRATCH, USE LINES BELOW:
-- DECLARE
-- 	new_seg_agg_ts DEFAULT(
-- 		SELECT
-- 			SAFE_CAST ('2019-12-31 23:59:48 UTC' AS TIMESTAMP));

-- # If UPDATING, use DECLARE STATEMENT BELOW
DECLARE
        new_seg_agg_ts DEFAULT(
                SELECT
                        MAX(timestamp_end)
                        FROM `benioff-ocean-initiative.whalesafe.gfw_segments_agg`
                WHERE
                        date > DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
                LIMIT 1);

        -- # Step 1: Create temporary table to inesrt aggregated segments data for updating `gfw_segments_agg` table in BigQuery
        CREATE TEMPORARY TABLE `temp_gfw_segments_agg` (
                mmsi INT64,
                date DATE,
                speed_bin_num INT64,
                seg_id INT64,
                avg_speed_knots NUMERIC,
                avg_implied_speed_knots NUMERIC,
                avg_calculated_knots NUMERIC,
                avg_speed_knots_1 NUMERIC,
                avg_speed_knots_final NUMERIC,
                total_distance_nm NUMERIC,
                seg_min NUMERIC,
                unix_beg INT64,
                unix_end INT64,
                timestamp_beg TIMESTAMP,
                timestamp_end TIMESTAMP,
                geom GEOGRAPHY,
                npts INT64
);

        -- # Step 2: Insert data from `whalesafe.gfw_data` into temporary table that has a timestamp greater than the declared new_seg_agg_ts
        INSERT INTO `temp_gfw_segments_agg`
        SELECT
                *,
                SAFE_CAST (ST_NUMPOINTS(geom) AS int64) AS npts
        FROM (
                -- # QUERY TO PULL IT ALL TOGETHER:
                -- # GROUP BY mmsi, date, speed_bin_num, seg_id
                -- # ST_UNION line (LINESTRING) if the MAX(t2) - MIN(t1) is less than or equal to 120 minutes
                SELECT
                        mmsi,
                        date,
                        SAFE_CAST (speed_bin_num AS INT64) AS speed_bin_num,
                        SAFE_CAST (seg_id AS INT64) AS seg_id,
                        ROUND(AVG(speed_knots), 2) AS avg_speed_knots,
                        ROUND(AVG(implied_speed_knots), 2) AS avg_implied_speed_knots,
                        ROUND(AVG(calculated_knots), 2) AS avg_calculated_knots,
                        ROUND(AVG(speed_knots_1), 2) AS avg_speed_knots_1,
                        ROUND(AVG(speed_knots_2), 2) AS avg_speed_knots_final,
                        SAFE_CAST (ROUND(SUM(distance_nm), 2) AS NUMERIC) AS total_distance_nm,
                        SAFE_CAST (ROUND((TIMESTAMP_DIFF (MAX(TIMESTAMP(t2)), MIN(TIMESTAMP(t1)), SECOND) / 60), 2) AS NUMERIC) AS seg_min,
                        UNIX_SECONDS (SAFE_CAST (MIN(t1) AS TIMESTAMP)) AS unix_beg,
                        UNIX_SECONDS (SAFE_CAST (MAX(t2) AS TIMESTAMP)) AS unix_end,
                        SAFE_CAST (MIN(t1) AS TIMESTAMP) AS timestamp_beg,
                        SAFE_CAST (MAX(t2) AS TIMESTAMP) AS timestamp_end,
                        CASE WHEN (TIMESTAMP_DIFF (MAX(TIMESTAMP(t2)),
                                        MIN(TIMESTAMP(t1)),
                                        SECOND) / 60) <= 120 THEN
                                ST_UNION_AGG (line)
                        ELSE
                                NULL
                        END AS geom
                FROM (
                        SELECT
                                *,
                                CASE WHEN seg_chg = 1 THEN
                                        segid - 1
                                ELSE
                                        segid
                                END AS seg_id
                        FROM (
                                -- # SUBQUERY to assign seg_id based on cumulative sum of seg_chg
                                SELECT
                                        *,
                                        SUM(seg_chg) OVER (ORDER BY date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS segid,
                                FROM (
                                -- # SUBQUERY TO GET MAKE line, LINESTRING for ST_UNION above.
                                -- # Also calculate distance in nautical miles, and seg_chg (whether the next row has a channge in speed_bin_num)
                                SELECT
                                        *,
                                        (ST_DISTANCE(geom, geom2) * 0.000539957) AS distance_nm,
                                -- # CASE STATEMENT to get seg_chg (change in speed_bin_num from one row to next)
                                CASE WHEN speed_bin_num = LEAD(speed_bin_num) OVER w1 THEN
                                        0
                                WHEN speed_bin_num <> LEAD(speed_bin_num) OVER w1 THEN
                                        1
                                END AS seg_chg,
                                ST_MAKELINE (geom,
                                geom2) AS line,
                FROM (
                -- # SUBQUERY TO ASSIGN speed_bin_num BASED ON speed_knots_2
                SELECT
                        *,
                        -- # CASE STATEMENT FOR speed_bin_num
                        CASE WHEN speed_knots_2 = 0 THEN
                                0
                        WHEN speed_knots_2 > 0
                                AND speed_knots_2 <= 10 THEN
                                1
                        WHEN speed_knots_2 > 10
                                AND speed_knots_2 <= 12 THEN
                                2
                        WHEN speed_knots_2 > 12
                                AND speed_knots_2 <= 15 THEN
                                3
                        WHEN speed_knots_2 > 15
                                AND speed_knots_2 <= 50 THEN
                                4
                        WHEN speed_knots_2 > 50 THEN
                                5
                        ELSE
                                NULL
                        END AS speed_bin_num
                FROM (
                --  # SUBQUERY TO GET WHICHEVER SPEED (speed_knots_1 or calculated_knots is between 0.00001 and 50 knots
                SELECT
                        *,
                        CASE WHEN calculated_knots > 0
                                AND calculated_knots <= 50 THEN
                                calculated_knots
                        WHEN speed_knots_1 > 0
                                AND speed_knots_1 <= 50 THEN
                                speed_knots_1
                        ELSE
                                NULL
                        END AS speed_knots_2
                FROM (
                --  # SUBQUERY TO CALCULATE SPEEDS
                SELECT
                        *,
                        -- # CASE STATEMENT TO calculate speeds
                        SAFE_CAST ( CASE WHEN (TIMESTAMP_DIFF (TIMESTAMP(t2),
                        TIMESTAMP(t1),
                        MILLISECOND)) > 0 THEN
                                ROUND(((CAST(st_distance(geom2, geom) / 1000 AS NUMERIC) / (TIMESTAMP_DIFF (TIMESTAMP(t2), TIMESTAMP(t1), MILLISECOND) / 3600000)) * 0.539957), 4)
                        ELSE
                                NULL
                        END AS NUMERIC) AS calculated_knots
        FROM (
        -- # SUBQUERY to get geom and geom2, as well as t1 and t2, and speed_knots_1
        SELECT
                ais.mmsi,
                ais.timestamp,
                DATE(ais.timestamp) AS date,
                ais.lon,
                ais.lat,
                ais.speed_knots,
                ais.implied_speed_knots,
                --     # CASE STATEMENT TO USE WHICHEVER SPEED IS BETWEEN 0.00001 AND 50 KNOTS.
                --     # Reported speed first, then GFW calc. speed
                CASE WHEN ais.implied_speed_knots > 0
                        AND ais.implied_speed_knots <= 50 THEN
                        ais.implied_speed_knots
                WHEN ais.implied_speed_knots = 0
                        AND ais.implied_speed_knots > 50
                        AND ais.speed_knots > 0
                        AND ais.speed_knots <= 50 THEN
                        ais.speed_knots
                ELSE
                        NULL
                END AS speed_knots_1,
                -- # assign row nmber for each linestring made
                --     row_number() OVER w AS num,
                -- # Create geom and geom2 (point 1 and point2 to make linstrings above.)
                ST_GeogPoint (ais.lon,
                ais.lat) AS geom,
        LEAD(ST_GeogPoint (ais.lon, ais.lat)) OVER w AS geom2,
                --   # Timestamp 1 (t1) and timestamp2 (t2)
                LEAD(STRING (ais.timestamp), 0) OVER w AS t1,
                        LEAD(STRING (ais.timestamp), 1) OVER w AS t2
                        FROM
                                `whalesafe.gfw_data` AS ais
                        LEFT JOIN `benioff-ocean-initiative.benioff_datasets.mmsi_list` AS ihs ON ais.mmsi = ihs.mmsi
                WHERE
                        timestamp > new_seg_agg_ts
                        AND gt >= 300 WINDOW w AS (PARTITION BY ais.mmsi ORDER BY timestamp)))))
WINDOW w1 AS (PARTITION BY mmsi ORDER BY
        timestamp))))
GROUP BY
        seg_id,
        mmsi,
        date,
        speed_bin_num)
WHERE
        ST_NUMPOINTS(geom) > 1
        AND geom IS NOT NULL
        AND total_distance_nm > 0
        AND total_distance_nm < 100;

        -- # Step 3: Create `whalesafe.gfw_segments_agg` if it's not already made.
        CREATE TABLE IF NOT EXISTS `benioff-ocean-initiative.whalesafe.gfw_segments_agg` (
                mmsi INT64,
                date DATE,
                speed_bin_num INT64,
                seg_id INT64,
                avg_speed_knots NUMERIC,
                avg_implied_speed_knots NUMERIC,
                avg_calculated_knots NUMERIC,
                avg_speed_knots_1 NUMERIC,
                avg_speed_knots_final NUMERIC,
                total_distance_nm NUMERIC,
                seg_min NUMERIC,
                unix_beg INT64,
                unix_end INT64,
                timestamp_beg TIMESTAMP,
                timestamp_end TIMESTAMP,
                geom GEOGRAPHY,
                npts INT64
)
        PARTITION BY date CLUSTER BY
                mmsi, seg_id, geom OPTIONS (description = "partitioned by day, clustered by (mmsi, seg_id, geom)", require_partition_filter = TRUE);

                -- # Step 4: Insert everything from temporary table into BigQuery table
                INSERT INTO `benioff-ocean-initiative.whalesafe.gfw_segments_agg`
                SELECT
                        *
                FROM
                        `temp_gfw_segments_agg`;
                
                -- # Step 5: Make timestamp log if not already existing.
                CREATE TABLE IF NOT EXISTS `benioff-ocean-initiative.whalesafe.timestamp_log` (
                        newest_timestamp TIMESTAMP,
                        date_accessed TIMESTAMP,
                        table_name STRING);

                        -- # Step 6: Insert previous newest timestamp, new_seg_agg_ts, into `timestamp_log`
                        INSERT INTO `benioff-ocean-initiative.whalesafe.timestamp_log`
                        SELECT
                                new_seg_agg_ts AS newest_timestamp,
                                CURRENT_TIMESTAMP() AS date_accessed,
                                'gfw_segments_agg' AS table_name;

                        -- # Step 7: Insert updated newest timestamp into `timestamp_log` 
                        INSERT INTO `benioff-ocean-initiative.whalesafe.timestamp_log`
                        SELECT
                                (
                                        SELECT
                                                MAX(timestamp_end)
                                        FROM
                                                `benioff-ocean-initiative.whalesafe.gfw_segments_agg`
                                        WHERE
                                                date > DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
                                        LIMIT 1) AS newest_timestamp,
                                CURRENT_TIMESTAMP() AS date_accessed,
                                'gfw_segments_agg' AS table_name;