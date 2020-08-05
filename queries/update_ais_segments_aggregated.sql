-- # -- Updating `whalesafe_v2.ais_segments_agg` -- # --
-- # -- Necessary for WhaleSafe API and Webpage.
-- # -- Aggregating vessel segments by region, seg_id, speed_bin_num, and date.

-- # Step 0: If starting from scratch, load in 6 month batches. TODO: load by month Python script for BigQuery
-- DECLARE
-- 	new_seg_agg_ts DEFAULT(
-- 		SELECT
-- 			(SAFE_CAST ('2017-01-01 00:00:00 UTC' AS TIMESTAMP)));

-- # Step 1: DECLARE new_seg_agg_ts AS the newest timestamp_end in the ais_segments_agg table
DECLARE
    new_seg_agg_ts DEFAULT(
        SELECT
            MAX(timestamp_end)
                    FROM `benioff-ocean-initiative.whalesafe_v2.ais_segments_agg`
                    WHERE
            date > DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
                    LIMIT 1);

-- # Step 2: Create temporary table to hold aggregated segments data that will update ais_segments_agg table
CREATE TEMPORARY TABLE `temp_ais_segments_agg` (
    mmsi INT64,
    date DATE,
    speed_bin_num INT64,
    seg_id INT64,
    avg_speed_knots NUMERIC,
    avg_implied_speed_knots NUMERIC,
    avg_calculated_knots NUMERIC,
    avg_speed_knots_final NUMERIC,
    total_distance_nm NUMERIC,
    seg_min NUMERIC,
    unix_beg INT64,
    unix_end INT64,
    timestamp_beg TIMESTAMP,
    timestamp_end TIMESTAMP,
    touches_coast BOOL,
    region STRING,
    npts INT64,
    geom GEOGRAPHY
);

-- -- -- # Step 3: Insert aggregated segments data created from gfw_segments data
INSERT INTO `temp_ais_segments_agg` 
SELECT
* 
FROM(
SELECT
* 
EXCEPT(geom)
,
st_numpoints(geom) as npts, -- # Get number of points in each aggregated segment (geom)
-- # -- CASE statement to "iron" out noisy Multilinestrings
-- # -- Use geom if it's a "linestring" otherwise apply increasing amounts of simplification
CASE 
WHEN
LOWER(ST_ASTEXT(geom)) LIKE 'linestring%'
THEN
geom
WHEN
LOWER(ST_ASTEXT(geom)) LIKE 'multilinestring%'
AND
LOWER(ST_ASTEXT(ST_SIMPLIFY(geom, 5))) LIKE 'linestring%'
THEN
ST_SIMPLIFY(geom, 5)
WHEN
LOWER(ST_ASTEXT(geom)) LIKE 'multilinestring%'
AND
LOWER(ST_ASTEXT(ST_SIMPLIFY(geom, 10))) LIKE 'linestring%'
THEN
ST_SIMPLIFY(geom, 10)
WHEN
LOWER(ST_ASTEXT(geom)) LIKE 'multilinestring%'
AND
LOWER(ST_ASTEXT(ST_SIMPLIFY(geom, 50))) LIKE 'linestring%'
THEN
ST_SIMPLIFY(geom, 50)
WHEN
LOWER(ST_ASTEXT(geom)) LIKE 'multilinestring%'
AND
LOWER(ST_ASTEXT(ST_SIMPLIFY(geom, 100))) LIKE 'linestring%'
THEN
ST_SIMPLIFY(geom, 100)
WHEN
LOWER(ST_ASTEXT(geom)) LIKE 'multilinestring%'
AND
LOWER(ST_ASTEXT(ST_SIMPLIFY(geom, 250))) LIKE 'linestring%'
THEN
ST_SIMPLIFY(geom, 250)
WHEN
LOWER(ST_ASTEXT(geom)) LIKE 'multilinestring%'
AND
LOWER(ST_ASTEXT(ST_SIMPLIFY(geom, 500))) LIKE 'linestring%'
THEN
ST_SIMPLIFY(geom, 500)
WHEN
LOWER(ST_ASTEXT(geom)) LIKE 'multilinestring%'
AND
LOWER(ST_ASTEXT(ST_SIMPLIFY(geom, 750))) LIKE 'linestring%'
THEN
ST_SIMPLIFY(geom, 750)
WHEN
LOWER(ST_ASTEXT(geom)) LIKE 'multilinestring%'
AND
LOWER(ST_ASTEXT(ST_SIMPLIFY(geom, 1000))) LIKE 'linestring%'
THEN
ST_SIMPLIFY(geom, 1000)
ELSE
geom
END AS geom
FROM(
SELECT
mmsi,
date,
final_speed_bin_num AS speed_bin_num,
seg_id,
ROUND(AVG( speed_knots ), 2)         AS avg_speed_knots,
ROUND(AVG( implied_speed_knots ), 2) AS avg_implied_speed_knots ,
ROUND(AVG( calculated_knots ), 2)    AS avg_calculated_knots ,
ROUND(AVG( final_speed_knots ), 2)   AS avg_speed_knots_final, -- # get the averaged final_speed_knots 
ROUND(SUM( distance_nm ), 2)         AS total_distance_nm, 
-- # get the summed distance for the aggregated segment
SAFE_CAST(SUM( segment_time_minutes ) AS NUMERIC) AS seg_min,
UNIX_SECONDS(MIN(timestamp_beg)) AS unix_beg,
UNIX_SECONDS(MAX(timestamp_end)) AS unix_end,
MIN(timestamp_beg) AS timestamp_beg, -- # Get beginning timestamp for the aggregated segment
MAX(timestamp_end) AS timestamp_end, -- # Get end timestamp for the aggregated segment
touches_coast,
region,
ST_UNION_AGG(linestring) AS geom 
-- -- # Aggregate individual linestring geometries, grouped by mmsi, date, seg_id, final_speed_bin_num
FROM(
SELECT 
*,
  CASE WHEN seg_chg = 1 THEN
  segid - 1
    ELSE
  segid
    END AS seg_id
  -- # Case statement to fix 'segid' and create accurate seg_id column
FROM(
    SELECT
  *,
    SUM(seg_chg) OVER (PARTITION BY mmsi ORDER BY timestamp) AS segid
    -- # Cumulative sum of the seg_chg column, partitioned by mmsi, ordered by timestamp, to make 'segid'
    FROM(
SELECT 
    * ,
  -- # CASE STATEMENT to get seg_chg (returns 1 when there's change in speed_bin_num from one row to next)
  CASE WHEN final_speed_bin_num = LEAD(final_speed_bin_num) OVER w THEN
  0
    WHEN final_speed_bin_num <> LEAD(final_speed_bin_num) OVER w THEN
  1
    END AS seg_chg,
FROM `benioff-ocean-initiative.whalesafe_v2.ais_segments` 
    WHERE 
        (timestamp) > new_seg_agg_ts -- # Queries gfw_segments for data with a timestamp (partitioning column) Greater than DECLARED new_seg_agg_ts 
        -- AND
        -- date(timestamp) <= DATE_ADD(DATE(new_seg_agg_ts), INTERVAL 6 MONTH)
        AND touches_coast IS FALSE
        WINDOW w AS (PARTITION BY mmsi ORDER BY timestamp) 
        -- # WINDOW function to correctly partition and order segments
)
)
)
GROUP BY
mmsi, 
date,
seg_id, 
final_speed_bin_num,
touches_coast,
region
-- # Fields used to aggregate the gfw_segments
)
)
;

-- # Step 4: Create partitioned and clustered `whalesafe.gfw_segments_agg` if it doesn't already exist
CREATE TABLE IF NOT EXISTS `whalesafe_v2.ais_segments_agg` (
    mmsi INT64,
    date DATE,
    speed_bin_num INT64,
    seg_id INT64,
    avg_speed_knots NUMERIC,
    avg_implied_speed_knots NUMERIC,
    avg_calculated_knots NUMERIC,
    avg_speed_knots_final NUMERIC,
    total_distance_nm NUMERIC,
    seg_min NUMERIC,
    unix_beg INT64,
    unix_end INT64,
    timestamp_beg TIMESTAMP,
    timestamp_end TIMESTAMP,
    touches_coast BOOL,
    region STRING,
    npts INT64,
    geom GEOGRAPHY
)
    PARTITION BY date 
    CLUSTER BY
        mmsi, seg_id, region, geom 
    OPTIONS (description = "partitioned by day, clustered by (mmsi, seg_id, region, geom)", 
           require_partition_filter = TRUE);

-- -- -- # Step 5: Insert everything from temporary table into `whalesafe.gfw_segments_agg`
INSERT INTO `whalesafe_v2.ais_segments_agg`
    SELECT
        *
    FROM
        `temp_ais_segments_agg`;

-- -- -- # Step 6: Create timestamp_log if not already existing
    CREATE TABLE IF NOT EXISTS 
    `whalesafe_v2.whalesafe_timestamp_log` (
        newest_timestamp TIMESTAMP,
        date_accessed TIMESTAMP,
        table_name STRING,
        query_exec STRING
);

-- -- # Step 7: Insert DECLARED new_seg_agg_ts, newest timestamp_end in ais_segments_agg FROM BEFORE UPDATING
-- -- # QUERY EXECUTION (query_exec): 'query_start'
INSERT INTO `whalesafe_v2.whalesafe_timestamp_log`
    SELECT
        new_seg_agg_ts                  AS newest_timestamp,
        CURRENT_TIMESTAMP() AS date_accessed,
        'ais_segments_agg'            AS table_name,
        'query_start'                        AS query_exec;

-- -- # Step 8: Insert newest timestamp_end in ais_segments_agg FROM AFTER UPDATING
-- -- # QUERY EXECUTION (query_exec): 'query_end'
-- -- # should show either, new data has been added (relatively newer timestamp in query_end column)
-- -- # OR, no new data has been added (same timestamp in 'newest_timestamp' in query_start and query_end column)
    INSERT INTO `whalesafe_v2.whalesafe_timestamp_log`
    SELECT
        (
        SELECT
            MAX(timestamp_end)
                    FROM
            `whalesafe_v2.ais_segments_agg`
                    WHERE
            date > DATE_SUB(DATE(new_seg_agg_ts), INTERVAL 21 DAY)
                    LIMIT 1)                   AS newest_timestamp,
        CURRENT_TIMESTAMP() AS date_accessed,
        'ais_segments_agg'            AS table_name,
        'query_end'                         AS query_exec;