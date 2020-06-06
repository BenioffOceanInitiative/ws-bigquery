-- # Step 0: Declare 'new_vsr_seg_agg_ts'
-- # If starting from scratch, USE DECLARE STATEMENT BELOW:
-- DECLARE
-- 	new_vsr_seg_agg_ts DEFAULT(
-- 		SELECT
-- 			(SAFE_CAST ('1990-01-01' AS DATE)));

-- # If UPDATING, USE DECLARE STATEMENT BELOW, AND COMMENT OFF DECLARE STATEMENT ABOVE:
-- # Since VSR season is not always active, this statement can return NULL if the new VSR polygon and date range has not been added to the `benioff_datasets.vsr_zones` table. SEE GFW AIS METHODOLOGY FOR DETAILS.
DECLARE
    new_vsr_seg_agg_ts DEFAULT(
            SELECT
                CASE WHEN (
                            SELECT
                                MAX(date)
                                        FROM `benioff-ocean-initiative.whalesafe.gfw_vsr_segments_agg`
                                        WHERE
                                date > DATE_SUB(CURRENT_DATE(), INTERVAL 9 MONTH)
                                        LIMIT 1) IS NULL THEN
                        '2020-01-01'
                                ELSE
                        (
                                    SELECT
                                        MAX(date)
                                                FROM `benioff-ocean-initiative.whalesafe.gfw_vsr_segments_agg`
                                                WHERE
                                        date > DATE_SUB(CURRENT_DATE(), INTERVAL 9 MONTH)
                                                LIMIT 1)
                                END);

    -- # Step 1: Create temporary table to hold new vsr segments aggregated data.
    CREATE TEMPORARY TABLE `temp_gfw_vsr_segments_agg` (
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
                npts INT64,
                vsr_category STRING,
                vsr_geom GEOGRAPHY
);

    -- # Step 2: INSERT new aggregated segments data (with a date greater than 'new_vsr_seg_agg_ts') that intersects VSR Zones into `temp_gfw_ihs_vsr_segments`
    INSERT INTO `temp_gfw_vsr_segments_agg`
    SELECT
        s.*,
        z.vsr_category,
        # Spatial intersection case statement
        CASE WHEN ST_CoveredBy (s.geom,
                                        z.geog) THEN
            s.geom
        ELSE
            ST_Intersection(s.geom, z.geog)
        END AS vsr_geom
    FROM
        `benioff-ocean-initiative.whalesafe.gfw_segments_agg` AS s
        INNER JOIN `benioff-ocean-initiative.benioff_datasets.vsr_zones` AS z ON ST_Intersects(s.geom, z.geog)
    WHERE
        s.timestamp_end <= z.datetime_end
        AND s.timestamp_beg >= z.datetime_beg
        AND s.date > new_vsr_seg_agg_ts
        AND s.date <= (
                    SELECT
                        MAX(DATE(datetime_end))
                                FROM
                        `benioff-ocean-initiative.benioff_datasets.vsr_zones`
                                LIMIT 1);

    -- # Step 3: Create `whalesafe.gfw_vsr_segments_agg` table if not already existing.
    CREATE TABLE IF NOT EXISTS `benioff-ocean-initiative.whalesafe.gfw_vsr_segments_agg` (
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
                npts INT64,
                vsr_category STRING,
                vsr_geom GEOGRAPHY
)
    PARTITION BY date CLUSTER BY
        mmsi, seg_id, geom, vsr_geom OPTIONS (description = "partitioned by day, clustered by (mmsi, seg_id, geom, vsr_geom)", require_partition_filter = TRUE);

        -- # Step 4: Insert everything from temporary table into BigQuery table
        INSERT INTO `benioff-ocean-initiative.whalesafe.gfw_vsr_segments_agg`
        SELECT
            *
        FROM
            `temp_gfw_vsr_segments_agg`;

        -- # Step 5: Insert 'new_vsr_seg_agg_ts' into timestamp log. 
        -- # It's actually a date so it's CAST to TIMESTAMP data type.
        INSERT INTO `benioff-ocean-initiative.whalesafe.timestamp_log`
        SELECT
            TIMESTAMP(new_vsr_seg_agg_ts) AS newest_timestamp,
            CURRENT_TIMESTAMP() AS date_accessed,
            'gfw_vsr_segments_agg' AS table_name;

        -- # Step 6: Insert max(timestamp_end) from `whalesafe.gfw_vsr_segments_agg` into timestamp log.
        -- # This is actually a TIMESTAMP. 
        INSERT INTO `benioff-ocean-initiative.whalesafe.timestamp_log`
        SELECT
            (
                            SELECT
                                MAX(timestamp_end)
                                        FROM
                                `benioff-ocean-initiative.whalesafe.gfw_vsr_segments_agg`
                                        WHERE
                                date > DATE_SUB(CURRENT_DATE(), INTERVAL 9 MONTH)
                                        LIMIT 1) AS newest_timestamp,
            CURRENT_TIMESTAMP() AS date_accessed,
            'gfw_vsr_segments_agg' AS table_name;