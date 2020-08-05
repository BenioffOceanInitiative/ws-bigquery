-- --  # Step 0:
-- --  # IF STARTING FROM SCRATCH, USE LINES BELOW:
-- DECLARE
-- new_gfw_ts DEFAULT
--  (SELECT SAFE_CAST('1990-01-01 00:00:00' AS TIMESTAMP));

-- # IF UPDATING: Declare newest GFW timestamp from the `whalesafe.gfw_data` table as 'new_gfw_ts'
DECLARE
        new_gfw_ts DEFAULT(
                SELECT
                        MAX(timestamp)
                        FROM `benioff-ocean-initiative.whalesafe.gfw_data`
                WHERE
                        DATE(timestamp) > DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
                LIMIT 1);

        -- # Step 1: Make temporary table `temp_gfw_data` for incoming GFW data
        CREATE TEMPORARY TABLE `temp_gfw_data` (
                mmsi INT64,
                timestamp TIMESTAMP,
                lon FLOAT64,
                lat FLOAT64,
                speed_knots NUMERIC,
                implied_speed_knots NUMERIC,
                source STRING
);

        -- # Step 2: Insert GFW data from gfw pipeline (with a timestamp > new_gfw_ts, the max timestamp in the existing `benioff-ocean-initiative.whalesafe.gfw_data` table) into the `temp_gfw_ihs_data` table
        INSERT INTO `temp_gfw_data`
        SELECT
                SAFE_CAST (ais.ssvid AS INT64) AS mmsi,
                ais.timestamp,
                ais.lon,
                ais.lat,
                SAFE_CAST (ais.speed_knots AS NUMERIC) AS speed_knots,
                SAFE_CAST (ais.implied_speed_knots AS NUMERIC) AS implied_speed_knots,
                ais.source
        FROM
                `world-fishing-827.gfw_research.pipe_v20190502` AS ais
        WHERE
                date > new_gfw_ts
                AND lat >= (33.3011)
                AND lat <= (34.5738)
                AND lon >= (- 121.0299)
                AND lon <= (- 117.4998)
                --  -- #AND source = 'spire' # include ORBCOMM
;

        -- # Step 3: Create empty whalesafe_ais GFW table if not already existing
        -- # `whalesafe.gfw_data` table
        CREATE TABLE IF NOT EXISTS `benioff-ocean-initiative.whalesafe.gfw_data` (
                mmsi INT64,
                timestamp TIMESTAMP,
                lon FLOAT64,
                lat FLOAT64,
                speed_knots NUMERIC,
                implied_speed_knots NUMERIC,
                source STRING
)
        PARTITION BY DATE(timestamp) CLUSTER BY
                        mmsi OPTIONS (description = "partitioned by day, clustered by (mmsi)", require_partition_filter = TRUE);

                -- # Step 4: Insert everything from `temp_gfw_ihs_data` table into the clustered tables,
                --  # `gfw_data` table
                INSERT INTO `benioff-ocean-initiative.whalesafe.gfw_data`
                SELECT
                        *
                FROM
                        `temp_gfw_data`;

                --  Step 5: Make timestamp log if not already existing.
                CREATE TABLE IF NOT EXISTS `benioff-ocean-initiative.whalesafe.timestamp_log` (
                        newest_timestamp TIMESTAMP,
                        date_accessed TIMESTAMP,
                        table_name STRING
);

                        -- # Step 6: Insert previous newest timestamp into log
                        INSERT INTO `benioff-ocean-initiative.whalesafe.timestamp_log`
                        SELECT
                                new_gfw_ts AS newest_timestamp,
                                CURRENT_TIMESTAMP() AS date_accessed,
                                'gfw_data' AS table_name;

                        -- # Step 7: Insert the current newest timestamp into log
                        INSERT INTO `benioff-ocean-initiative.whalesafe.timestamp_log`
                        SELECT
                                (
                                        SELECT
                                                MAX(timestamp)
                                        FROM
                                                `benioff-ocean-initiative.whalesafe.gfw_data`
                                        WHERE
                                                DATE(timestamp) > DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
                                        LIMIT 1) AS newest_timestamp,
                                CURRENT_TIMESTAMP() AS date_accessed,
                                'gfw_data' AS table_name;