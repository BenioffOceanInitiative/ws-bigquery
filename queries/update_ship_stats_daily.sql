-- # -- # Updating `whalesafe_v2.ship_stats_daily` -- # --
-- # Benioff Ocean Initiative: 2020-07-30
-- # -- # Run this STATS SCRIPT FIRST.
-- # -- Step 0: DROP EXISTING `ship_stats_daily` TABLE.
-- # -- #TODO: write it so it updates instead using temp tables instead of dropping.

DROP TABLE IF EXISTS `whalesafe_v2.ship_stats_daily`;

-- # -- Step 1: Create updated ship_stats_daily table.
CREATE TABLE IF NOT EXISTS `whalesafe_v2.ship_stats_daily` CLUSTER BY
                mmsi,
                operator,
                coop_score_daily,
                rolling_coop_score AS
                SELECT
                        mmsi,
                        name_of_ship,
                        operator,
                        operator_code,
                        technical_manager,
                        shiptype,
                        ship_category,
                        date,
                        vsr_region,
                        cooperation_score_daily,
                        ROUND((coop_score_daily), 2) AS coop_score_daily,
                        CASE WHEN (
                        coop_score_daily) >= 99 THEN
                                'A+'
                        WHEN (
                                coop_score_daily) < 99
                                AND(
                                        coop_score_daily) >= 80 THEN
                                'A'
                        WHEN (
                                coop_score_daily) < 80
                                AND(
                                        coop_score_daily) >= 60 THEN
                                'B'
                        WHEN (
                                coop_score_daily) < 60
                                AND(
                                        coop_score_daily) >= 40 THEN
                                'C'
                        WHEN (
                                coop_score_daily) < 40
                                AND(
                                        coop_score_daily) >= 20 THEN
                                'D'
                        ELSE
                                'F'
                        END AS coop_grade_daily,
                        ROUND(
                                rolling_coop_score, 1) AS rolling_coop_score,
                        CASE WHEN (
                                rolling_coop_score) >= 99 THEN
                                'A+'
                        WHEN (
                                rolling_coop_score) < 99
                                AND(
                                        rolling_coop_score) >= 80 THEN
                                'A'
                        WHEN (
                                rolling_coop_score) < 80
                                AND(
                                        rolling_coop_score) >= 60 THEN
                                'B'
                        WHEN (
                                rolling_coop_score) < 60
                                AND(
                                        rolling_coop_score) >= 40 THEN
                                'C'
                        WHEN (
                                rolling_coop_score) < 40
                                AND(
                                        rolling_coop_score) >= 20 THEN
                                'D'
                        ELSE
                                'F'
                        END AS rolling_coop_grade,
                        total_distance_nm,
                        total_distance_nm_under_10,
                        percent_under_10_knots,
                        total_distance_nm_btwn_10_12,
                        percent_10_12_knots,
                        total_distance_nm_btwn_12_15,
                        percent_12_15_knots,
                        total_distance_nm_over_15,
                        percent_over_15_knots,
                        total_distance_nm_weird,
                        total_distance_nm_wo_filter,
                        avg_speed_knots,
                        min_timestamp,
                        max_timestamp,
                        gt,
                        day_count,
                        seg_count
                FROM (
                        SELECT
                                ROUND(AVG(coop_score_daily) OVER (PARTITION BY mmsi ORDER BY mmsi, date), 3) AS rolling_coop_score,
                                *
                        FROM (
                        SELECT
                                mmsi,
                                date,
                                vsr_region,
                                name_of_ship,
                                shiptype,
                                IFNULL(ship_category, "OTHER") AS ship_category,
                                operator,
                                operator_code,
                                technical_manager,
                                -- # Get cooperation score
                                CONCAT(ROUND((total_distance_nm_under_10 / total_distance_nm * 100), 1), '%') AS cooperation_score_daily,
                                ((total_distance_nm_under_10 / total_distance_nm * 100)) AS coop_score_daily,
                -- # Assign Grading
                CASE WHEN ((total_distance_nm_under_10 / total_distance_nm) * 100) >= 99 THEN
                        'A+'
                WHEN ((total_distance_nm_under_10 / total_distance_nm) * 100) < 99
AND((total_distance_nm_under_10 / total_distance_nm) * 100) >= 80 THEN
                        'A'
                WHEN ((total_distance_nm_under_10 / total_distance_nm) * 100) < 80
AND((total_distance_nm_under_10 / total_distance_nm) * 100) >= 60 THEN
                        'B'
                WHEN ((total_distance_nm_under_10 / total_distance_nm) * 100) < 60
AND((total_distance_nm_under_10 / total_distance_nm) * 100) >= 40 THEN
                        'C'
                WHEN ((total_distance_nm_under_10 / total_distance_nm) * 100) < 40
AND((total_distance_nm_under_10 / total_distance_nm) * 100) >= 20 THEN
                        'D'
                ELSE
                        'F'
                END AS month_grade,
                -- # Show distances km in each speed bin and Calculate percentages for distances travelled in each speed bin
                total_distance_nm_under_10,
                CONCAT(ROUND((total_distance_nm_under_10 / total_distance_nm * 100), 1), '%') AS percent_under_10_knots,
                --
                total_distance_nm_btwn_10_12,
                CONCAT(ROUND((total_distance_nm_btwn_10_12 / total_distance_nm * 100), 1), '%') AS percent_10_12_knots,
                --
                total_distance_nm_btwn_12_15,
                CONCAT(ROUND((total_distance_nm_btwn_12_15 / total_distance_nm * 100), 1), '%') AS percent_12_15_knots,
                --
                total_distance_nm_over_15,
                CONCAT(ROUND((total_distance_nm_over_15 / total_distance_nm * 100), 1), '%') AS percent_over_15_knots,
                -- # Show total distance traveled in VSR ZONE
                total_distance_nm,
                day_count,
                seg_count,
                -- # Show total distance traveled at speeds > 60 or speeds < 0 in VSR ZONE
                total_distance_nm_weird,
                total_distance_nm_wo_filter,
                -- # Average speed
                avg_speed_knots,
                -- # Max and Min speeds in reported, implied, calculated, and lenient speeds
                max_speed_knots,
                min_speed_knots,
                max_implied_speed_knots,
                min_implied_speed_knots,
                max_final_calculated_speed_knots,
                min_final_calculated_speed_knots,
                max_timestamp,
                min_timestamp,
                gt
        FROM ( SELECT DISTINCT
                (mmsi),
        date,
        vsr_region,
        name_of_ship,
        shiptype,
        ship_category,
        operator,
        operator_code,
        technical_manager,
        gt,
        -- # Average speed in knots
        ROUND(AVG(final_speed_knots), 1) AS avg_speed_knots,
        -- # Total distance in km where speed <= 10 knots
        -- # A little weirdness with there being distance travelled at 0 knots...
        SUM( CASE WHEN final_speed_knots >= 0
                AND final_speed_knots <= 10 THEN
                ((distance_nm))
ELSE
        0
        END) AS total_distance_nm_under_10,
        -- # Total distance in km where speed > 10 knots and <= 12 knots
        SUM( CASE WHEN final_speed_knots > 10
                AND final_speed_knots <= 12 THEN
                ((distance_nm))
ELSE
        0
        END) AS total_distance_nm_btwn_10_12,
        -- # Total distance in km where speed > 12 knots and <= 15 knots
        SUM( CASE WHEN final_speed_knots > 12
                AND final_speed_knots <= 15 THEN
                ((distance_nm))
ELSE
        0
        END) AS total_distance_nm_btwn_12_15,
        -- # Total distance in km where speed > 15 knots
        SUM( CASE WHEN final_speed_knots > 15
                AND final_speed_knots <= 50 THEN
                ((distance_nm))
ELSE
        0
        END) AS total_distance_nm_over_15,
        -- # Checking for weird speeds from edge cases
        SUM( CASE WHEN final_speed_knots <= 0
                AND final_speed_knots > 50
                OR final_speed_knots IS NULL THEN
                distance_nm
        ELSE
                0
        END) AS total_distance_nm_weird,
        SUM( CASE WHEN final_speed_knots >= 0
                AND final_speed_knots <= 50 THEN
                distance_nm
        ELSE
                0
        END) AS total_distance_nm,
        (SUM(distance_nm)) AS total_distance_nm_wo_filter,
-- # Get distinct day count that a vessel transitted the VSR zone
COUNT(num) AS seg_count,
COUNT(DISTINCT (date)) AS day_count,
-- # Check max and min speeds for:
-- # speed_knots (reported)
ROUND(MAX(final_speed_knots), 3) AS max_speed_knots,
ROUND(MIN(final_speed_knots), 3) AS min_speed_knots,
-- # implied_speed_knots (gfw calculated)
ROUND(MAX(implied_speed_knots), 3) AS max_implied_speed_knots,
ROUND(MIN(implied_speed_knots), 3) AS min_implied_speed_knots,
-- # Final calculated speed (most lenient among the implied_speed_knots (gfw calculated) and calculated_knots (BOI calculated speed))
MAX(final_speed_knots) AS max_final_calculated_speed_knots,
MIN(final_speed_knots) AS min_final_calculated_speed_knots,
MAX(timestamp_end) AS max_timestamp,
MIN(timestamp_beg) AS min_timestamp
FROM (
SELECT
        ais.*,
        cats.*
EXCEPT (shiptype)
FROM (
SELECT
        ais.*,
        ihs.*
EXCEPT (mmsi,
gt)
FROM
        `whalesafe_v2.ais_vsr_segments` AS ais
        LEFT JOIN `whalesafe_v2.ihs_data_all` AS ihs ON ais.mmsi = ihs.mmsi
WHERE
        ais.timestamp > '1990-01-01'
        --   AND touches_coast IS TRUE -- # touches_coast FILTER, UNCOMMENT IF NEEDED
                AND ais.timestamp_beg >= TIMESTAMP(ihs.start_date)
                AND ais.timestamp_end <= TIMESTAMP(ihs.end_date)) AS ais
        LEFT JOIN `whalesafe_v2.shiptype_categories` cats ON TRIM(ais.shiptype) = TRIM(cats.shiptype)
WHERE
        ais.gt >= 300)
GROUP BY
        mmsi, name_of_ship, operator, operator_code, technical_manager, shiptype, gt, date, vsr_region, ship_category))
);

-- # -- Step 2: Create a timestamp log to track newest timestamps in stats data.
CREATE TABLE IF NOT EXISTS `whalesafe_v2.stats_log` (
        newest_timestamp TIMESTAMP,
        newest_date DATE,
        date_accessed TIMESTAMP,
        table_name STRING
);

-- # -- Step 3: Insert newest timestamp into log
INSERT INTO `whalesafe_v2.stats_log`
SELECT
        (
                SELECT
                        MAX(max_timestamp)
                FROM
                        `whalesafe_v2.ship_stats_daily`
                LIMIT 1) AS newest_timestamp,
                (
                        SELECT
                                MAX(date)
                        FROM
                                `whalesafe_v2.ship_stats_daily`
                        LIMIT 1) AS newest_date,
                CURRENT_TIMESTAMP() AS date_accessed,
        'ship_stats_daily' AS table_name;