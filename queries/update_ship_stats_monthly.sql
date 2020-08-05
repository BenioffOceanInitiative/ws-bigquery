-- # -- # Updating `whalesafe_v2.ship_stats_monthly` -- # --
-- # Benioff Ocean Initiative: 2020-07-30
-- # -- Needs `ship_stats_daily` to run.
-- # -- Step 0: Drop existing `ship_stats_monthly` table.

DROP TABLE IF EXISTS `whalesafe_v2.ship_stats_monthly`;

-- # -- Step 1: Create Updated ship_stats_monthly table.
CREATE TABLE IF NOT EXISTS `whalesafe_v2.ship_stats_monthly` CLUSTER BY
    mmsi,
    operator,
    coop_score_monthly,
    vsr_region AS
    SELECT
        mmsi,
        name_of_ship,
        operator,
        operator_code,
        technical_manager,
        shiptype,
        ship_category,
        vsr_region,
        month,
        year,
        coop_score_monthly,
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
        ROUND(AVG(coop_score_monthly) OVER (PARTITION BY mmsi, year ORDER BY mmsi, month, year ROWS BETWEEN 12 PRECEDING AND CURRENT ROW), 3) AS rolling_coop_score,
        ROUND(total_distance_nm, 2) AS total_distance_nm,
        ROUND(total_distance_nm_under_10, 2) AS total_distance_nm_under_10,
        CONCAT(ROUND(((total_distance_nm_under_10 / total_distance_nm) * 100), 2), '%') AS percent_distance_under_10,
        ROUND(total_distance_nm_btwn_10_12, 2) AS total_distance_nm_btwn_10_12,
        CONCAT(ROUND(((total_distance_nm_btwn_10_12 / total_distance_nm) * 100), 2), '%') AS percent_distance_btwn_10_12,
        ROUND(total_distance_nm_btwn_12_15, 2) AS total_distance_nm_btwn_12_15,
        CONCAT(ROUND(((total_distance_nm_btwn_12_15 / total_distance_nm) * 100), 2), '%') AS percent_distance_btwn_12_15,
        ROUND(total_distance_nm_over_15, 2) AS total_distance_nm_over_15,
        CONCAT(ROUND(((total_distance_nm_over_15 / total_distance_nm) * 100), 2), '%') AS percent_distance_over_15,
        ROUND(avg_speed_knots, 2) AS avg_speed_knots,
        min_timestamp,
        max_timestamp,
        gt,
        day_count,
        seg_count
    FROM (
                    SELECT
                        mmsi,
                                name_of_ship,
                                operator,
                                operator_code,
                                technical_manager,
                                shiptype,
                                ship_category,
                                vsr_region,
                                EXTRACT(MONTH FROM date) AS month,
                                EXTRACT(YEAR FROM date) AS year,
                                ROUND(((SUM(total_distance_nm_under_10) / SUM(total_distance_nm)) * 100), 2) AS coop_score_monthly,
                                SUM(total_distance_nm) AS total_distance_nm,
                                SUM(total_distance_nm_under_10) AS total_distance_nm_under_10,
                                SUM(total_distance_nm_btwn_10_12) AS total_distance_nm_btwn_10_12,
                                SUM(total_distance_nm_btwn_12_15) AS total_distance_nm_btwn_12_15,
                                SUM(total_distance_nm_over_15) AS total_distance_nm_over_15,
                                AVG(avg_speed_knots) AS avg_speed_knots,
                                MIN(min_timestamp) AS min_timestamp,
                                MAX(max_timestamp) AS max_timestamp,
                                gt,
                                SUM(day_count) AS day_count,
                                SUM(seg_count) AS seg_count
                                FROM
                        `whalesafe_v2.ship_stats_daily`
                                GROUP BY
                        mmsi,
                                name_of_ship,
                                operator,
                                operator_code,
                                technical_manager,
                                shiptype,
                                ship_category,
                                vsr_region,
                                month,
                                year,
                                gt
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
                `whalesafe_v2.ship_stats_monthly`
                        LIMIT 1) AS newest_timestamp,
        (
                    SELECT
                        DATE(MAX(max_timestamp))
                                FROM
                        `whalesafe_v2.ship_stats_monthly`
                                LIMIT 1) AS newest_date,
        CURRENT_TIMESTAMP() AS date_accessed,
    'ship_stats_monthly' AS table_name;