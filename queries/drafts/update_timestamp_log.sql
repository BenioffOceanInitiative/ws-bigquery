-- # STEP 0: CREATE timestamp_log table
-- # Add newest timestamps, the data accessed, and the table name for future queries and logging

CREATE TABLE IF NOT EXISTS `benioff-ocean-initiative.whalesafe.timestamp_log` (
        newest_timestamp TIMESTAMP,
        date_accessed TIMESTAMP,
        table_name STRING
);

-- # STEP 1: Insert newest timestamp (datetime) from gfw_data
INSERT INTO `benioff-ocean-initiative.whalesafe.timestamp_log`
SELECT
        MAX(timestamp) AS newest_timestamp,
        CURRENT_TIMESTAMP() AS date_accessed,
        'gfw_data' AS table_name
FROM
        `benioff-ocean-initiative.whalesafe.gfw_data`
WHERE
        DATE(timestamp) > DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY);

-- # STEP 2: Insert newest timestamp (datetime) from gfw_segments
INSERT INTO `benioff-ocean-initiative.whalesafe.timestamp_log`
SELECT
        MAX(timestamp) AS newest_timestamp,
        CURRENT_TIMESTAMP() AS date_accessed,
        'gfw_segments' AS table_name
FROM
        `benioff-ocean-initiative.whalesafe.gfw_segments`
WHERE
        DATE(timestamp) > DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY);

-- # STEP 3: Insert newest timestamp (datetime) from gfw_segments_agg
INSERT INTO `benioff-ocean-initiative.whalesafe.timestamp_log`
SELECT
        MAX(timestamp_end) AS newest_timestamp,
        CURRENT_TIMESTAMP() AS date_accessed,
        'gfw_segments_agg' AS table_name
FROM
        `benioff-ocean-initiative.whalesafe.gfw_segments_agg`
WHERE
        date > DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY);

-- # STEP 4: Insert newest timestamp (datetime) from gfw_vsr_segments
INSERT INTO `benioff-ocean-initiative.whalesafe.timestamp_log`
SELECT
        MAX(timestamp) AS newest_timestamp,
        CURRENT_TIMESTAMP() AS date_accessed,
        'gfw_vsr_segments' AS table_name
FROM
        `benioff-ocean-initiative.whalesafe.gfw_vsr_segments`
WHERE
        DATE(timestamp) > DATE_SUB(CURRENT_DATE(), INTERVAL 8 MONTH);

-- # STEP 5: Insert newest timestamp (datetime) from gfw_vsr_segments_agg
INSERT INTO `benioff-ocean-initiative.whalesafe.timestamp_log`
SELECT
        MAX(timestamp_end) AS newest_timestamp,
        CURRENT_TIMESTAMP() AS date_accessed,
        'gfw_vsr_segments_agg' AS table_name
FROM
        `benioff-ocean-initiative.whalesafe.gfw_vsr_segments_agg`
WHERE
        date > DATE_SUB(CURRENT_DATE(), INTERVAL 8 MONTH);