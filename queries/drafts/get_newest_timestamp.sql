-- # Declare new_ts as the newest timestamp in our `gfw_ihs_data` for querying the GFW data pipe
DECLARE new_ts DEFAULT 
((SELECT MAX(timestamp) 
FROM `benioff-ocean-initiative.whalesafe_ais.gfw_data` 
WHERE DATE(timestamp) > DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH)) LIMIT 1);

-- # STEP 0: CREATE gfw_timestamp_log table and add newest gfw_ihs_data timestamp for future queries and logging
CREATE TABLE IF NOT EXISTS 
`benioff-ocean-initiative.benioff_datasets.gfw_timestamp_log` (
newest_timestamp TIMESTAMP,
date_accessed TIMESTAMP,
);