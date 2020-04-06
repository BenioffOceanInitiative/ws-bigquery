CREATE TABLE IF NOT EXISTS `benioff-ocean-initiative.scratch_sean.formatted_gfw_data` AS
SELECT 
mmsi, 
timestamp, 
lon, 
lat, 
CAST(speed_knots AS NUMERIC) AS speed_knots, 
CAST(implied_speed_knots AS NUMERIC) AS implied_speed_knots, 
source 
FROM `benioff-ocean-initiative.gfw_sample.bq_results_20190601`
WHERE 
source = "spire";
