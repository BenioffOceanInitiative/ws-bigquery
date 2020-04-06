CREATE TEMPORARY TABLE `temp_gfw_data`(
mmsi INT64, 
timestamp TIMESTAMP, 
lon FLOAT64, 
lat FLOAT64,
speed_knots NUMERIC,
implied_speed_knots NUMERIC,
source STRING
);

INSERT INTO `temp_gfw_data`
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

INSERT INTO `benioff-ocean-initiative.whalesafe_ais.gfw_data`
SELECT 
* 
FROM `temp_gfw_data`;