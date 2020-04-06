CREATE TEMPORARY TABLE `temp_gfw_ihs_data` (
mmsi INT64, 
timestamp TIMESTAMP, 
lon FLOAT64, 
lat FLOAT64,
speed_knots NUMERIC,
implied_speed_knots NUMERIC,
source STRING,
X	INT64,	
imo_lr_ihs_no INT64,	
name_of_ship STRING,	
callsign STRING,	
shiptype STRING,	
length FLOAT64,	
gt INT64,	
group_owner	STRING,	
technical_manager	STRING,	
ship_manager STRING,	
registered_owner STRING,	
operator STRING,	
operator_code INT64
);

INSERT INTO `temp_gfw_ihs_data`
SELECT 
ais_data.*, 
ihs_data.* EXCEPT(mmsi)
FROM `temp_gfw_data` as ais_data
LEFT JOIN `benioff-ocean-initiative.benioff_datasets.ihs_data` as ihs_data 
ON 
ais_data.mmsi = ihs_data.mmsi;

INSERT INTO `benioff-ocean-initiative.whalesafe_ais.gfw_ihs_data`
SELECT 
*
FROM
temp_gfw_ihs_data;