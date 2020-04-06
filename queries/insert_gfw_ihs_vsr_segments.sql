CREATE TEMPORARY TABLE `temp_gfw_ihs_vsr_segments` (
timestamp	TIMESTAMP,	
date_diff_minutes	INT64,	
mmsi INT64,	
num	INT64,	
speed_knots	NUMERIC,	
implied_speed_knots	NUMERIC,	
lon	FLOAT64,	
lat	FLOAT64,	
distance NUMERIC,	
imo_lr_ihs_no	INT64,	
name_of_ship STRING,	
callsign STRING,	
shiptype STRING,	
length FLOAT64,	
gt INT64,	
operator STRING,	
operator_code	INT64,	
point	GEOGRAPHY,	
linestring GEOGRAPHY,	
vsr_category STRING,	
vsr_linestring GEOGRAPHY
);

INSERT INTO `temp_gfw_ihs_vsr_segments` 
SELECT
s.*,
z.vsr_category,
CASE WHEN 
  ST_CoveredBy (s.linestring, z.geog) 
THEN
  s.linestring
ELSE
  ST_Intersection(s.linestring, z.geog)
END AS 
  vsr_linestring
FROM
  `temp_gfw_ihs_segments` AS s
  INNER JOIN `benioff-ocean-initiative.benioff_datasets.vsr_zones` AS z 
  ON ST_Intersects(s.linestring, z.geog)
WHERE
  s.timestamp <= z.datetime_end
  AND 
  s.timestamp >= z.datetime_beg;
  
INSERT INTO `benioff-ocean-initiative.whalesafe_ais.gfw_ihs_vsr_segments`
SELECT
*
FROM
temp_gfw_ihs_vsr_segments;