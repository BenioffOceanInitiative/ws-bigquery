CREATE TEMPORARY TABLE `temp_gfw_ihs_segments` (
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
  linestring GEOGRAPHY	
);

INSERT INTO `temp_gfw_ihs_segments`
SELECT
timestamp,
TIMESTAMP_DIFF( TIMESTAMP(t2),  TIMESTAMP(t1), MINUTE) AS date_diff_minutes,
mmsi,
num,
speed_knots,
implied_speed_knots,
lon,
lat,
cast(st_distance(geom2, geom) AS numeric) AS distance,
imo_lr_ihs_no,
name_of_ship,
callsign,
shiptype,
length,
gt,
operator,
operator_code,
geom AS point,
ST_MAKELINE (geom, geom2) AS linestring,
FROM (
  SELECT
  timestamp,
  mmsi,
  speed_knots,
  implied_speed_knots,
  lon,
  lat,
  imo_lr_ihs_no,
  name_of_ship,
  callsign,
  shiptype,
  length,
  gt,
  operator,
  operator_code,
  row_number() OVER w AS num,
  ST_GeogPoint(lon,lat) as geom,
  LEAD(ST_GeogPoint(lon,lat)) OVER w AS geom2,
  LEAD(STRING(timestamp), 0) OVER w AS t1, 
  LEAD(STRING(timestamp), 1) OVER w AS t2
  FROM
  `temp_gfw_ihs_data` 
  WINDOW w AS (PARTITION BY mmsi ORDER BY timestamp)) AS q
WHERE
geom2 IS NOT NULL
AND
TIMESTAMP_DIFF( TIMESTAMP(t2),  TIMESTAMP(t1), MINUTE) <= 120
AND 
st_distance(geom2, geom) <= 10000
AND
gt >= 300;

INSERT INTO `benioff-ocean-initiative.whalesafe_ais.gfw_ihs_segments`
SELECT
*
FROM 
`temp_gfw_ihs_segments`;