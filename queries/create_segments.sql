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
    `benioff-ocean-initiative.test_datasets.big_query_sample_ais_ihs_20190601` WINDOW w AS (PARTITION BY mmsi ORDER BY timestamp)) AS q
WHERE
geom2 IS NOT NULL
AND
TIMESTAMP_DIFF( TIMESTAMP(t2),  TIMESTAMP(t1), MINUTE) <= 120
AND 
st_distance(geom2, geom) <= 10000
AND
gt >= 300;