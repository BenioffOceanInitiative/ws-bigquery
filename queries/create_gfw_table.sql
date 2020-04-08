CREATE TABLE IF NOT EXISTS 
`benioff-ocean-initiative.whalesafe_ais.gfw_data` (
mmsi INT64, 
timestamp TIMESTAMP, 
lon FLOAT64, 
lat FLOAT64,
speed_knots NUMERIC,
implied_speed_knots NUMERIC
);