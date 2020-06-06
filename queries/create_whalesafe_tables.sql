-- # GFW AIS data: `whalesafe.gfw_data` table
CREATE TABLE IF NOT EXISTS `benioff-ocean-initiative.whalesafe.gfw_data` (
        mmsi INT64,
        timestamp TIMESTAMP,
        lon FLOAT64,
        lat FLOAT64,
        speed_knots NUMERIC,
        implied_speed_knots NUMERIC,
        source STRING
)
PARTITION BY DATE(timestamp) CLUSTER BY
                mmsi OPTIONS (description = "partitioned by day, clustered by (mmsi)", require_partition_filter = TRUE);

-- # Individual segments table: `whalesafe.gfw_segments`
CREATE TABLE IF NOT EXISTS `benioff-ocean-initiative.whalesafe.gfw_segments` (
        timestamp TIMESTAMP,
        date DATE,
        mmsi INT64,
        num INT64,
        timestamp_beg TIMESTAMP,
        timestamp_end TIMESTAMP,
        speed_knots NUMERIC,
        implied_speed_knots NUMERIC,
        calculated_knots NUMERIC,
        distance_km NUMERIC,
        distance_nm NUMERIC,
        segment_time_minutes FLOAT64,
        lon FLOAT64,
        lat FLOAT64,
        lead_lon FLOAT64,
        lead_lat FLOAT64,
        source STRING,
        gt NUMERIC,
        point GEOGRAPHY,
        linestring GEOGRAPHY,
        final_speed_knots NUMERIC,
        speed_bin_num INT64,
        implied_speed_bin_num INT64,
        calculated_speed_bin_num INT64,
        final_speed_bin_num INT64
)
PARTITION BY DATE(timestamp) CLUSTER BY
                mmsi, point, linestring OPTIONS (description = "partitioned by day, clustered by (mmsi, point, linestring)", require_partition_filter = TRUE);

-- #  Aggregated segments table: `whalesafe.gfw_segments_agg`
CREATE TABLE IF NOT EXISTS `benioff-ocean-initiative.whalesafe.gfw_segments_agg` (
        mmsi INT64,
        date DATE,
        speed_bin_num INT64,
        seg_id INT64,
        avg_speed_knots NUMERIC,
        avg_implied_speed_knots NUMERIC,
        avg_calculated_knots NUMERIC,
        avg_speed_knots_1 NUMERIC,
        avg_speed_knots_final NUMERIC,
        total_distance_nm NUMERIC,
        seg_min NUMERIC,
        unix_beg INT64,
        unix_end INT64,
        timestamp_beg TIMESTAMP,
        timestamp_end TIMESTAMP,
        geom GEOGRAPHY,
        npts INT64
)
PARTITION BY date CLUSTER BY
        mmsi, seg_id, geom OPTIONS (description = "partitioned by day, clustered by (mmsi, seg_id, geom)", require_partition_filter = TRUE);

-- #  Individual segments intersecting vsr zones: `whalesafe.gfw_vsr_segments`
CREATE TABLE IF NOT EXISTS `benioff-ocean-initiative.whalesafe.gfw_vsr_segments` (
        timestamp TIMESTAMP,
        date DATE,
        mmsi INT64,
        num INT64,
        timestamp_beg TIMESTAMP,
        timestamp_end TIMESTAMP,
        speed_knots NUMERIC,
        implied_speed_knots NUMERIC,
        calculated_knots NUMERIC,
        distance_km NUMERIC,
        distance_nm NUMERIC,
        segment_time_minutes FLOAT64,
        lon FLOAT64,
        lat FLOAT64,
        lead_lon FLOAT64,
        lead_lat FLOAT64,
        source STRING,
        gt NUMERIC,
        point GEOGRAPHY,
        linestring GEOGRAPHY,
        final_speed_knots NUMERIC,
        speed_bin_num INT64,
        implied_speed_bin_num INT64,
        calculated_speed_bin_num INT64,
        final_speed_bin_num INT64,
        vsr_category STRING,
        vsr_linestring GEOGRAPHY
)
PARTITION BY DATE(timestamp) CLUSTER BY
                mmsi, linestring, vsr_linestring OPTIONS (description = "partitioned by day, clustered by (mmsi, linestring, vsr_linestring)", require_partition_filter = TRUE);

-- # Aggregated segments intersecting VSR zones: `whalesafe.gfw_vsr_segments_agg`
CREATE TABLE IF NOT EXISTS `benioff-ocean-initiative.whalesafe.gfw_vsr_segments_agg` (
        mmsi INT64,
        date DATE,
        speed_bin_num INT64,
        seg_id INT64,
        avg_speed_knots NUMERIC,
        avg_implied_speed_knots NUMERIC,
        avg_calculated_knots NUMERIC,
        avg_speed_knots_1 NUMERIC,
        avg_speed_knots_final NUMERIC,
        total_distance_nm NUMERIC,
        seg_min NUMERIC,
        unix_beg INT64,
        unix_end INT64,
        timestamp_beg TIMESTAMP,
        timestamp_end TIMESTAMP,
        geom GEOGRAPHY,
        npts INT64,
        vsr_category STRING,
        vsr_geom GEOGRAPHY
)
PARTITION BY date CLUSTER BY
        mmsi, seg_id, geom, vsr_geom OPTIONS (description = "partitioned by day, clustered by (mmsi, seg_id, geom, vsr_geom)", require_partition_filter = TRUE);