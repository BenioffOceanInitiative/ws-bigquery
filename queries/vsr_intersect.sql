CREATE TABLE IF NOT EXISTS `benioff-ocean-initiative.scratch_sean.gfw_ihs_vsr_segments` AS
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
        `benioff-ocean-initiative.test_datasets.ais_ihs_segments_20190601` AS s
        INNER JOIN `benioff-ocean-initiative.benioff_datasets.vsr_zones` AS z 
        ON ST_Intersects(s.linestring, z.geog)
WHERE
        s.timestamp <= z.datetime_end
        AND 
        s.timestamp >= z.datetime_beg;