CREATE TABLE IF NOT EXISTS `benioff-ocean-initiative.scratch_sean.gfw_ihs_data` AS
SELECT 
ais_data.*, 
ihs_data.* EXCEPT(mmsi)
FROM `benioff-ocean-initiative.gfw_sample.big_query_sample_20190601` as ais_data
LEFT JOIN `benioff-ocean-initiative.benioff_datasets.ihs_data` as ihs_data 
ON 
ais_data.mmsi = ihs_data.mmsi;