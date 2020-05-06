#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr  9 12:31:09 2020

@author: seang
"""

from google.cloud import bigquery
from google.oauth2 import service_account
from jinjasql import JinjaSql
 
#FILL IN YOUR PATH TO THE 'Benioff Ocean Initiative-454f666d1896.json'
credentials_json = '/Users/seangoral/bq_api_test/benioff-ocean-initiative-cc06de0db3ce.json'

credentials = service_account.Credentials.from_service_account_file(credentials_json)

project_id = 'benioff-ocean-initiative'
client = bigquery.Client(credentials= credentials,project=project_id)

stats_query = '''CREATE TABLE IF NOT EXISTS `benioff-ocean-initiative.scratch.mmsi_cooperation_stats` AS
SELECT *,
total_distance_km_under_10 + total_distance_km_btwn_10_12 + total_distance_km_btwn_12_15 + total_distance_km_over_15 AS total_dist_check,
(total_distance_km_under_10/total_distance_km) * 100 AS coop_score
FROM(
SELECT
DISTINCT(mmsi),
operator,
#Average speed in knots
AVG(implied_speed_knots) AS avg_speed_knots,  
# Total distance in km where speed <= 10 knots
SUM(CASE  
             WHEN implied_speed_knots <= 10 
             AND implied_speed_knots > 0 
             THEN distance/1000 
             ELSE 0 
           END) AS total_distance_km_under_10,
# Total distance in km where speed > 10 knots and <= 12 knots
SUM(CASE  
             WHEN implied_speed_knots <= 12 
             AND implied_speed_knots > 10 
             THEN distance/1000 
             ELSE 0 
           END) AS total_distance_km_btwn_10_12,
# Total distance in km where speed > 12 knots and <= 15 knots
SUM(CASE  
             WHEN implied_speed_knots <= 15 
             AND implied_speed_knots > 12 
             THEN distance/1000 
             ELSE 0 
           END) AS total_distance_km_btwn_12_15,
# Total distance in km where speed > 15 knots
SUM(CASE  
             WHEN implied_speed_knots > 15 
             AND implied_speed_knots < 100 
             THEN distance/1000 
             ELSE 0 
           END) AS total_distance_km_over_15,
SUM(CASE  
             WHEN implied_speed_knots <= 15 
             THEN distance/1000 
             ELSE 0 
           END) AS total_distance_km_10_noaa,
SUM(distance)/1000 AS total_distance_km
FROM `benioff-ocean-initiative.whalesafe_ais.gfw_ihs_segments` 
WHERE STRING(timestamp) 
BETWEEN '{{ user_date_1 }}'
AND '{{ user_date_2 }}'
GROUP BY mmsi, operator)
ORDER BY coop_score, total_distance_km DESC;'''

params = {
    'user_date_1': '2019-01-01',
    'user_date_2': '2019-03-31',
}


#j = JinjaSql(param_style='pyformat')
#query, bind_params = j.prepare_query(stats_query, params)

#query_job = client.query(query % bind_params)
#tst=query_job.result() 
 
def get_new_ts ():      
    sql = """SELECT MAX(newest_timestamp) as newest_timestamp 
    FROM `benioff-ocean-initiative.benioff_datasets.newest_gfw_timestamp`;"""
    df = client.query(sql)
    for result in df:
        return(result[0])

new_ts = get_new_ts().strftime('%Y-%m-%d %H:%M:%S')

para = {'newest_ts': new_ts}

q = '''SELECT
 ssvid AS mmsi,
 timestamp,
 lat,
 lon,
 speed_knots,
 implied_speed_knots,
 source
FROM
 `world-fishing-827.gfw_research.pipe_v20190502`
WHERE 
 date >= '{{ newest_ts }}' LIMIT 1000;'''
j = JinjaSql(param_style='pyformat')
query, bind_params = j.prepare_query(q, para)

df_ts = client.query(query % bind_params).to_dataframe()
