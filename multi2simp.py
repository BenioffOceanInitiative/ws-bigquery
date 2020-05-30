#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 30 14:29:16 2020

@author: seang
"""
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import geopandas as gpd
import shapely.wkt
import numpy as np

#FILL IN YOUR PATH TO THE 'Benioff Ocean Initiative-454f666d1896.json'
credentials_json = '/Users/seangoral/bq_api_test/venv/Benioff Ocean Initiative-454f666d1896.json'
    
credentials = service_account.Credentials.from_service_account_file(credentials_json)

project_id = 'benioff-ocean-initiative'
client = bigquery.Client(credentials= credentials,project=project_id)

def get_multiline_data():
    sql = """SELECT * 
            FROM 
            `benioff-ocean-initiative.test.r_copy_cat_sample` 
            WHERE date > "2019-01-01";"""
    df = client.query(sql).to_dataframe()
    return(df)
    print(df.info(memory_usage='deep'))
 
def bq2simplified_df(df):
    #df = client.query(sql).to_dataframe()
    
    geometry = df['geom'].map(shapely.wkt.loads)
    df = df.drop('geom', axis=1)
    crs = {'init': 'epsg:4326'}
    gdf = gpd.GeoDataFrame(df, crs=crs, geometry=geometry)
    
    gdf_simplified = gdf
    gdf_simplified["geometry"] = gdf.geometry.simplify(tolerance=0.015, preserve_topology=False)
    
    gdf_simplified['date'] = pd.to_datetime(gdf_simplified['date'], format='%Y-%m-%d')
    
    df1 = pd.DataFrame(gdf_simplified)
    df1['unix_timestamp'] = df1.date.values.astype(np.int64) // 10 ** 9   
    
    df1["wkt"] = df1["geometry"].astype(str)
    
    # pandas drop columns using list of column names
    df1 = df1.drop(['geometry'], axis=1)
    df1 = df1.drop(['avg_speed_knots'], axis=1)
    df1 = df1.drop(['avg_speed_knots_1'], axis=1)
    df1 = df1.drop(['avg_calculated_knots'], axis=1)
    
    df1["avg_speed_knots"] = df1["avg_speed_knots_final"]
    
    df1 = df1.drop(['avg_speed_knots_final'], axis=1)
    df1 = df1.drop(['avg_implied_speed_knots'], axis=1)
    
    return(df1)
    print(df1.info(memory_usage='deep'))

def write_simplified_lines(df1):
    DATASET = "test"
    TABLE = "r_copy_cat_simp_sample"
    
    job_config = bigquery.LoadJobConfig(
        schema = [
            bigquery.SchemaField("mmsi", "INTEGER"),
            bigquery.SchemaField("date", "TIMESTAMP"),
            bigquery.SchemaField("speed_bin_num", "FLOAT"),
            bigquery.SchemaField("seg_id", "INTEGER"),
            bigquery.SchemaField("avg_speed_knots", "NUMERIC"),            
            bigquery.SchemaField("total_distance_nm", "NUMERIC"),
            bigquery.SchemaField("seg_min", "NUMERIC"),
            bigquery.SchemaField("unix_beg", "INTEGER"),
            bigquery.SchemaField("unix_end", "INTEGER"),
            bigquery.SchemaField("timestamp_beg", "TIMESTAMP"),
            bigquery.SchemaField("timestamp_end", "TIMESTAMP"),
            bigquery.SchemaField("npts", "INTEGER"),
            bigquery.SchemaField("unix_timestamp", "INTEGER"),
            bigquery.SchemaField("wkt", "STRING")
        ],
        time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="date"  # field to use for partitioning
        #expiration_ms=NULL  # 90 days
    ),
        clustering_fields = ["mmsi"]
    )    
    load_job = client.load_table_from_dataframe(
        df1, '.'.join([project_id, DATASET, TABLE]), job_config = job_config)
    
    result = load_job.result()
    
    print("Written {} rows to {}".format(result.output_rows, result.destination))
    print("Partitioning: {}".format(result.time_partitioning))
    
if __name__ == '__main__':
    df = get_multiline_data()
    df1 = bq2simplified_df(df)
    write_simplified_lines(df1)
    