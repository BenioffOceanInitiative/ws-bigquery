#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr  2 14:04:52 2020

@author: seang
"""

from google.cloud import bigquery
from google.oauth2 import service_account

#FILL IN YOUR PATH TO THE 'Benioff Ocean Initiative-454f666d1896.json'
credentials_json = '/Users/seangoral/bq_api_test/venv/Benioff Ocean Initiative-454f666d1896.json'

credentials = service_account.Credentials.from_service_account_file(credentials_json)

project_id = 'benioff-ocean-initiative'
client = bigquery.Client(credentials= credentials,project=project_id)

def run_query(sql):
    with open('queries/' + sql + '.sql') as sql_file:
        query = sql_file.read()
        query_job = client.query(query)
        query_job.result()        

if __name__ == '__main__':
    run_query('create_gfw_table')
    run_query('create_gfw_ihs_table')
    run_query('create_gfw_ihs_segments_table')
    run_query('create_gfw_ihs_vsr_segments_table')
    run_query('insert_gfw_data')
    run_query('insert_gfw_ihs_data')
    run_query('insert_gfw_ihs_segments')
    run_query('insert_gfw_ihs_vsr_segments')
  
