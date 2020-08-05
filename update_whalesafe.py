#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr  2 14:04:52 2020

@author: sean goral and ben best
"""

from google.cloud import bigquery
from google.oauth2 import service_account

#FILL IN YOUR PATH TO your BigQuery credentials JSON ex: 'Benioff Ocean Initiative-454f666d1896.json'
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
#    print("Updating ais data table")
#    run_query('update_ais_data')
    print("Updating ais segments table")
    run_query('update_ais_segments')
    print("Updating ais segments aggregated table")
    run_query('update_ais_segments_aggregated')
    print("Updating ais vsr segments table")
    run_query('update_ais_vsr_segments')
    print("Updating ship stats daily data table")
    run_query('update_ship_stats_daily')
    print("Updating ship stats monthly data table")
    run_query('update_ship_stats_monthly')
    print("Updating ship stats annual data table")
    run_query('update_ship_stats_annual')
    print("Updating operator stats daily data table")
    run_query('update_operator_stats_daily')
    print("Updating operator stats monthly data table")
    run_query('update_operator_stats_monthly')
    print("Updating operator stats annual data table")
    run_query('update_operator_stats_annual')
    print("done")
