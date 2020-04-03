#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr  2 14:04:52 2020

@author: seang
"""

from google.cloud import bigquery
from google.oauth2 import service_account


credentials = service_account.Credentials.from_service_account_file(
    '/Users/seangoral/bq_api_test/Benioff Ocean Initiative-454f666d1896.json')

project_id = 'benioff-ocean-initiative'
client = bigquery.Client(credentials= credentials,project=project_id)

def format_gfw_data():
    with open('queries/format_gfw_data.sql') as query_file:
        query = query_file.read()
        query_job = client.query(query)
        query_job.result()

def join_gfw_ihs_data():
    with open('queries/join_gfw_ihs_data.sql') as query_file:
        query = query_file.read()
        query_job = client.query(query)
        query_job.result()

def create_gfw_ihs_segments():
    with open('queries/create_gfw_ihs_segments.sql') as query_file:
        query = query_file.read()
        query_job = client.query(query)
        query_job.result()
        
def vsr_intersect():
    with open('queries/vsr_intersect.sql') as query_file:
        query = query_file.read()
        query_job = client.query(query)
        query_job.result()

if __name__ == '__main__':
    format_gfw_data()
    join_gfw_ihs_data()
    create_gfw_ihs_segments()
    vsr_intersect()
