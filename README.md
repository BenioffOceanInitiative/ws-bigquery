# whalesafe_bigquery_sql
Execute SQL to process Global Fishing Watch AIS messages data into geospatial data and produce vessel speed reduction statistics.

So far, only creates test tables. SQL is in 'queries' folder.

## Setup Python

Get BigQuery Client Libraries:

- https://cloud.google.com/bigquery/docs/reference/libraries

FYI, here's the google cloud python development setup:

- https://cloud.google.com/python/setup

Essentially:

1. Install the latest version of Python.
1. Use `venv` to isolate dependencies.
1. Install an editor (optional).
1. Install the Cloud SDK (optional).
1. Install the Cloud Client Libraries for Python (optional).
1. Install other useful tools.

## Run Queries

Here are the JSON credentials you'll need to download:

- [Benioff Ocean Initiative-454f666d1896.json](https://drive.google.com/open?id=1qwxuSorWEKiFDl7AxrsGPFwkwCZrd6te)

Then update the path your JSON in [`test.py`](https://github.com/BenioffOceanInitiative/whalesafe_bigquery_sql_execution/blob/e1d451a596e2398bc4f8c077857a26a6f3144523/test.py#L13):

```python
credentials_json = '/Users/seangoral/bq_api_test/venv/Benioff Ocean Initiative-454f666d1896.json'
```
