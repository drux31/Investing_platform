
import pandas as pd
from google.cloud import bigquery

def get_rates()-> pd.DataFrame:
    # Creating BigQuery Client
    bigquery_client = bigquery.Client()

    # Fetching the data from BigQuery
    query = "Select * from forex_platform.rates order by date, symbol ;"

    # Getting the results
    query_job = bigquery_client.query(query)
    results = query_job.result()

    # Creating a pandas DataFrame
    df = results.to_dataframe()
    print (df.head())
    return df

'''
Code to help debugging app.py
'''
def main_func():
    get_rates()

if __name__ == "__main__":
    main_func()