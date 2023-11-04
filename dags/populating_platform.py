import json
from textwrap import dedent
import requests

import pendulum
from Helpers import *

from airflow.decorators import dag, task

start_date_extraction = '2023-01-01'
end_date_extraction = '2023-10-31'
@dag(
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["Populating platform: Investment platform"],
)
def populating_platform():
    """
    ### Taskflow investing platform documentation
    A data pipeline to tests the ingestion of data from an online API frankfurter.app to GCP (storage and bigquery)
    The original tutorial [here](https://towardsdev.com/data-engineering-project-creating-an-investing-platform-part-1-e777b5bd27cd)
    """
    @task()
    def extract_data(start_date: str, end_date: str)-> str:
        """
        #### Extract task
        An extract task to get data ready for the rest of the data
        pipeline. In this case, the data a downloaded from this [free API](https://frankfurter.app).
        """
        url_frank = f"https://api.frankfurter.app/{start_date}..{end_date}"
 
        ## Calling the API
        response= requests.request("GET", url_frank)
        results = response.text

        #rates = results['rates']

        return results #rates
    
    @task()
    def transform_data(rates: dict, start_date: str, end_date: str)-> list:
        """
        #### Transform task
        A Transform task which takes in the raw_data and create two CSV files:
         - one for the raw data : rates.csv
         - one for the processed data : processed_rates.csv
        """
        ##Raw CSV:
        rates_csv = 'dags/rates.csv'
        ##Processed CSV
        processed_rates = 'dags/processed_rates.csv'
        
        #ceate a dataframe with the dict
        df = create_dataframe(rates, start_date, end_date, export_to_csv=False)
        df.to_csv(rates_csv)
        
        # Create the processed file
        reordered_df = process_rates(file_name=rates_csv)
        reordered_df.to_csv(processed_rates, index=False)

        local_files = [rates_csv, processed_rates]
        return local_files

    @task()
    def load_data_to_gcs(local_file: list) -> None:
        """
        #### First Load task
        A task which takes local files and loads them to cloud storage
        """
        for file in local_file:
            load_to_gcs(local_data=file, file_name=file[5:])

    ## Load from GCS to BigQuery
    load_gcs_to_bq = GCSToBigQueryOperator(
        task_id='gcs_to_bigquery_rates',
        bucket=GCS_BUCKET,
        source_objects=['processed_rates.csv'],
        destination_project_dataset_table=f"{GOOGLE_PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}",
        source_format='CSV',
        skip_leading_rows=1,
        schema_fields=[
            {'name': 'date', 'type': 'DATE', 'mode': 'REQUIRED'},
            {'name': 'symbol', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'rate', 'type': 'DECIMAL', 'mode': 'REQUIRED'}
        ],
        write_disposition='WRITE_TRUNCATE',
    )
    load_gcs_to_bq.doc_md = dedent(
        """\
       
        #### Second Load task
        A task which takes processed_file from GCS and load to BQ
        """
    )
    results = extract_rates(start_date_extraction, end_date_extraction)
    rates = extract_rates_dictionary(results)
    local_files = transform_data(rates, start_date_extraction, end_date_extraction)
    load_data_to_gcs(local_files) >> load_gcs_to_bq
populating_platform()