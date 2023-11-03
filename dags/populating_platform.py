from airflow.decorators import task, dag

from Helpers import *
import pendulum

# Initial date
start_date = '2023-01-01'
end_date = '2023-03-01'
local_data = 'dags/rates.csv'
rates_file = 'rates.csv'

# setting defualt configs

#Dag #1 - populating the platform
@dag(dag_id='populating_platform', start_date=pendulum.datetime(2023, 1, 1, tz="UTC"), catchup=False, tags=['Initial_Load'])
def populate_platform():
    #task #1 - Extract rates from frankfurter.app
    @task()
    def task1_extract_rates(start_date: str, end_date: str) -> str:
        results = extract_rates(start_date=start_date, end_date=end_date)
        return results

    #task #2 - Extrate rates dictionary
    @task()
    def task2_extract_rates_dictionary(results: str) -> dict:
        rates = extract_rates_dictionary(results=results)
        return rates
    
    #task #3 - Create a dataframe
    @task()
    def task3_create_dataframe(rates: dict, start_date: str, end_date: str) -> None:
        create_dataframe(rates=rates, start_date=start_date, end_date=end_date)
    #task #4 - Upload to GCS
    @task()
    def task4_load_to_gcs() -> None:
        load_to_gcs(local_data=local_data, file_name=rates_file)

    # Dependencies
    results = task1_extract_rates(start_date=start_date, end_date=end_date)
    rates = task2_extract_rates_dictionary(results=results)
    task3_create_dataframe(rates, start_date=start_date, end_date=end_date) >> task4_load_to_gcs()

#Instantiating the DAG
populate_platform = populate_platform()
