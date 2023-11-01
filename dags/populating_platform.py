from airflow.decorators import task, dag

from Helpers import *
#from datetime import date
import pendulum

import os
from dotenv import load_dotenv

# Configs
#For reading .env variable
#dotenv_path = Path('~/de_project/Investing_platform/')
#load_dotenv(dotenv_path=dotenv_path)
#project_folder = os.path.expanduser('~/de_project/Investing_platform/')
#load_dotenv(os.path.join(project_folder, '.env'))

load_dotenv()
#api_key= os.getenv('FIXER_API_KEY')
#print (api_key)

# Initial date
start_date = '2023-01-01'
end_date = '2023-03-01'

# setting defualt configs

#Dag #1 - populating the platform
@dag(dag_id='populating_platform', start_date=pendulum.datetime(2023, 1, 1, tz="UTC"), catchup=False, tags=['Initial_Load'])
def populate_platform():
    #task #1 - Extract rates from Fixer.io
    @task()
    def task1_extract_rates(start_date, end_date):
        results = extract_rates(start_date=start_date, end_date=end_date)
        return results

    # Dependencies
    results = task1_extract_rates(start_date=start_date, end_date=end_date)

#Instantiating the DAG
populate_platform = populate_platform()
