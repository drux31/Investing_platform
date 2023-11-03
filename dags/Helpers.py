import requests
import json
import pandas as pd
import os
from dotenv import load_dotenv
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

#loading environnment variables
load_dotenv()
GOOGLE_APPLICATION_CREDENTIALS = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
GOOGLE_PROJECT_ID = os.environ.get('GOOGLE_PROJECT_ID')
GCS_BUCKET = os.environ.get('GCS_BUCKET')

#Task #1- extract rates dictionary
def extract_rates(start_date:str, end_date:str) -> str:
    '''
    Extract Forex rates from frankfurter.app

    Args:
        -> sart_date:str, end_date:str
    Returns:
        -> result:txt    
    '''
    #url=f"http://data.fixer.io/api/timeseries?access_key={api_key}&start_date={start_date}&end_date={end_date}"
    url_frank = f"https://api.frankfurter.app/{start_date}..{end_date}"
 
    ## Calling the API
    response= requests.request("GET", url_frank) #, headers = headers, data = payload)
    results = response.text

    return results

#Task #2 - extract the rates dictionary
def extract_rates_dictionary(results:str) -> dict:
    '''
    Extract the rates dictionary from frankfurter.app

    Args:
        -> result = frankfurter.app response
    Returns 
        -> rates = rates dictionary
    '''
    data = json.loads(results)
    rates = data['rates']
    return rates

#Task #3 - create a dataframe with the data
def create_dataframe(rates: dict, start_date: str, end_date: str, export_to_csv=True) -> None: # pd.DataFrame:
    '''
    Create a DataFrame from rates dictionary.
    Export the dataframe as a CSV file
    
    Args:
        -> rates(dict)
        -> start_date(str)
        -> end_date(str)
        -> export_to_csv(bool)
            - Default = True
    Returns:
        -> None (pd.DataFrame)
    '''
    print('into creation of dataframe')
    #Create a dataframe with the colums and indices from the first day's data
    #first_day_data = rates.get(str(start_date))
    #print (first_day_data)
    '''
    if first_day_data is None:
        #Return an empty dataframe if there is no data from the start date
        return None
    '''   
    #Iterate over the dates from the start_date to the en_date
    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    datas = []
    #print(rates.get(str('2023-02-27')))

    print('\nentering the loop')
    for date in dates.date:
        data = rates.get(str(date))
        #print(data)
       
        if data is None:
            #remove the date with no rates data
            dates = dates.drop(date)
            continue
        #Append the data for the date to the dataframe
        datas.append(data)

    first_day_df = pd.DataFrame(datas)


    #Add dates to the dataframe
    first_day_df.index = dates.date

    #Export as CSV
    if export_to_csv == True:
        first_day_df.to_csv('dags/rates.csv')
        #return None
    #return first_day_df

#Task #4 Load raw data to cloud storage
def load_to_gcs(local_data: str, file_name: str, **kwargs) -> None:
    '''
    Get or create a Google Cloud Storage Bucket.
    Load the CSV file: rates.csv to the Storage Bucket.
    Args:
        -> None
    Returns:
        -> None    
    '''
    # Create storage client :
    # storage_client = storage.Client()

    try:
        local_data = local_data
        dst = file_name
        upload_to_gcs_task = LocalFilesystemToGCSOperator (
            task_id = 'local_to_gcs_stock_analysis',
            bucket=GCS_BUCKET,
            src=local_data,
            dst=dst,
        )
        upload_to_gcs_task.execute(kwargs)
        
    except Exception as e:
        print(f'Data load error: {str(e)}')

'''
Code to help debugging Helpers.py

def main_func():
    test = extract_rates('2023-01-01', '2023-05-01')
    rates_test = extract_rates_dictionary(test)
    
    print(f"Entering dataframe create function")

    create_dataframe(rates_test, '2023-01-01', '2023-05-01', export_to_csv=True)

    print("end of main")

if __name__ == "__main__":
    main_func()
'''