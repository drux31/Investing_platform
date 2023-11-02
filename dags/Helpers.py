import requests
import json
import pandas as pd
import os
from dotenv import load_dotenv

#loading environnment variables
load_dotenv()
GOOGLE_APPLICATION_CREDENTIALS = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')

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
    first_day_data = rates.get(str(start_date))
    #print (first_day_data)
    '''
    if first_day_data is None:
        #Return an empty dataframe if there is no data from the start date
        return None
    '''   
    #Iterate over the dates from the start_date to the en_date
    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    datas = []
    valid_dates = []
    #print(rates.get(str('2023-02-27')))

    print('\nentering the loop')
    for date in dates.date:
        data = rates.get(str(date))
        #print(data)
       
        if data is None:
            dates = dates.drop(date)
            continue
        #Append the data for the date to the dataframe
        #first_day_df = pd.concat([first_day_df, data], ignore_index=True)
        datas.append(data)

    first_day_df = pd.DataFrame(datas)


    #Add dates to the dataframe
    first_day_df.index = dates.date

    #Export as CSV
    if export_to_csv == True:
        first_day_df.to_csv('dags/rates.csv')
        #return None
    #return first_day_df
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