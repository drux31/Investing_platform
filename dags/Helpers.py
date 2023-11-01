import requests
from typing import Dict

#Dag #1- extract rates dictionary
def extract_rates(start_date:str, end_date:str) -> str:
    '''
    Extract Forex rates from Fixer.io

    Args:
        -> api_key:str, sart_date:tstr, end_date:str
    Returns:
        -> result:dict    
    '''
    #url=f"http://data.fixer.io/api/timeseries?access_key={api_key}&start_date={start_date}&end_date={end_date}"
    url_frank = f"https://api.frankfurter.app/{start_date}..{end_date}"
    '''
    payload: Dict[str, str] = {}
    headers={
        "apikey": api_key
    }
    '''
    ## Calling the API
    response= requests.request("GET", url_frank) #, headers = headers, data = payload)
    results = response.text

    return results