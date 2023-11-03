import pandas as pd


#Task #5 - process data from rates.csv
def process_rates(file_name: str = 'dags/rates.csv') -> pd.DataFrame:
    '''
    Process the rates Dataframes.
    Convert format from the current file :
        - date|AED|AFN|AMD|ANG ... etc.
    To:
        - date|symbol|rate
    Args: 
        -> rates_CSV_location(str) - The location of rates.csv
    Returns:
        -> DataFrame : return the processed rates dataframe
    '''
    df_rates = pd.read_csv(file_name, index_col='Unnamed: 0')
    print(df_rates.head())
    #df = pd.DataFrame(mylist,index=['A','B','C','D','E'],columns=['Col1','Col2'])

    # rotate df
    stacked_df = pd.DataFrame(df_rates.stack().reset_index())
    # name columns
    stacked_df.columns = ['date','symbol','rate']
    # reorder columns
    reordered_df = pd.DataFrame(stacked_df,columns=['date','symbol','rate'])

    #print(reordered_df.head())
    # Create a CSV file with the ordered rates
    #reordered_df.to_csv('dags/processed_rates.csv', index=False)
    return reordered_df

#Task #6 Load to BigQuery
def load_to_bq(df: pd.DataFrame) -> None:
    '''
    Read the given processed rates dataframe and export it to BigQuery

    Prerequities :
     - create the dataset into BigQuery called "forex_platform"
     - create a table called rates
    '''

def main_func():
    file_path = f'dags/rates.csv'

    process_rates(file_name=file_path)

    print("end of main")

if __name__ == "__main__":
    main_func()