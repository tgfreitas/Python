import pandas as pd
import requests
import re
import json
import time
import warnings
import gspread
from google.oauth2.credentials import Credentials

warnings.filterwarnings('ignore')

def get_creds(token_path:str) -> Credentials:
    '''
        Retrieves credentials for accessing the Google Sheets API.

        Requires the user to have an API token.
        
        Paramaters:
        - token_path (str): String containing the path to the API token file.
        Returns:
        - Credentials object for accessing the Google Sheets API.

    '''
        
    creds = Credentials.from_authorized_user_file(token_path, ['https://www.googleapis.com/auth/spreadsheets'])
    return creds

def import_metabase_query(creds_path:str,query_number:str, print_level:int=0) -> pd.DataFrame:
    '''
        Imports a specified query from Metabase using its API and converts the results into a pandas DataFrame.
        
        Paramaters:
        - creds_path(str):  Path to the JSON file containing the Metabase API credentials (dictionary containing 'user' and 'password' keys).
        - query_number(str): ID number of the Metabase query to import.
        - print_level(int): Optional parameter indicating the level of information you want printed as the script plays, a bigger number equals more information displayed:
            0 - No print.
            1 - Print at the end indicating the df was imported and the total time.
            2 - Print at start indicanting the df started to be imported.
        Returns:
        - (pd.DataFrame): DataFrame containing the results of the imported query.
    '''  
    start_time = time.perf_counter()
    if print_level >=2:
        print(f'>>> Importing df...') 

    with open(creds_path, 'r') as file:
        data = file.read()
        json_data = json.loads(data)

    res = requests.post('https://metabase.madeiramadeira.com.br/api/session',
                        json =  {
                            "username":json_data['user'],
                            "password": json_data['password']
                            })
    assert res.ok == True
    token = res.json()['id']
    resq = requests.post(f'https://metabase.madeiramadeira.com.br./api/card/{query_number}/query/json',
                         headers = {'Content-Type': 'application/json',
                                    'X-Metabase-Session': token
                                    })
    df = pd.DataFrame(resq.json())
    
    end_time = time.perf_counter()
    running_time = end_time - start_time
    if print_level >=1:
        print(f'>>> df imported in {round(running_time,2)}s')
    
    return df

def get_sheets(creds:Credentials, url:str, tab:str, print_level:int=0, process_columns:bool=False) -> pd.DataFrame:
    '''
        Imports a specified sheet from Google Sheets and creates a pandas DataFrame.

        Parameters:
        - creds (Credentials): Google Sheets API credentials.
        - url (str): String containing the Google Sheet URL key.
        - tab (str): String containing the Google Sheet tab name.
        - process_columns (bool)(Optional): uses the process_gsheets_columns function to convert numbers and datetime columns to the correct datatype.
        - print_level(int): Optional parameter indicating the level of information you want printed as the script plays, a bigger number equals more information displayed:
            0 - No print.
            1 - Print at the end indicating the df was imported and the total time.
            2 - Print at start indicanting the df started to be imported.

        Returns:
        - df (pd.DataFrame): DataFrame containing values from the imported sheet.
    '''
    start_time = time.perf_counter()
    if print_level >=2:
        print(f'>>> Importing df...') 

    gc = gspread.authorize(creds)
    destino_key = gc.open_by_key(url)
    worksheet = destino_key.worksheet(tab)
    rows = worksheet.get_all_values()
    df = pd.DataFrame.from_records(rows)
    df.columns = df.iloc[0]
    df = df.iloc[1: , :]
    if process_columns:
        df = process_gsheets_columns(df)
    
    end_time = time.perf_counter()
    running_time = end_time - start_time
    if print_level >=1:
        print(f'>>> df imported in {round(running_time,2)}s')
    
    return df

def export_to_sheets(creds:Credentials, df:pd.DataFrame, destiny_sheets:list[list], print_level:int=1, interval:str=None, **kwargs) -> None:
    '''
    Exports a pandas DataFrame to specified Google Sheets tabs. Optionally, it can delete and replace data within a specified interval,
    reorder columns, and filter columns based on a provided list.
        
        Paramaters:
        - creds(Credentials): The Google sheets API credentials.
        - df(pd.DataFrame): Pandas DataFrame containing the data to be exported.
        - destiny_sheets(list):  List of lists specifying the destination sheets and tabs. Each sublist should be in the format ['sheets_url:str','tab:str']. 
        - interval(str): Optional parameter specifying the interval where data will be deleted and replaced.
        - order(list): Optional parameter containing a list of column names to reorder and filter the DataFrame.
        - print_level(int): Optional parameter indicating the level of information you want printed as the script plays, a bigger number equals more information displayed:
            0 - No print.
            1 - Print at the end indicating the quantity of dfs exported and the total time.
            2 - Print at start indicanting the quantity of dfs to be exported.
            3 - Print at start and end of every export, indicating each export start and finish.
        Returns:
        - None
    '''  
    start_time = time.perf_counter()
    gc = gspread.authorize(creds)
    
    n_exports = len(destiny_sheets)

    if print_level >=2:
        print(f'>>> Exporting {n_exports} df(s)...')  
    
    if 'order' in kwargs:
        df = df[kwargs['order']]
    else:
        pass   

    datetime_columns = df.select_dtypes(include=['datetime64']).columns
    if not datetime_columns.empty:
        for column in datetime_columns:
            df[column] = df[column].dt.strftime('%d/%m/%Y')
    
    category_columns = df.select_dtypes(include=['category']).columns
    if not category_columns.empty:
        for column in category_columns:
            df[column] = df[column].astype(str)
            
    numeric_columns = df.select_dtypes(include=['number']).columns
    if not numeric_columns.empty:
        for column in numeric_columns:
            if df[column].isnull().any():
                df[column] = df[column].astype(str).str.replace('.',',')
    
    df = df.fillna('')
    df = df.replace('nan', '')

    df_headers = df.columns.to_list()
    df_values = df.to_numpy().tolist()
    df_headers_values = [df_headers] + df_values

    n = 0
    for i in destiny_sheets:
        n +=1
        if print_level >=3:
            print(f'>>> Exporting df {n} of {n_exports}...')
        destiny_url = gc.open_by_key(i[0])
        destiny = destiny_url.worksheet(i[1])
        if interval is None or interval == '':
            interval = 'A1'
        try:
            destiny.batch_clear([interval])
        except Exception as e:
            print(f'Error clearing interval: {e}')
        try:
            destiny.update(interval, df_headers_values, raw = False)
        except Exception as e:
            print(f'Error updating interval: {e}') 
        if print_level >=3:
            print(f'>>> df {n} of {n_exports} exported')
    
    end_time = time.perf_counter()
    running_time = end_time - start_time
    if print_level >=1:
        print(f'>>> {n_exports} df(s) exported in {round(running_time,2)}s')

def process_gsheets_columns(df:pd.DataFrame, replace_values:dict={'': None, '#N/A': None, '#N/D': None}, print_level:int=0)->pd.DataFrame:
    '''
    This function checks if all values in each column of the DataFrame
    comply with the rule of containing only numbers or the date format.
    If so, it converts the column to numeric or datetime accordingly.
    Also, convert all '' values to None.

    Parameters:
    - df: Pandas DataFrame to be processed.

    Returns:
    Pandas DataFrame with columns processed according to the specified rules.
    '''
    formatted_columns_numeric = 0
    formatted_columns_datetime = 0
    formatted_columns_category = 0

    df = df.replace(replace_values)

    def check_numbers_only(string):
        if string is None or string == '':
            return True
        return bool(re.match(r'^[\d.,]+$', string))

    def check_date_format(string):
        if string is None or string == '':
            return True
        return bool(re.match(r'^\d{2}/\d{2}/\d{4}$', string))
    
    def check_category(column):
        unique_values_count = column.nunique()
        total_records = len(column) 
        return unique_values_count < 1000 and unique_values_count < total_records / 2

    for column in df.columns:
        check_numbers_only_column = df[column].apply(check_numbers_only).all()
        check_date_format_column = df[column].apply(check_date_format).all()
        check_category_format_column = check_category(df[column])

        if check_numbers_only_column:
            formatted_columns_numeric += 1
            df[column] = df[column].str.replace('.', '')
            df[column] = df[column].str.replace(',', '.')
            df[column] = pd.to_numeric(df[column], errors='coerce')

        elif check_date_format_column:
            formatted_columns_datetime += 1
            df[column] = pd.to_datetime(df[column], errors='coerce', dayfirst=True, infer_datetime_format=True)

        elif check_category_format_column:
            formatted_columns_category += 1
            df[column] = pd.Categorical(df[column])

    if print_level > 0 and (formatted_columns_numeric > 0 or formatted_columns_datetime > 0):
        print(" - Numeric columns formatted:", formatted_columns_numeric, "\n - Datetime columns formatted:", formatted_columns_datetime, "\n - Category columns formatted:", formatted_columns_category)

    return df

def columns_type_fix(df:pd.DataFrame, ints:list, floats:list) -> pd.DataFrame:
    '''
        (old version, try function process_gsheets_columns)Replaces empty strings with None and converts column data types into integers and floats according to the parameters provided.
        
        Paramaters:
        - df(pd.DataFrame): Pandas DataFrame where the columns are located.
        - ints(list): List of column names to be converted into integers.
        - ints(list): List of column names to be converted into floats.
        Returns:
        - (p.DataFrame)The modified DataFrame without empty strings and with converted data types.
    '''

    for i in df.columns:
        if i in ints:
            df[i] = df[i].apply(lambda x: None if x == '' or x == None or type(x) == int else int(x))
        elif i in floats:
            df[i] = df[i].apply(lambda x: None if x == '' or x == None or type(x) == float else float(x.replace(',', '.')))    
        else:
            pass
    
    print('(old version, try function process_gsheets_columns)')
    return df
