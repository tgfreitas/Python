import numpy as np
import pandas as pd

def calculate_turnover(df:pd.DataFrame, group:str, status_column:str = 'Tabela', hc_type_column:str = 'Tipo_HC', date_column:str = 'Data') -> pd.DataFrame:
    '''
    Calculates turnover metrics for a specified group based on the provided DataFrame.
    The DataFrame must contain three required columns: date_column, status_column, and tipo_hc_column.

    Paramaters:
    - df(pd.dataframe): DataFrame containing the data for turnover calculations.
    - group(str): The group for which to calculate turnover metrics.
    - status_column(str): The name of the field that indicates the employee's status (e.g., Atv, Inv, Vol).
    - hc_type_column(str): The name of the field that specifies the headcount type (e.g. Lider, IC, Oper, Entrada).
    - date_column(str): The name of the field containing the date reference.
    Returns:
    - (pd.DataFrame): DataFrame containing the calculated turnover metrics.

    '''

    columns = ['Key','Ano','Mes','Data','Perimetro','Atv','Resc_Inv','Resc_Red','Resc_Vol','Resc_Total'
            ,'Resc_Total+Red','HC_Ref','TO_Inv','TO_Vol','TO_Total','TO_Red','TO_Total+Red','Resc_Inv_YTD'
            ,'Resc_Red_YTD','Resc_Vol_YTD','Resc_Total_YTD','Resc_Total+Red_YTD','TO_Inv_YTD','TO_Vol_YTD'
            ,'TO_Total_YTD','TO_Red_YTD','TO_Total+Red_YTD']
    
    new_names = {'Inv' : 'Resc_Inv', 'Red' : 'Resc_Red' , 'Vol' : 'Resc_Vol'}

    df = df[(df[hc_type_column] != 'Entrada') & (df[status_column] != 'New')]
    pivot = df.pivot_table(index = [date_column,'Ano','Mes',group],columns=status_column, values = hc_type_column, aggfunc='count').fillna(0)
    pivot['Resc_Total'] = pivot['Inv'] + pivot['Vol']
    pivot['Resc_Total+Red'] = pivot['Resc_Total'] + pivot['Red']
    pivot['HC_Ref'] = pivot['Resc_Total+Red']  + pivot['Atv']
    pivot['TO_Total'] = (pivot['Resc_Total'] / pivot['HC_Ref']).fillna(0).round(4)
    pivot['TO_Vol'] = (pivot['Vol'] / pivot['HC_Ref']).fillna(0).round(4)
    pivot['TO_Inv'] = (pivot['Inv'] / pivot['HC_Ref']).fillna(0).round(4)
    pivot['TO_Red'] = (pivot['Red'] / pivot['HC_Ref']).fillna(0).round(4)
    pivot['TO_Total+Red'] = (pivot['Resc_Total+Red'] / pivot['HC_Ref']).fillna(0).round(4)
    pivot['Resc_Total_YTD'] = pivot.groupby(['Ano',group])['Resc_Total'].cumsum().round(4)
    pivot['Resc_Inv_YTD'] = pivot.groupby(['Ano',group])['Inv'].cumsum().round(4)
    pivot['Resc_Vol_YTD'] = pivot.groupby(['Ano',group])['Vol'].cumsum().round(4)
    pivot['Resc_Red_YTD'] = pivot.groupby(['Ano',group])['Red'].cumsum().round(4)
    pivot['Resc_Total+Red_YTD'] = pivot.groupby(['Ano',group])['Resc_Total+Red'].cumsum().round(4)
    pivot['TO_Total_YTD'] = pivot.groupby(['Ano',group])['TO_Total'].cumsum().round(4)
    pivot['TO_Inv_YTD'] = pivot.groupby(['Ano',group])['TO_Inv'].cumsum().round(4)
    pivot['TO_Red_YTD'] = pivot.groupby(['Ano',group])['TO_Red'].cumsum().round(4)
    pivot['TO_Vol_YTD'] = pivot.groupby(['Ano',group])['TO_Vol'].cumsum().round(4)
    pivot['TO_Total+Red_YTD'] = pivot.groupby(['Ano',group])['TO_Total+Red'].cumsum().round(4)
    pivot.fillna(0)
    pivot = pivot.reset_index().rename(columns = {group:'Perimetro'}) 
    pivot['Key'] = pivot['Perimetro'] + pivot['Ano'] + pivot['Mes']
    pivot['Sort_Date'] = pd.to_datetime(pivot['Data'],infer_datetime_format=True,dayfirst=True)
    pivot = pivot.sort_values(by='Sort_Date')
    pivot.rename(columns=new_names,inplace=True)
    pivot = pivot[columns]
    return pivot

def make_turnover_table(df: pd.DataFrame, group_type: list[str] = None, status_column:str = 'Tabela', hc_type_column:str = 'Tipo_HC', date_column:str = 'Data') -> pd.DataFrame:
    '''
    Generates a consolidated turnover dataset for various groups (e.g., Company, EXCO, VP) using the calculate_turnover function.

    Parameters:
        - df (pd.DataFrame): DataFrame containing the data for turnover calculations.
        - group_types (List[str]): List of group types used for turnover calculation (e.g., Company, EXCO, VP).
        - status_column(str): The name of the field that indicates the employee's status to be used in the turnover calculation (e.g., Atv, Inv, Vol).
        - hc_type_column(str): The name of the field that specifies the headcount type to be used in the turnover calculation (e.g. Lider, IC, Oper, Entrada).
        - date_column(str): The name of the field containing the date reference to be used in the turnover calculation.
        
    Returns:
        - pd.DataFrame: A DataFrame containing the complete turnover dataset for all specified group types.
    '''
    if group_type is None:
            group_type = ['Company', 'EXCO', 'VP', 'Diretoria', 'CC_Desc', 'BP_Ger', 'BP_Coord', 'Grupo CC 1', 'Grupo CC 2', 'Grupo CC 3']

    tab_turnover_list = [calculate_turnover(df, group, status_column, hc_type_column, date_column) for group in group_type]
    return pd.concat(tab_turnover_list, ignore_index=True)

def calculate_tenure(df:pd.DataFrame,group:str, status_column:str = 'Tabela', hc_type_column:str = 'Tipo_HC', date_column:str = 'Data') -> pd.DataFrame:
    '''
    Calculates tenure metrics for a specified group based on the provided DataFrame.
    The DataFrame must contain three required columns: a date column, a situation column, 
    and a personal tenure column. 

    Paramaters:
    - df(pd.dataframe): DataFrame containing the data for tenure calculations.
    - group(str): The group for which to calculate tenure metrics.
    - status_column(str): The name of the field that indicates the employee's status (e.g., Atv, Inv, Vol).
    - hc_type_column(str): The name of the field that specifies the headcount type (e.g. Lider, IC, Oper, Entrada).
    - date_column(str): The name of the field containing the date reference.
    Returns:
    - (pd.DataFrame): DataFrame containing the calculated tenure metrics.

    '''
    df = df[df[hc_type_column] != 'Entrada']
    df[date_column] = pd.to_datetime(df[date_column], dayfirst=True, infer_datetime_format=True, errors='coerce')
    df = df.dropna(subset=[date_column])
    df['Atv_Resc'] = np.where(((df[status_column] == 'Atv') | (df[status_column] == 'New')),'Atv','Resc')
    tenure = pd.pivot_table(df, values='Tenure', index=[date_column, group],
                    columns=['Atv_Resc'], aggfunc="median").reset_index().sort_values(by=date_column)
    tenure['Ano'] = tenure[date_column].dt.year
    tenure['Mes'] = tenure[date_column].dt.month
    tenure.rename(columns={'Atv' : 'Meses_Mediana_Atv', 'Resc' : 'Meses_Mediana_Resc', group : 'Perimetro'},inplace=True)
    tenure['Key'] = tenure['Perimetro'] + tenure['Ano'].astype(str) + tenure['Mes'].astype(str)
    tenure[date_column] = tenure[date_column].astype(str)
    tenure = tenure[['Key','Ano', 'Mes',date_column, 'Perimetro', 'Meses_Mediana_Atv', 'Meses_Mediana_Resc']]
    return tenure

def make_tenure_table(df: pd.DataFrame, group_type: list[str] = None, status_column:str = 'Tabela', hc_type_column:str = 'Tipo_HC', date_column:str = 'Data') -> pd.DataFrame:
    '''
    Generates a consolidated turnover dataset for various groups (e.g., Company, EXCO, VP) using the calculate_turnover function.

    Parameters:
        - df (pd.DataFrame): DataFrame containing the data for turnover calculations.
        - group_types (List[str]): List of group types used for turnover calculation (e.g., Company, EXCO, VP).
        - status_column(str): The name of the field that indicates the employee's status to be used in the turnover calculation (e.g., Atv, Inv, Vol).
        - hc_type_column(str): The name of the field that specifies the headcount type to be used in the turnover calculation (e.g. Lider, IC, Oper, Entrada).
        - date_column(str): The name of the field containing the date reference to be used in the turnover calculation.
        
    Returns:
        - pd.DataFrame: A DataFrame containing the complete turnover dataset for all specified group types.
    '''
    if group_type is None:
            group_type = ['Company', 'EXCO', 'VP', 'Diretoria', 'CC_Desc', 'BP_Ger', 'BP_Coord', 'Grupo CC 1', 'Grupo CC 2', 'Grupo CC 3']

    tab_turnover_list = [calculate_tenure(df, group, status_column, hc_type_column, date_column) for group in group_type]
    return pd.concat(tab_turnover_list, ignore_index=True)

def calculate_turnover2(df:pd.DataFrame, group:str, status_column:str = 'Tabela', hc_type_column:str = 'Tipo_HC', date_column:str = 'Data') -> pd.DataFrame:
    '''
    Calculates turnover metrics for a specified group based on the provided DataFrame.
    The DataFrame must contain three required columns: date_column, status_column, and tipo_hc_column.

    Paramaters:
    - df(pd.dataframe): DataFrame containing the data for turnover calculations.
    - group(str): The group for which to calculate turnover metrics.
    - status_column(str): The name of the field that indicates the employee's status (e.g., Atv, Inv, Vol).
    - hc_type_column(str): The name of the field that specifies the headcount type (e.g. Lider, IC, Oper, Entrada).
    - date_column(str): The name of the field containing the date reference.
    Returns:
    - (pd.DataFrame): DataFrame containing the calculated turnover metrics.

    '''

    columns = ['Key','Ano','Mes','Data','Perimetro','Atv','Resc_Inv','Resc_Red','Resc_Vol','Resc_Total'
            ,'Resc_Total+Red','HC_Ref','TO_Inv','TO_Vol','TO_Total','TO_Red','TO_Total+Red','Resc_Inv_YTD'
            ,'Resc_Red_YTD','Resc_Vol_YTD','Resc_Total_YTD','Resc_Total+Red_YTD','TO_Inv_YTD','TO_Vol_YTD'
            ,'TO_Total_YTD','TO_Red_YTD','TO_Total+Red_YTD']
    
    new_names = {'Inv' : 'resc_inv', 'Red' : 'resc_red' , 'Vol' : 'resc_vol', 'Atv': 'atv'}

    initial_metrics =['resc_vol','resc_inv','resc_total','resc_red','resc_total_red']
    sum_metrics = ['resc_vol','resc_inv','resc_total','resc_red','resc_total_red','to_vol','to_inv','to_total','to_red','to_total_red']
    avg_metrics = ['atv','hc_ref']

    df = df[(df[hc_type_column] != 'Entrada') & (df[status_column] != 'New')]
    df[date_column] = pd.to_datetime(df[date_column], dayfirst=True, infer_datetime_format=True, errors='coerce')
    df['ano'] = df[date_column].dt.year
    df['mes'] = df[date_column].dt.month
    table = df.pivot_table(index = [date_column,'ano','mes',group],columns=status_column, values = hc_type_column, aggfunc='count').fillna(0)
    table = table.sort_values(by=date_column)
    table.rename(columns=new_names,inplace=True)
    table['quarter'] = table[date_column].dt.quarter
    table['resc_total'] = table['resc_inv'] + table['resc_vol']
    table['resc_total_red'] = table['resc_total'] + table['resc_red']
    table['hc_ref'] = table['resc_total_red']  + table['atv']

    for i in initial_metrics:
        turnover_name = i.replace('resc', 'to')
        table[turnover_name] = (table[i] / table['hc_ref']).fillna(0).round(4)

    for i in sum_metrics:
        ytd_name = i + '_to'
        table[ytd_name] = table.groupby(['ano',group])[i].cumsum().round(4)

    table.fillna(0)
    table = table.reset_index().rename(columns = {group:'perimetro'}) 
    table['key'] = table['perimetro'] + table['ano'] + table['mes']
    #table = table[columns]
    return table