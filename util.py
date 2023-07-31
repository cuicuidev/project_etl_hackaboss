import numpy as np
import pandas as pd
import ast
import requests

from time import time
from datetime import datetime

def removeNaFromLists(row):
    if row is not np.nan:
        return [x for x in row if not pd.isna(x)]
    return row

def removeEmptyLists(row):
    if row == []:
        return np.nan
    else:
        return row
    
def _unpackLists(series):
    data = []
    for list_ in series.iloc:
        if isinstance(list_, list):
            data.extend(list_)
    df = pd.DataFrame(data).rename(columns = {0 : f'{series.name}'})
    return df

def _getCounts(data_frame, column):
    df = data_frame.groupby(column).size().reset_index(name = f'{column}_count')
    return df

def _concatCounts(data_frames, columns, main_column, axis = 0):
    data = _getMultipleCounts(data_frames = data_frames,
                          columns = columns,
                          main_column = main_column)
    df = pd.concat(data, axis = axis)
    return df

def _getMultipleCounts(data_frames, columns, main_column):
    counts = []
    for data_frame, column in zip(data_frames, columns):
        counts.append(_getCounts(data_frame, column))
    
    data = []
    for index, count in enumerate(counts):
        if index == 0:
            data.append(count[[columns[index] ,f'{columns[index]}_count']].rename(columns = {columns[index] : main_column}))
            continue
        
        data.append(count[[f'{columns[index]}_count']])
    return data

def loadToAirtable(key, app, tbls, data_frame):
    start = time()
    
    df = data_frame.copy()
    
    airtable_base_url = "https://api.airtable.com/v0"
    
    endpoints = [f"{airtable_base_url}/{app}/{tbl}" for tbl in tbls]

    headers = {"Authorization" : f"Bearer {key}",
               "Content-Type"  : "application/json"}

    df.fillna('', inplace = True)
    df = df[df.columns].astype(str)
    
    datos_json = [{"fields" : df.iloc[i, :].to_dict()} for i in range(df.shape[0])]
    
    for offset, endpoint in zip(range(0, len(datos_json), 50000), endpoints):
        for i in range(offset, offset + 50000, 10):
            data_ = {"records" : datos_json[i : i + 10]}

            startResponse = time()
            
            response = requests.post(url = endpoint, json = data_, headers = headers) # POST
            
            endResponse = time()
            responseTime = endResponse - startResponse
            
            now = datetime.strftime(datetime.now(), '%x %X')

            log_str = f'[REQUEST] | {now} | Status {response.status_code} | Table {round((offset/50000) + 1)} | Batch {round(i/10 + 1)} | Response time [{round(responseTime, 2)}s] | Time elapsed [{round(endResponse - start, 2)}s]'
            
            print(log_str)
            
    end = time()
    duration = round(end - start)
    hours = duration // 60 // 60
    minutes = duration // 60 % 60
    seconds = duration % 60

    #Registro
    now = datetime.strftime(datetime.now(), '%x %X')
    log_str = f'[loadToAirtable] | {now} | LOADING FINISHED | TOTAL TIME: {hours} hours, {minutes} minutes and {seconds} seconds'
    
    print(log_str)
    
def parseStr(s):
    if pd.isna(s):
        return s

    try:
        return ast.literal_eval(s)
    except (ValueError, SyntaxError, TypeError):
        pass

    try:
        return int(s)
    except (ValueError, TypeError):
        pass

    return s
    

def extractTbl(key, app, tbl):
    airtable_base_url = "https://api.airtable.com/v0"
    
    endpoint = f"{airtable_base_url}/{app}/{tbl}"

    headers = {"Authorization" : f"Bearer {key}",
               "Content-Type"  : "application/json"}
    
    params = {"offset" : None}

    df_airtable = pd.DataFrame()

    while params.get("offset") != None or df_airtable.shape[0] == 0:

        response = requests.get(url = endpoint, headers = headers, params = params)

        print(response.url)

        print(f"response: {response.status_code}")

        params["offset"] = response.json().get("offset")

        print(params.get("offset"))

        df_airtable = pd.concat([df_airtable, pd.json_normalize(response.json()["records"])], ignore_index = True)
        
        print(df_airtable.shape[0])
        
        if (params.get("offset") == None and df_airtable.shape[0] == 0):
            break

    return df_airtable

def extractFromAirtable(key, app, tbls):
    df_airtable = pd.DataFrame()
    
    for tbl in tbls:
        extracted_df = extractTbl(key, app, tbl)
    
        df_airtable = pd.concat([df_airtable, extracted_df], ignore_index = True)
    
    df_airtable = df_airtable[df_airtable.columns[2:]]
    
    df_airtable = df_airtable.rename(columns = {x : x.split('.')[1] for x in df_airtable.columns})

    for column in df_airtable.columns:
        df_airtable[column] = df_airtable[column].apply(parseStr)
        
    df_airtable = df_airtable.replace('nan', np.nan)
    
    return df_airtable