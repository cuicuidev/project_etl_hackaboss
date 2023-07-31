import numpy as np
import pandas as pd
import ast
import requests

from time import time, sleep
from datetime import datetime

class Data:
    
    #Constructor
    def __init__(self, clientID, clientSecret):
        try:
            accessToken = requests.post(url = f'https://id.twitch.tv/oauth2/token?client_id={clientID}&client_secret={clientSecret}&grant_type=client_credentials')
        except:
            accessToken = None
        self.accessToken = accessToken
        
        try:
            token = self.accessToken.json()['access_token']
        except:
            token = None
        
        self.headers = {'Client-ID' : clientID,
                        "Authorization" : f"Bearer {token}"}
        
        self.dataframes = {}
        self.main = None
    
    #Los métodos [__getitem__] y [__setitem__] permiten utilizar la clase como un diccionario
    def __getitem__(self, key):
        return self.dataframes[key]
    
    def __setitem__(self, key, value):
        self.dataframes[key] = value
        
    def read_csvs(self, paths):
        """
        
        PUBLIC METHOD
        
        
        paths (list<str>): Una lista de cadenas especificando el directorio de cada uno de los csv's.
               El formato de los archivos csv debe ser {name}_data.csv.
        
        Convierte los csv's en pd.DataFrame, guarda el primer pd.DataFrame bajo self.main y el resto bajo claves {name}.
        """
        
        for index, path in enumerate(paths):
            df = pd.read_csv(path)
            if index == 0:
                self.main = df
            else:
                self.dataframes[path[:-9]] = df
        
    def extract(self, endpoints, batches, batchSize = 500, fields = '*', keep_logs = False, show_logs = True, save_csv = False):
        """
        
        PUBLIC METHOD
        
        
        endpoints (str): Una lista de cadenas especificando el endpoint de la API de IGDB, sin la url base.
        
        batches (int): Número de requests máximo por cada endpoint.
        
        BatchSize (int): Número de datos extraidos en cada request (máximo permitido por la API: 500).
        
        fields (str): Campos de las tablas extraidas de los endpoints. '*' para extraer todos.
        
        keep_logs (bool): Guarda registros de las requests en un archivo logs.txt.
        
        show_logs (bool): Imprime en la consola los registros de las requests.
        
        save_csv (bool): Guarda todos los pd.DataFrame generados en archivos .csv en formato {endpoint}_data.csv
        
        
        Extrae datos de los endpoints de la API de IGDB, los convierte a un pd.DataFrame,
        guarda el primer pd.DataFrame bajo self.main y el resto bajo claves {endpoint}.
        """
        
        #Trackeo del tiempo de ejecución de la función para llevar los registros
        start = time()
        
        #Aquí iteramos sobre los endpoints
        for index, endpoint in enumerate(endpoints):
            
            #Llamamos [_fetchData] que nos devuelve una lista con diccionarios de los datos del endpoint
            data = self._fetchData(endpoint = f'https://api.igdb.com/v4/{endpoint}', headers = self.headers, fields = fields, keep_logs = keep_logs, show_logs = show_logs, batches = batches, batchSize = batchSize)
            
            #Creamos un pd.DataFrame con la lista data
            df = pd.DataFrame(data)
            
            #Si es el primer endpoint de la lista, 
            if index == 0:
                #lo guardamos en self.main
                self.main = df
            else:
                #si no, lo guardamos bajo una clave
                self.dataframes[endpoint] = df
                
            #Creamos un csv si es necesario
            if save_csv:
                df.to_csv(f'{endpoint}_data.csv')
                
        #Trackeo del tiempo de ejecución de la función para llevar los registros
        end = time()
        duration = round(end - start)
        hours = duration // 60 // 60
        minutes = duration // 60 % 60
        seconds = duration % 60
        
        #Registro
        now = datetime.strftime(datetime.now(), '%x %X')
        log_str = f'[Data.extract] | {now} | DATA EXTRACTION FINISHED | TOTAL TIME: {hours} hours, {minutes} minutes and {seconds} seconds'
        
        #Guardamos registros si es necesario
        if keep_logs:
            self._log(log_str)

        #Mostramos registros si es necesario
        if show_logs:
            print(log_str)
    
    def parseLists(self, columns, data_frames, fields, data_frame = None, accessColumns = 'id', inplace = False):
        """
        
        PUBLIC METHOD
        
        
        columns (list<str>): Lista de columnas que se quieren modificar.
        
        dataframes (list<str>): Lista de claves de dataframes que se quieren usar para modificar las columns.
        
        fields (list<dynamic>): Lista de columnas de los dataframes.
        
        accessColumns (list<dynamic> or str): Columna(s) de referencia (normalmente el foreign id de la tabla).
        
        inplace (bool): Se marca como True si se quiere realizar la operación in-place.
        
        Este método modifica el contenido de las columnas especificadas del self.main por contenido de otras
        columnas de otros pd.DataFrame de self.dataframes. Omite los np.NaN y llena con np.NaN aquellos elementos
        que no es capaz de encontrar en los dataframes de referencia.
        
        """
        
        #Comprueba si accessColumns es tipo str o list
        if isinstance(accessColumns, list):
            
            #Gestión de errores
            if len(data_frames) != len(columns) != len(fields) != len(accessColumns):
                raise IndexError("'columns', 'dataframes' and 'fields' must all be the same size!")
            #Itera sobre los parámetros y llama [self._replaceIds] que se encarga de cambiar los elementos de cada columna
            for column, dataframe, field, accessColumn in zip(columns, data_frames, fields, accessColumns):
                #Lógica de in-place
                if inplace:
                    if data_frame is None:
                        df = self.main
                    else:
                        df = self.dataframes[dataframe]
                else:
                    if data_frame is None:
                        df = self.main.copy()
                    else:
                        df = self.dataframes[dataframe].copy()
                
                
                self._replaceIds(df, column, self.dataframes[dataframe], field, accessColumn = accessColumn)
        else:
            
            #Gestión de errores
            if len(data_frames) != len(columns) != len(fields):
                raise IndexError("'columns', 'dataframes' and 'fields' must all be the same size!")

            #Itera sobre los parámetros y llama [self._replaceIds] que se encarga de cambiar los elementos de cada columna
            for column, dataframe, field in zip(columns, data_frames, fields):
                #Lógica de in-place
                if inplace:
                    if data_frame is None:
                        df = self.main
                    else:
                        df = self.dataframes[dataframe]
                else:
                    if data_frame is None:
                        df = self.main.copy()
                    else:
                        df = self.dataframes[dataframe].copy()
                self._replaceIds(df, column, self.dataframes[dataframe], field, accessColumn = accessColumns)

        #Lógica de in-place
        if not inplace:
            return df
    
    def filterColumns(self, columns, inplace = False):
        """
        
        PUBLIC METHOD
        
        
        columns (list<dynamic>): Columnas de self.main.
        
        inplace (bool): Se marca como True si se quiere realizar la operación in-place.
        
        Filtra el self.main de tal modo que solamente contenga las columnas que se especifique.
        
        """
        
        if not inplace:
            return self.main[columns]
        else:
            self.main = self.main[columns]
            

    def splitColumn(self, column, query_field, queries, data_frame = None , inplace = False):
        """
        
        PUBLIC METHOD

        
        """
        if inplace:
            if data_frame is None:
                df = self.main
            else:
                df = self.dataframes[data_frame]
        else:
            if data_frame is None:
                df = self.main.copy()
            else:
                df = self.dataframes[data_frame].copy()

        for element in queries:
            self._filter(df, column, query_field, element)

        if not inplace:
            return df
        
    
    def _fetchData(self, endpoint, headers, fields, batches = 100000, batchSize = 500, keep_logs = True, show_logs = False):
        """
        
        PRIVATE METHOD
        

        """
        
        data = []
        startTime = time()

        for offset in range(0, batchSize * batches, batchSize):
            start = time()

            response = requests.post(url = endpoint, headers = headers, data = f'fields {fields}; limit {batchSize}; offset {offset};')

            end = time()
            responseTime = end - start

            data.extend(response.json())

            if len(response.json()) == 0:
                break

            now = datetime.strftime(datetime.now(), '%x %X')

            log_str = f'[REQUEST] | {now} | Status {response.status_code} | Endpoint: {endpoint} | Batch {round(offset/batchSize) + 1} | Response time [{round(responseTime, 2)}s] | Time elapsed [{round(end - startTime, 2)}s]'

            if show_logs:
                print(log_str)

            if keep_logs:
                self._log(log_str)

            if responseTime < 0.255:
                sleep(0.255 - responseTime)

        endTime = time()

        duration = endTime - startTime

        now = datetime.strftime(datetime.now(), '%x %X')
        log_str = f'[FETCH_DATA] | {now} | Total time {round(duration, 2)}s | Dataset size {len(data)}'

        if show_logs:
            print(f'\n{log_str}\n\n' + '='*100 + '\n\n')

        if keep_logs:
            self._log(log_str)
        return data        
    
    def _log(self, log_str):
        """
        
        PRIVATE METHOD

        
        """
        with open('logs.txt', 'a+') as file:
            file.write(f'{log_str}\n')
        
    def _replaceIds(self, main_df, main_column, foreign_df, foreign_column, accessColumn):
        """
        
        PRIVATE METHOD

        
        """
        df = main_df

        id_field_dict = self._getIdFieldDict(foreign_df, accessColumn, foreign_column)

        def replace(ids):
            def toInt(content):
                try:
                    return int(content)
                except ValueError:
                    return content
            
            if isinstance(ids, (list, np.ndarray)):
                return [id_field_dict.get(toInt(id_), np.nan) for id_ in ids]
            elif ids is np.NaN:
                return ids
            return id_field_dict.get(toInt(ids), np.nan)
        
        def parseStr(s):
            if pd.isna(s):
                return s

            try:
                return ast.literal_eval(s)
            except (ValueError, SyntaxError, TypeError):
                pass

            try:
                return int(s)
            except ValueError:
                pass

            try:
                return float(s)
            except ValueError:
                pass

            return s
            
        if self._isStr(df[main_column]):
            df[main_column] = df[main_column].apply(parseStr)

        df[main_column] = df[main_column].apply(replace)

    def _isStr(self, series):
        """
        
        PRIVATE METHOD

        
        """
        cleaned_series = series.dropna()
        return all(isinstance(element, str) for element in cleaned_series)
    
    def _isInt(self, series):
        """
        
        PRIVATE METHOD

        
        """
        cleaned_series = series.dropna()
        return all(isinstance(element, int) for element in cleaned_series)

    def _isFloat(self, series):
        """
        
        PRIVATE METHOD

        
        """
        cleaned_series = series.dropna()
        return all(isinstance(element, float) for element in cleaned_series)

    def _isObject(self, series):
        """
        
        PRIVATE METHOD

        
        """
        cleaned_series = series.dropna()
        return all(isinstance(element, object) for element in cleaned_series)

    def _getIdFieldDict(self, df, accessColumn, column):
        """
        
        PRIVATE METHOD

        
        """
        return df.set_index(accessColumn)[column].to_dict()
    
    def _filter(self, data_frame, column, query_field, query):
        """
        
        PRIVATE METHOD

        
        """
        df = data_frame
        return_column = []
        for item, queryItem in zip(df[column], df[query_field]):
            if query == queryItem:
                return_column.append(item)
            else:
                return_column.append(np.nan)
        df[f'{column}_{query}'] = return_column
        return df