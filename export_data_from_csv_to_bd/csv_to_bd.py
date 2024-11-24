import pandas as pd
import numpy as np
import psycopg2
from io import StringIO
import re
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import config_to_connection
from DB_CONNECT import DatabaseConnector

def name_of_col_to_norm_view(column):
    replacements = [
        (r'[,]', '_'), (r'[.]', ''), (r'[(]', ''), (r'[%]', 'perc'),
        (r'[+]', 'plus'), (r'[-]', 'minus'), (r'[)]', ''), (r'[/]', '_'),
        (r'\s+', '_'), (r'[№]', 'number_of')]
    for pattern, repl in replacements:
        column = re.sub(pattern, repl, column)
    column = column.lower()
    return column[:28] if len(column) > 28 else column

def ensure_unique_column_names(df):
    columns_array = df.columns.tolist()  # Преобразуем индекс колонок в список
    count_dict = {}

    for i in range(len(columns_array)):
        element = columns_array[i]
        if element in count_dict:
            count_dict[element] += 1
            columns_array[i] = f"one_{element}"
        else:
            count_dict[element] = 0

    df.columns = columns_array  # Присваиваем новые имена колонок после цикла
    return df




class DatabaseManager:
    def __init__(self, params):
        self.db_connector = DatabaseConnector(params)

    def connect(self):
        self.db_connector.connect()

    def close(self):
        self.db_connector.close()

    def create_table(self, table_name, df):
        try:
            columns = [f'"{column}" {"INTEGER" if df[column].dtype == "int64" else "FLOAT" if df[column].dtype == "float64" else "TEXT"}' for column in df.columns]
            create_table_query = f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                {', '.join(columns)}
            );
            '''
            print(f"SQL-запрос для создания таблицы {table_name}:")
            print(create_table_query)
            self.db_connector.cur.execute(create_table_query)
            self.db_connector.conn.commit()
            print(f"Таблица {table_name} создана.")
        except psycopg2.Error as e:
            print(f"Ошибка при создании таблицы: {e}")

    def import_data(self, table_name, df):
        try:
            output = StringIO()
            df.to_csv(output, sep=',', header=False, index=False)
            output.seek(0)
            columns = [name_of_col_to_norm_view(column) for column in df.columns]
            copy_query = f'''
            COPY {table_name} ({', '.join(columns)})
            FROM STDIN
            DELIMITER ','
            CSV;
            '''
            print(f"SQL-запрос для импорта данных в таблицу {table_name}:")
            print(copy_query)
            self.db_connector.cur.copy_expert(sql=copy_query, file=output)
            self.db_connector.conn.commit()
            print(f"Данные импортированы в таблицу {table_name}.")
        except psycopg2.Error as e:
            print(f"Ошибка при импорте данных: {e}")

def read_csv_and_convert_mixed_types(file_path):
    df = pd.read_csv(file_path, low_memory=False, encoding='cp1251', sep=";")
    df.columns = [name_of_col_to_norm_view(column) for column in df.columns]
    df = ensure_unique_column_names(df)
    df.fillna('NULL', inplace=True)
    
    for col in df.columns:
        if df[col].apply(type).nunique() > 1:
            df[col] = df[col].astype(str)
    
    try:
        
        df = df.drop(columns = ['unnamed:_0'])
        df.fillna('NULL', inplace=True)    
        df.columns = [name_of_col_to_norm_view(column) for column in df.columns]
        df = ensure_unique_column_names(df)
    except:
        
        df.fillna('NULL', inplace=True)    
        df.columns = [name_of_col_to_norm_view(column) for column in df.columns]
        df = ensure_unique_column_names(df)
        print(df.columns)
    
    return df

def process_csv_files(csv_files):
    db_manager = DatabaseManager(config_to_connection.db_params)
    db_manager.connect()
    for csv_file_path in csv_files:
        df = read_csv_and_convert_mixed_types(csv_file_path)
        table_name = os.path.splitext(os.path.basename(csv_file_path))[0]
        print(f"Имена столбцов перед созданием таблицы {table_name}:")
        for col in df.columns:
            print(col)
        db_manager.create_table(table_name, df)
        db_manager.import_data(table_name, df)
    db_manager.close()
