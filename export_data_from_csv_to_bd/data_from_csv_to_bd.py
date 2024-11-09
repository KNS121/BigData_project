import pandas as pd
import numpy as np
import psycopg2
from io import StringIO

import Names_of_df_csv
import os
import re
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import config_to_connection

def name_of_col_to_norm_view(column):
    
    column = re.sub(r'[,]', '', column)
    column = re.sub(r'[.]', '', column)
    column = re.sub(r'[(]', '', column)
    column = re.sub(r'[%]', 'perc', column)
    column = re.sub(r'[)]', '', column)
    column = re.sub(r'[/]', '_', column)
    column = re.sub(r'\s+', '_', column)
    
    
    return column




class DatabaseManager:
    
    def __init__(self, params):
        self.params = params
        self.conn = None
        self.cur = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(**self.params)
            self.cur = self.conn.cursor()
            print("Подключение к базе данных установлено.")
        
        except psycopg2.Error as e:
            print(f"Ошибка подключения к базе данных: {e}")

    def close(self):
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
            print("Соединение с базой данных закрыто.")
            
    
            
    def create_table(self, table_name, df):
        
        try:
            columns = []
            
            for column in df.columns:
                
                column_type = 'TEXT'
                
                if df[column].dtype == 'int64':
                    column_type = 'INTEGER'
                elif df[column].dtype == 'float64':
                    column_type = 'FLOAT'
                elif df[column].dtype == 'object':
                    column_type = 'TEXT'
                
                
                columns.append(f'"{column}" {column_type}')

            create_table_query = f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                {', '.join(columns)}
            );
            '''
            
            self.cur.execute(create_table_query)
            self.conn.commit()
            print(f"Таблица {table_name} создана.")
        
        except psycopg2.Error as e:
            print(f"Ошибка при создании таблицы: {e}")

    
    def import_data(self, table_name, df):
        
        try:
            output = StringIO()
            df.to_csv(output, sep=',', header=False, index=False)
            output.seek(0)
            
            columns = [ name_of_col_to_norm_view(column) for column in df.columns]
            
            copy_query = f'''
            COPY {table_name} ({', '.join(columns)})
            FROM STDIN
            DELIMITER ','
            CSV;
            '''
            self.cur.copy_expert(sql=copy_query, file=output)
            self.conn.commit()
            print(f"Данные импортированы в таблицу {table_name}.")
        
        except psycopg2.Error as e:
            print(f"Ошибка при импорте данных: {e}")

    #def verify_data(self, table_name):

        #try:
            #self.cur.execute(f'SELECT * FROM {table_name} LIMIT 10;')
            #rows = self.cur.fetchall()
            #for row in rows:
             #   print(row)
            #print(f"Данные в таблице {table_name} проверены.")
        
        except psycopg2.Error as e:
            print(f"Ошибка при проверке данных: {e}")

            
def read_csv_and_convert_mixed_types(file_path):
    
    df = pd.read_csv(file_path, low_memory=False)
    
    for col in df.columns:
        if df[col].apply(type).nunique() > 1:
            df[col] = df[col].astype(str)

    return df


def process_csv_files(csv_files):

    db_manager = DatabaseManager(config_to_connection.db_params)
    db_manager.connect()

    for csv_file_path in csv_files:
            
        df = read_csv_and_convert_mixed_types(csv_file_path)
        df = df.drop(columns = ['Unnamed: 0'])
        df.fillna('NULL', inplace=True)
            
        df.columns = [name_of_col_to_norm_view(column) for column in df.columns]
            
        table_name = os.path.splitext(os.path.basename(csv_file_path))[0]
            
        db_manager.create_table(table_name, df)
        
        db_manager.import_data(table_name, df)

        db_manager.verify_data(table_name)
            

    db_manager.close()
    
    
if __name__ == "__main__":
    csv_files = Names_of_df_csv.df_names
    process_csv_files(csv_files)