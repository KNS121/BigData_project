import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from Read_SQL import read_sql_file
from DATAPROCESSOR import DataProcessor
from read_json_config import read_json_file

def main():
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    file_path_json = os.path.join(project_root, 'config_to_connection.json')
    
    db_params = read_json_file(file_path_json)
    
    query_create_table = read_sql_file('create_result_table_q_with_coords.sql')
    query_insert_data = read_sql_file('insert_into_result_table_q_with_coords.sql')
    
    try:
        data_proc = DataProcessor(db_params['db_params'], query_create_table, query_insert_data)
        data_proc.connect()
        data_proc.create_result_table()
        data_proc.insert_data()
        print('Данные обработаны.')
        data_proc.close()
            
    except:
        print("Не удалось выполнить обработку данных.")
        data_proc.close()

if __name__ == "__main__":
    main()
