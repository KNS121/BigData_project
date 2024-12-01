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
    
    query_create_table = read_sql_file('/app/Task2/create_result_table_time_analys.sql')
    query_insert_data = read_sql_file('/app/Task2/insert_into_result_table_time_analys.sql')
    
    try:
        data_proc = DataProcessor(db_params['db_params'], query_create_table, query_insert_data)
        data_proc.connect()
        data_proc.create_result_table()
        data_proc.insert_data()
        print('Данные обработаны.')
        
        try:
            data_proc.up_miss("avgtime", 
                        "2015-12-01", "2017-04-01",
                        "2014-11-01", "2015-11-01",
                        "avg_time_inj", "avg_time_prod")
            
            print('Восстановлены пропущенные значения.')
            data_proc.close()

        except:
            print('Не удалось восстановить пропущенные значения.')
            data_proc.close()
            
    except:
        print("Не удалось выполнить обработку данных.")
        data_proc.close()

if __name__ == "__main__":
    main()
