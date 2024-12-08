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
    
    query_create_table = read_sql_file('/app/Task3/create_table.sql')
    query_insert_data = read_sql_file('/app/Task3/insert_into_table.sql')
    
    try:
        data_proc = DataProcessor(db_params['db_params'], query_create_table, query_insert_data)
        data_proc.connect()
        data_proc.create_result_table()
        data_proc.insert_data()
        print('Данные обработаны.')
        
        try:
            data_proc.up_miss("dsw_by_q_inj",
                        "2015-12-01", "2017-04-01",
                        "2014-11-01", "2015-11-01",
                        "sum_q_inj", "dsw")
            
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
