import os
import sys
from Read_SQL import read_sql_file
from DATAPROCESSOR import DataProcessor
from read_json_config import read_json_file
from update_diff_q import update_diff_q

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


def main():
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    file_path_json = os.path.join(project_root, 'config_to_connection.json')
    
    db_params = read_json_file(file_path_json)
    
    query_create_table = read_sql_file('create_table_debit_and_p.sql')
    query_insert_data = read_sql_file('insert_into_table_debit_and_p.sql')
    
    try:
        data_proc = DataProcessor(db_params['db_params'], query_create_table, query_insert_data)
        data_proc.connect()
        data_proc.create_result_table()
        data_proc.insert_data()
        print('Данные обработаны.')
        
        try:
            data_proc.up_miss("debit_and_p",
                        "2015-12-01", "2017-04-01",
                        "2018-01-01", "2019-01-01",
                        "sum_q_inj", "sum_q_prod")
            print('Восстановлены пропущенные значения sum_q_prod.')

            data_proc.execute_query(update_diff_q(), False)
            print('Обновлены данные разницы давлений.')

            data_proc.up_miss("debit_and_p",
                        "2015-12-01", "2017-04-01",
                        "2018-01-01", "2019-01-01",
                        "avg_p_plast", "diff_q")

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

