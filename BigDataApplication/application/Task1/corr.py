import numpy as np
import os
import sys
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from DB_CONNECT import DatabaseConnector
from read_json_config import read_json_file

class DataProc:
    def __init__(self, data):
        self.data = data

    def convert_data(self):
        field = np.array([row[0] for row in self.data])
        sum_q_prod = np.array([row[1] for row in self.data])
        sum_q_inj = np.array([row[2] for row in self.data])

        
        return field, sum_q_prod, sum_q_inj



def main_df_corr():
    
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    file_path_json = os.path.join(project_root, 'config_to_connection.json')
    
    db_params = read_json_file(file_path_json)
    query = f"SELECT field, sum_q_prod, sum_q_inj FROM q_prod_and_inj WHERE q_prod_and_inj.sum_q_inj <> 0 AND q_prod_and_inj.sum_q_prod <> 0 ORDER BY q_prod_and_inj.date_origin;"

    db_conn = DatabaseConnector(db_params['db_params'])
    db_conn.connect()
    data = db_conn.execute_query(query)
        
    data_processor = DataProc(data)
    field, sum_q_prod, sum_q_inj = data_processor.convert_data()
    db_conn.close()
    
    df = pd.DataFrame({
    'field': field,
    'sum_q_prod': sum_q_prod,
    'sum_q_inj': sum_q_inj
    })
    
    
    for field in df['field'].unique():
        correlation_matrix = df[df['field'] == field].drop(columns = ['field']).corr()


        plt.figure(figsize=(8, 8))
        sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', linewidths=0.5)
        plt.title(f'Correlation Matrix of flow_rate_inj and flow_rate_prod field {field}')
        #plt.show()
        plt.savefig(f"/app/output/{field}_corr_matrix_q_prod_and_q_inj.png")
        print(f"График сохранен в /output/{field}_corr_matrix_q_prod_and_q_inj.png")
    