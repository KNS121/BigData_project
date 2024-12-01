import numpy as np
import matplotlib.pyplot as plt
import os
import sys
import matplotlib.ticker as ticker
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from DB_CONNECT import DatabaseConnector
from read_json_config import read_json_file


def start_data(array):
    start_day = 0
    for i in range(len(array)):
        if array[i]==0 and array[i+1]!=0:
            start_day = i+1
            break
    return start_day

def moving_average(data, window_size):

    moving_averages = []
    for i in range(len(data) - window_size + 1):
        window = data[i:i + window_size]
        window_average = sum(window) / window_size
        moving_averages.append(window_average)

    return moving_averages

class DataProc:
    def __init__(self, data):
        self.data = data

    def convert_data(self):
        dates = np.array([row[0] for row in self.data])
        sum_q_prod = np.array([row[1] for row in self.data])
        sum_q_inj = np.array([row[2] for row in self.data])

        start_index = start_data(sum_q_prod)
        
        dates = dates[start_index:]
        sum_q_prod = sum_q_prod[start_index:]
        sum_q_inj = sum_q_inj[start_index:]
        
        return dates, sum_q_prod, sum_q_inj
    
    
   # @staticmethod
    #def filter_data(data):
     #   result = []
      #  include_next_zero = False

       # for row in data:
        #    if row[1] != 0:
         #       result.append(row)
          #      include_next_zero = True
           # elif include_next_zero:
            #    result.append(row)
             #   include_next_zero = False

       # return result

class Plotter:
    
    def __init__(self, dates, sum_q_prod, sum_q_inj, field_name):
        
        self.dates = dates
        self.sum_q_prod = sum_q_prod
        self.sum_q_inj = sum_q_inj
        self.field_name = field_name
        
        self.avg_sum_q_prod = moving_average(self.sum_q_prod, 12)
        self.avg_sum_q_inj = moving_average(self.sum_q_inj, 12)

    def plot_scatter(self, alpha=0.6, figsize=(20, 8)):
        #plt.style.use('_mpl-gallery')
        fig, ax = plt.subplots(figsize=figsize)
        
        
        
        ax.fill_between(self.dates , self.sum_q_prod, color = 'deepskyblue',label='Producing Wells', alpha=0.85)
        ax.fill_between(self.dates , self.sum_q_inj, color = 'lime', label='Injection Wells', alpha=0.85)
        #ax.fill_between(self.dates, 0, self.sum_q_inj, alpha=0.5, color='blue', label='Injection Wells')
        #ax.fill_between(self.dates, self.sum_q_inj, self.sum_q_inj + self.sum_q_prod, alpha=0.5, color='saddlebrown', label='Producing Wells')
        
        ax.plot(self.dates[11:], self.avg_sum_q_prod, color = 'navy',linewidth=2)
        ax.plot(self.dates[11:], self.avg_sum_q_inj, color = 'green',linewidth=2)
        ax.set_xlabel('date', fontsize=14)
        ax.set_ylabel('Flow rate, m^3', fontsize=14)
        ax.set_title(f"Field {self.field_name}. Sum. Flow rate by wells", fontsize=16)
        ax.legend(fontsize=14)
        plt.xticks(range(0, len(self.dates), 12), rotation=45, fontsize=12)
        ax.grid(True)
        ax.set_facecolor('aliceblue')
# Вращаем метки дат для лучшей читаемости
        plt.savefig(f"/app/output/{field_name}_q_prod_and_q_inj.png")
        
        
def main_plot(field_value):
    #print(field_value)
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    file_path_json = os.path.join(project_root, 'config_to_connection.json')
    
    db_params = read_json_file(file_path_json)
    #print(db_params['db_params'])
    query = f"SELECT date_origin, sum_q_prod, sum_q_inj FROM q_prod_and_inj WHERE q_prod_and_inj.field = {field_value} ORDER BY q_prod_and_inj.date_origin;"
    #query = f"SELECT * FROM q_prod_and_inj;"
    db_conn = DatabaseConnector(db_params['db_params'])
    db_conn.connect()
    #db_conn.close()
    data = db_conn.execute_query(query, field_value)
    #filtered_data = DataProc.filter_data(data)
    #try:
        
        
    
    data_processor = DataProc(data)
    #print(data)
    dates, sum_q_prod, sum_q_inj = data_processor.convert_data()
            
    #try:
    plotter = Plotter(dates, sum_q_prod, sum_q_inj, field_value)
    plotter.plot_scatter()
    #except:
      #  print('Не удалось построить график.')
        
    #except:
        #print('Не удалось конвертировать данные')
        
    db_conn.close()
        
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python your_script.py <field_name>")
        sys.exit(1)

    field_name = sys.argv[1]
    main_plot(field_name)
