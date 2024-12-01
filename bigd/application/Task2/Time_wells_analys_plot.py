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
        time_prod = np.array([row[1] for row in self.data])
        time_inj = np.array([row[2] for row in self.data])

        start_index = start_data(time_prod)
        
        dates = dates[start_index:]
        time_prod = time_prod[start_index:]
        time_inj = time_inj[start_index:]
        
        return dates, time_prod, time_inj
    
    
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
    
    def __init__(self, dates, time_prod, time_inj, field_name):
        
        self.dates = dates
        self.time_prod = time_prod
        self.time_inj = time_inj
        self.field_name = field_name
        
        self.avgtime_prod = moving_average(self.time_prod, 12)
        self.avgtime_inj = moving_average(self.time_inj, 12)

    def plot_scatter(self, alpha=0.6, figsize=(20, 8)):
        #plt.style.use('_mpl-gallery')
        fig, ax = plt.subplots(figsize=figsize)
        
        ax.stackplot(self.dates , self.time_prod, labels=['Producing Wells'])
        ax.stackplot(self.dates , self.time_inj, color = 'coral', labels=['Injection Wells'])
        ax.plot(self.dates[11:], self.avgtime_prod, color = 'navy',linewidth=2)
        ax.plot(self.dates[11:], self.avgtime_inj, color = 'firebrick',linewidth=2)
        ax.set_xlabel('date', fontsize=14)
        ax.set_ylabel('averge time working, days', fontsize=14)
        ax.set_title(f"Field {self.field_name}. Averge time working by producing and injection wells", fontsize=16)
        ax.legend(fontsize=14)
        plt.xticks(range(0, len(self.dates), 12), rotation=45, fontsize=12)
        ax.grid(True)
        ax.set_facecolor('aliceblue')
# Вращаем метки дат для лучшей читаемости
        plt.savefig(f"/app/output/{field_name}_time_well_analys.png")

        
        
def main_plot(field_value):

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    file_path_json = os.path.join(project_root, 'config_to_connection.json')
    
    db_params = read_json_file(file_path_json)
    print(db_params['db_params'])
    query = f"SELECT date_origin, avg_time_prod, avg_time_inj FROM avgtime WHERE avgtime.field = {field_value} ORDER BY avgtime.date_origin;"
    
    db_conn = DatabaseConnector(db_params['db_params'])
    db_conn.connect()
    #db_conn.close()
    data = db_conn.execute_query(query, field_value)
    #filtered_data = DataProc.filter_data(data)
    #try:
        
        
    try:
        data_processor = DataProc(data)
        dates, time_prod, time_inj = data_processor.convert_data()
            
        try:
            plotter = Plotter(dates, time_prod, time_inj, field_value)
            plotter.plot_scatter()
        except:
            print('Не удалось построить график.')
        
    except:
        print('Не удалось конвертировать данные')
        
    db_conn.close()
        
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python your_script.py <field_name>")
        sys.exit(1)

    field_name = sys.argv[1]
    main_plot(field_name)

