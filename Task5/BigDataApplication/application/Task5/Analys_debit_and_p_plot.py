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
        diff_q = np.array([row[1] for row in self.data])
        avg_p = np.array([row[2] for row in self.data])

        start_index = start_data(diff_q)
        
        dates = dates[start_index:]
        diff_q = diff_q[start_index:]
        avg_p = avg_p[start_index:]
        
        return dates, diff_q, avg_p
    
    
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
    
    def __init__(self, dates, diff_q, avg_p, field_name):
        
        self.dates = dates
        self.diff_q = diff_q
        self.avg_p = avg_p
        self.field_name = field_name
        
        self.avg_diff_q = moving_average(self.diff_q, 12)
        self.avg_avg_p = moving_average(self.avg_p, 12)

    def plot_scatter(self, alpha=0.6, figsize=(20, 8)):
        fig, ax1 = plt.subplots(figsize=figsize)

        ax2 = ax1.twinx()

    # Plot data
        ax1.fill_between(self.dates, self.diff_q, color='deepskyblue', label='diff flow rate', alpha=0.85)
        ax2.plot(self.dates, self.avg_p, color='brown', label='pressure at field', linewidth=1, linestyle='dotted')
        ax1.plot(self.dates[11:], self.avg_diff_q, color='navy', linewidth=2, label='moving averge diff flow rate')
        ax2.plot(self.dates[11:], self.avg_avg_p, color='red', linewidth=2, label='moving averge pressure')

    # Set labels
        ax1.set_xlabel('Date', fontsize=14)
        ax1.set_ylabel('Flow rate, m^3', fontsize=14)
        ax2.set_ylabel('Pressure, psi', fontsize=14)
        ax1.set_title(f"Field {self.field_name}. Averge Pressure and diff of flow rate.", fontsize=16)

    # Set x-ticks
        ax1.set_xticks(range(0, len(self.dates), 12))
        ax1.set_xticklabels([self.dates[i] for i in range(0, len(self.dates), 12)], rotation=45, fontsize=12)

    # Set grid and background color
        ax1.grid(True)
        ax1.set_facecolor('aliceblue')

    # Create legends
        lines_1, labels_1 = ax1.get_legend_handles_labels()
        lines_2, labels_2 = ax2.get_legend_handles_labels()
        ax1.legend(lines_1 + lines_2, labels_1 + labels_2, loc='upper left', fontsize=14)

        #plt.show()
        plt.savefig(f"/app/output/{field_name}_debit_and_p.png")
        
        
def main_plot(field_value):

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    file_path_json = os.path.join(project_root, 'config_to_connection.json')
    
    db_params = read_json_file(file_path_json)
    query = f"SELECT date_origin, diff_q, avg_p_plast FROM debit_and_p WHERE debit_and_p.field = {field_value}  AND debit_and_p.avg_p_plast <> 0  AND ABS(debit_and_p.avg_p_plast) < 300 ORDER BY debit_and_p.date_origin;"

	
	
    try:
        db_conn = DatabaseConnector(db_params['db_params'])
        db_conn.connect()
        data = db_conn.execute_query(query, field_value)

        data_processor = DataProc(data)

        dates, sum_q_prod, sum_q_inj = data_processor.convert_data()
		
        db_conn.close()
            
        try:
            plotter = Plotter(dates, sum_q_prod, sum_q_inj, field_value)
            plotter.plot_scatter()
            print(f'График сохранен в /app/output/{field_name}_debit_and_p.png')
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
