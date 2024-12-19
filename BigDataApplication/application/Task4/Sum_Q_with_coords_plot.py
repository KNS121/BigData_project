import numpy as np
import matplotlib.pyplot as plt
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from read_json_config import read_json_file
from DB_CONNECT import DatabaseConnector


class DataProcessor:
    def __init__(self, data):
        self.data = data

    def convert_data(self):
        Z = np.array([row[0] for row in self.data])
        X = np.array([row[1] for row in self.data])
        Y = np.array([row[2] for row in self.data])

        return X, Y, Z

class Plotter:
    def __init__(self, X, Y, Z, field_name, year_name, z_min, z_max):
        self.X = X
        self.Y = Y
        self.Z = Z
        self.field_name = field_name
        self.year_name = year_name
        self.z_min = float(z_min)
        self.z_max = float(z_max)

    def plot_scatter(self, alpha=0.6, figsize=(18, 18), size_scale=1500):
        #plt.style.use(style)
        fig, ax = plt.subplots(figsize=figsize)
        sizes = (self.Z - self.z_min) / (self.z_max - self.z_min) * size_scale
        scatter = ax.scatter(self.X, self.Y, c=self.Z, cmap='plasma', s=sizes, edgecolor='k', alpha=alpha, vmin=self.z_min, vmax=self.z_max)

# Добавление цветовой шкалы с меткой и изменением размера шрифта
        cbar = plt.colorbar(scatter, ax=ax)
        cbar.set_label('Flow rate, m^3', fontsize=14)

        plt.xlabel('X, м', fontsize=14)
        plt.ylabel('Y, м', fontsize=14)
        plt.title('Flow rate, Field ' + str(self.field_name) + ', year ' + str(self.year_name), fontsize=14)

        ax.set_facecolor('aliceblue')
        ax.grid(True)

# Раскомментируйте следующую строку, если хотите сохранить изображение
        plt.savefig(f"/app/output/Field_{field_name}_{year_name}_q_prod_coords.png")

        #plt.show()
        
        
def find_z_limits(field_value, year_range, db_connector):
    all_z = []
    
    for year in year_range:
        query = f"SELECT sum_q_prod, coordx, coordy FROM q_with_coords WHERE q_with_coords.field = {field_value} AND q_with_coords.coordx <> 0 AND q_with_coords.coordy <> 0 AND q_with_coords.sum_q_prod > 0;"
        data = db_connector.execute_query(query, (field_value, year))
        all_z.extend([row[0] for row in data])
        
    if not all_z:
        print('NotFound vaules...')
        print('Change other year')
        db_connector.close()
        sys.exit()

    z_min = min(all_z)
    z_max = max(all_z)

    return z_min, z_max




def main_plot(field_name, year_name):

    
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    file_path_json = os.path.join(project_root, 'config_to_connection.json')
    
    db_params = read_json_file(file_path_json)
        
    query = f"SELECT sum_q_prod, coordx, coordy FROM q_with_coords WHERE q_with_coords.field = {field_name} AND q_with_coords.date_origin = '{year_name}' AND q_with_coords.coordx <> 0 AND q_with_coords.coordy <> 0 AND q_with_coords.sum_q_prod > 0;"

    try:
        
        db_connector = DatabaseConnector(db_params['db_params'])
        db_connector.connect()
        data = db_connector.execute_query(query)
    
        if data == []:
            db_connector.close()
            return print(f"В {year_name} году на месторождении {field_name} добычи не было.")
    
        data_processor = DataProcessor(data)
    
        X, Y, Z = data_processor.convert_data()

        z_min, z_max = find_z_limits(field_name, range(1992,2024), db_connector)
    
        db_connector.close()
        try:
            plotter = Plotter(X, Y, Z, field_name, year_name, z_min, z_max)
            plotter.plot_scatter()
            print(f'График сохранен в /output/Field_{field_name}_{year_name}_q_prod_coords.png' )
        except:
            print('Не удалось построить график.')
    except:    
        print('Не удалось подключиться к БД...')
                 
if __name__ == "__main__":
    
    if len(sys.argv) != 3:
        print("Неверное кол-во аргументов.")
        sys.exit(1)

    field_name = sys.argv[1]
    year_name = sys.argv[2]
    main_plot(field_name, year_name)
