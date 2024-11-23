import numpy as np
import matplotlib.pyplot as plt
from matplotlib.tri import Triangulation
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import config_to_connection
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

    def plot_scatter(self, style='default', alpha=0.6, figsize=(12, 8), size_scale=400):
        plt.style.use(style)
        fig, ax = plt.subplots(figsize=figsize)

        # Нормализация значений Z для определения размера точек
        sizes = (self.Z - self.z_min) / (self.z_max - self.z_min) * size_scale
        
        scatter = ax.scatter(self.X, self.Y, c=self.Z, cmap='viridis', s=sizes, edgecolor='k', alpha=alpha, vmin=self.z_min, vmax=self.z_max)
        plt.colorbar(scatter, label='Q, куб. м')
        plt.xlabel('X, м')
        plt.ylabel('Y, м')
        plt.title('Месторождение: ' + str(self.field_name) + ' год: ' + str(self.year_name))
        plt.show()

        
        
def find_z_limits(field_value, year_range):
    all_z = []
    db_params = config_to_connection.db_params
    db_connector = DatabaseConnector(db_params)
    db_connector.connect()

    for year in year_range:
        query = f"SELECT sum_q_prod, coordx, coordy FROM q_with_coords WHERE q_with_coords.field = {field_value} AND q_with_coords.coordx <> 0 AND q_with_coords.coordy <> 0 AND q_with_coords.sum_q_prod > 0;"
        data = db_connector.execute_query(query, (field_value, year))
        all_z.extend([row[0] for row in data])

    db_connector.close()

    z_min = min(all_z)
    z_max = max(all_z)

    return z_min, z_max

    
def main_plot(field_value, year_value, z_min, z_max, style='default', alpha=0.6, figsize=(12, 8), size_scale=800):

    
    db_params = config_to_connection.db_params
    db_connector = DatabaseConnector(db_params)
    db_connector.connect()

    query = f"SELECT sum_q_prod, coordx, coordy FROM q_with_coords WHERE q_with_coords.field = {field_value} AND q_with_coords.date_origin = '{year_value}' AND q_with_coords.coordx <> 0 AND q_with_coords.coordy <> 0 AND q_with_coords.sum_q_prod > 0;"

    data = db_connector.execute_query(query, (field_value, year_value))

    db_connector.close()

    data_processor = DataProcessor(data)
    
    X, Y, Z = data_processor.convert_data()

    plotter = Plotter(X, Y, Z, field_value, year_value, z_min, z_max)
    plotter.plot_scatter(style, alpha, figsize, size_scale)

