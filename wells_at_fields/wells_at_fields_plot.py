import os
import re
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import config_to_connection
from DB_CONNECT import DatabaseConnector
from WellProcessor import WellDataProcessor
from Well_PLOTTER import WellPlotter

def main():
    db_connector = DatabaseConnector(config_to_connection.db_params)
    well_processor = WellDataProcessor()
    well_plotter = WellPlotter()

    # Подключение к базе данных
    db_connector.connect()

    # Получение данных из базы данных
    production_wells_data = db_connector.execute_query("SELECT Скважина, Месторождение, coordx, coordy FROM  df_oil_for_bd_with_coord_red")
    injection_wells_data = db_connector.execute_query("SELECT Скважина, Месторождение, coordx, coordy FROM  df_ppd_for_bd_with_coord_red")

    # Закрытие соединения с базой данных
    db_connector.close()

    # Обработка данных
    well_processor.add_wells(production_wells_data, 'production')
    well_processor.add_wells(injection_wells_data, 'injection')

    # Построение графиков
    wells = well_processor.get_wells()
    well_plotter.plot_wells(wells)

    #print("Графики сохранены.")