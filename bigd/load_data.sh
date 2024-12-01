#!/bin/bash

sudo docker-compose exec python_app python /app/export_data_from_csv_to_bd/csv_to_bd.py
sudo docker-compose exec python_app python /app/Task1/Analys_Q_inj_and_prod.py
sudo docker-compose exec python_app python /app/Task2/Time_wells_analys.py
sudo docker-compose exec python_app python /app/Task4/Sum_Q_with_coords.py

