#!/bin/bash

start_time=$(date +%s)

sudo docker-compose exec python_app python /app/export_data_from_csv_to_bd/csv_to_bd.py


end_time=$(date +%s)

elapsed_time=$((end_time - start_time))
minutes=$((elapsed_time / 60))
seconds=$((elapsed_time % 60))

echo "Время переноса данных из .csv в Postgresql: ${minutes} минут, ${seconds} секунд"


start_time=$(date +%s)

sudo docker-compose exec python_app python /app/Task1/Analys_Q_inj_and_prod.py
sudo docker-compose exec python_app python /app/Task2/Time_wells_analys.py
sudo docker-compose exec python_app python /app/Task3/Sw_by_q_inj.py
sudo docker-compose exec python_app python /app/Task4/Sum_Q_with_coords.py

end_time=$(date +%s)

elapsed_time=$((end_time - start_time))
minutes=$((elapsed_time / 60))
seconds=$((elapsed_time % 60))

echo "Время обработки данных Задач и переноса в Postgresql: ${minutes} минут, ${seconds} секунд"
