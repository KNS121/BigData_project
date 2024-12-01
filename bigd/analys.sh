#!/bin/bash

# Запрос у пользователя номера задачи
read -p "Введите номер задачи 1-5: " task_number

if [[ $task_number -lt 1 || $task_number -gt 5 ]]; then
    echo "task_number must be between 1 and 5"
    exit 1
fi

# Запрос у пользователя аргумента field_name
read -p "Введите field_name 6-7: " field_name

# Проверка, что field_name является числом и находится в заданном диапазоне
if ! [[ $field_name =~ ^[0-9]+$ ]]; then
    echo "field_name должно быть числом"
    exit 1
elif [ $field_name -lt 6 ] || [ $field_name -gt 7 ]; then
    echo "field_name должно быть в диапазоне от 6 до 7"
    exit 1
fi


# Определение, какой скрипт запускать в зависимости от номера задачи
case $task_number in
    1)
        sudo docker-compose exec python_app  python /app/Task1/Analys_Q_inj_and_prod_plot.py $field_name
        ;;
    2)
        sudo docker-compose exec python_app python /app/Task2/Time_wells_analys_plot.py $field_name
        ;;
    4)
        read -p "change year: " year_name
        sudo docker-compose exec python_app python /app/Task4/Sum_Q_with_coords_plot.py $field_name $year_name
        ;;
    *)
        echo "Неверный номер задачи"
        exit 1
        ;;
esac
