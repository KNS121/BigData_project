#!/bin/bash

echo "Задача 1: Анализ зависимости дебета добывающих скважин от дебета нагнетательных"
echo "Задача 2: Анализ времени работы нагнетательных и добывающих скважин"
echo "Задача 3: Анализ зависимости водонасыщенности от дебета нагнетательных скважин"
echo "Задача 4: Анализ распределения дебета добывающих скважин на месторождении"
echo "Задача 5: Анализ зависимости среднепластового давления от суммарного дебета на месторождении"

# Запрос у пользователя номера задачи
read -p "Введите номер задачи 1-5 или 'a' для выполнения всех задач: " task_number

# Проверка, что task_number является числом или 'a'
if ! [[ $task_number =~ ^[0-9]+$ ]] && [[ $task_number != "a" ]]; then
    echo "task_number должно быть числом от 1 до 5 или 'a'"
    exit 1
fi

# Функция для выполнения задачи
run_task() {
    local task=$1
    local field=$2
    local year=$3

    case $task in
        1)
            sudo docker-compose exec python_app python /app/Task1/Analys_Q_inj_and_prod_plot.py $field
            ;;
        2)
            sudo docker-compose exec python_app python /app/Task2/Time_wells_analys_plot.py $field
            ;;
        3)
            sudo docker-compose exec python_app python /app/Task3/Sw_by_q_inj_plot.py $field
            ;;
        4)
            sudo docker-compose exec python_app python /app/Task4/Sum_Q_with_coords_plot.py $field $year
            ;;
        5)
            sudo docker-compose exec python_app python /app/Task5/Analys_debit_and_p_plot.py $field
            ;;
        *)
            echo "Неверный номер задачи"
            exit 1
            ;;
    esac
}

export -f run_task

# Функция для измерения времени выполнения
measure_time() {
    local start_time=$(date +%s)
    "$@"
    local end_time=$(date +%s)
    local elapsed_time=$((end_time - start_time))
    local minutes=$((elapsed_time / 60))
    local seconds=$((elapsed_time % 60))
    echo "Время выполнения: ${minutes} мин ${seconds} сек"
}

# Если task_number равно 'a', выполнить все задачи для всех месторождений и годов
if [[ $task_number == "a" ]]; then
    measure_time bash -c '
    for task in 1 2 3 4 5; do
        for field in 6 7; do
            if [[ $task == 4 ]]; then
                for year in {1992..2023}; do
                    run_task $task $field $year
                done
            else
                run_task $task $field
            fi
        done
    done
    '
else
    # Определение, какой скрипт запускать в зависимости от номера задачи
    if [[ $task_number -lt 1 || $task_number -gt 5 ]]; then
        echo "task_number должно быть между 1 и 5"
        exit 1
    fi

    # Запрос у пользователя аргумента field_name
    read -p "Введите field_name 6-7: " field_name

    # Проверка, что field_name является числом и находится в заданном диапазоне
    if ! [[ $field_name =~ ^[0-9]+$ ]]; then
        echo "field_name должно быть числом"
        exit 1
    elif [ $field_name -lt 6 ]  [ $field_name -gt 7 ]; then
        echo "field_name должно быть в диапазоне от 6 до 7"
        exit 1
    fi

    if [[ $task_number == 4 ]]; then
        read -p "change year: " year_name
        run_task $task_number $field_name $year_name
    else
        run_task $task_number $field_name
    fi
fi
