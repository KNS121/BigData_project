import os
import sys

# Исправление пути для импорта
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import config_to_connection
from DB_CONNECT import DatabaseConnector

class DataProcessor:
    def __init__(self, db_params):
        self.db_connector = DatabaseConnector(db_params)

    def connect(self):
        self.db_connector.connect()

    def create_result_table(self):
        query = """
    CREATE TABLE IF NOT EXISTS Task_2_Results_Q_prod_and_inj (
        date_previous VARCHAR(7),
        date_origin VARCHAR(7),
        Field INTEGER,
        Well INTEGER,
        Day INTEGER
    );
    """
        self.db_connector.execute_query(query, fetch_results=False)
#факт_часы_за_месяц__часы_раб
    def insert_data(self):
        query = """
    INSERT INTO Task_2_Results_Q_prod_and_inj (date_previous, date_origin, Field, Well, Day)
    SELECT
        TO_CHAR((TO_DATE(to_tdp.name_of_date_doc_col, 'YYYY-MM-DD') - interval '1 month'), 'YYYY-MM') AS date_previous,
        TO_CHAR(TO_DATE(to_tdp.name_of_date_doc_col, 'YYYY-MM-DD'), 'YYYY-MM') AS date_origin,
        to_tdp.месторождение AS Field,
        COUNT(DISTINCT to_tdp.скважина) AS CountWells,
        SUM(to_tdp.факт_часы_за_месяц__часы_раб)/24 AS CountDays
    FROM
        to_tdp AS to_tdp
    WHERE
        to_tdp.факт_дебит_жидкости_м3_сут IS NOT NULL
        AND to_tdp.факт_дебит_жидкости_м3_сут <> 'NULL'
        AND to_tdp.факт_дебит_жидкости_м3_сут <> '0'
        AND to_tdp.факт_дебит_жидкости_м3_сут <> '0.0'
        AND to_tdp.факт_дебит_жидкости_м3_сут <> 'nan'
        AND to_tdp.факт_дебит_жидкости_м3_сут <> 'NaN'
    GROUP BY
        TO_CHAR(TO_DATE(to_tdp.name_of_date_doc_col, 'YYYY-MM-DD'), 'YYYY-MM'),
        to_tdp.месторождение,
        TO_CHAR((TO_DATE(to_tdp.name_of_date_doc_col, 'YYYY-MM-DD') - interval '1 month'), 'YYYY-MM')
    """
    
        self.db_connector.execute_query(query, fetch_results=False)
        self.db_connector.commit()
    
    def close(self):
        self.db_connector.close()

def create_table_Q_prod_and_inj():
    db_params = config_to_connection.db_params
    data_proc = DataProcessor(db_params)
    data_proc.connect()
    data_proc.create_result_table()
    data_proc.insert_data()
    data_proc.close()

    print("Таблицы созданы и данные вставлены успешно.")
    
    