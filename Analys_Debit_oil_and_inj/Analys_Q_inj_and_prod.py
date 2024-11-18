import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import config_to_connection
from DB_CONNECT import DatabaseConnector

class DataProcessor:
    def __init__(self, db_params):
        self.db_connector = DatabaseConnector(db_params)

    def connect(self):
        self.db_connector.connect()

    def create_temp_table(self):
        query = """
        CREATE TEMP TABLE Vrem_Table AS
            SELECT
                df_oil_for_bd.Месторождение AS Месторождение,
                COALESCE(NULLIF(df_oil_for_bd.Факт_дебит_жидкостим3_сут, 0))::double precision AS Q_prod,
                0 AS Q_inj,
                TO_CHAR(TO_DATE(df_oil_for_bd.name_of_date_doc_col, 'YYYY-MM-DD'), 'YYYY-MM') AS date_origin
            FROM
                df_oil_for_bd
            
            UNION
            
            SELECT
                df_ppd_for_bd.Месторождение AS Месторождение,
                0 AS Q_prod,
                COALESCE(NULLIF(df_ppd_for_bd.Факт_дебит_жидкостим3_сут, 0))::double precision AS Q_inj,
                TO_CHAR(TO_DATE(df_ppd_for_bd.name_of_date_doc_col, 'YYYY-MM-DD'), 'YYYY-MM') AS date_origin
            FROM
                df_ppd_for_bd;
            """
        self.db_connector.execute_query(query)
            
        
    
    def create_result_table(self):
        query = """
        CREATE TABLE IF NOT EXISTS Results_Q_prod_and_inj (
            Месторождение INTEGER,
            Q_prod FLOAT,
            Q_inj FLOAT,
            date_origin VARCHAR(7)
        );
        """
        self.db_connector.execute_query(query)

    def insert_data(self):
        query = """
            INSERT INTO Results_Q_prod_and_inj ( Месторождение, Q_prod, Q_inj, date_origin )
            SELECT
                Месторождение AS Месторождение,
                SUM(Q_prod) AS Q_prod,
                SUM(Q_inj) AS Q_inj,
                date_origin AS date_origin,
            FROM
                Vrem_Table
            GROUP BY
                Месторождение,
                date_origin;
            """
        self.db_connector.execute_query(query)
        self.db_connector.commit()
        print('Таблица с результатами создана.')

    def close(self):
        self.db_connector.close()
        
        
        
def create_table_Q_prod_and_inj():
    db_params = config_to_connection.db_params
    data_proc = DataProcessor(db_params)
    data_proc.connect()
    data_proc.create_temp_table()
    data_proc.create_result_table()
    data_proc.insert_data()
    db_connector.close()

    print("Таблицы созданы и данные вставлены успешно.")