import psycopg2
import os
import re
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import config_to_connection
from DB_CONNECT import DatabaseConnector

class WellActivityPeriodsManager:
    def __init__(self, db_connector):
        self.db_connector = db_connector

    def create_tables(self):
        create_production_table_query = """
        CREATE TABLE IF NOT EXISTS oil_activity_periods (
            "Скважина" TEXT,
            "Месторождение" INTEGER,
            start_date DATE,
            end_date DATE
        );
        """

        create_injection_table_query = """
        CREATE TABLE IF NOT EXISTS ppd_activity_periods (
            "Скважина" TEXT,
            "Месторождение" INTEGER,
            start_date DATE,
            end_date DATE
        );
        """

        self.db_connector.execute_query(create_production_table_query, fetch_results=False)
        self.db_connector.execute_query(create_injection_table_query, fetch_results=False)
        self.db_connector.commit()
        print("Таблицы oil_activity_periods и ppd_activity_periods созданы.")

    def insert_data(self, table_name, well_type_table, value_column):
        insert_data_query = f"""
        WITH RECURSIVE active_periods AS (
            SELECT
                "Скважина"::TEXT,
                "Месторождение"::INTEGER,
                TO_DATE("name_of_date_doc_col", 'YYYY-MM-DD') AS date,
                LAG(TO_DATE("name_of_date_doc_col", 'YYYY-MM-DD')) OVER (PARTITION BY "Скважина" ORDER BY TO_DATE("name_of_date_doc_col", 'YYYY-MM-DD')) AS prev_date,
                LEAD(TO_DATE("name_of_date_doc_col", 'YYYY-MM-DD')) OVER (PARTITION BY "Скважина" ORDER BY TO_DATE("name_of_date_doc_col", 'YYYY-MM-DD')) AS next_date,
                LAG("{value_column}") OVER (PARTITION BY "Скважина" ORDER BY TO_DATE("name_of_date_doc_col", 'YYYY-MM-DD')) AS prev_value1,
                LEAD("{value_column}") OVER (PARTITION BY "Скважина" ORDER BY TO_DATE("name_of_date_doc_col", 'YYYY-MM-DD')) AS next_value1
            FROM
                {well_type_table}
            WHERE
                "{value_column}" IS NOT NULL
        ),
        start_periods AS (
            SELECT
                "Скважина",
                "Месторождение",
                date AS start_date
            FROM
                active_periods
            WHERE
                prev_date IS NULL OR prev_value1 IS NULL
        ),
        end_periods AS (
            SELECT
                "Скважина",
                "Месторождение",
                date AS end_date
            FROM
                active_periods
            WHERE
                next_date IS NULL OR next_value1 IS NULL
        ),
        periods AS (
            SELECT
                sp."Скважина",
                sp."Месторождение",
                sp.start_date,
                ep.end_date
            FROM
                start_periods sp
            JOIN
                end_periods ep
            ON
                sp."Скважина" = ep."Скважина" AND
                sp."Месторождение" = ep."Месторождение" AND
                sp.start_date < ep.end_date
            UNION ALL
            SELECT
                p."Скважина",
                p."Месторождение",
                p.start_date,
                ap.date AS end_date
            FROM
                periods p
            JOIN
                active_periods ap
            ON
                p."Скважина" = ap."Скважина" AND
                p."Месторождение" = ap."Месторождение" AND
                p.end_date = ap.prev_date
        )
        INSERT INTO {table_name} ("Скважина", "Месторождение", start_date, end_date)
        SELECT
            "Скважина",
            "Месторождение",
            start_date,
            end_date
        FROM
            periods
        ORDER BY
            "Скважина",
            start_date;
        """
        self.db_connector.execute_query(insert_data_query, fetch_results=False)
        self.db_connector.commit()
        print(f"Данные вставлены в таблицу {table_name}.")

    
    
    
def create_active_table():
    db_params = config_to_connection.db_params
    db_connector = DatabaseConnector(db_params)

    db_connector.connect()

    well_activity_manager = WellActivityPeriodsManager(db_connector)

    well_activity_manager.create_tables()

    well_activity_manager.insert_data('oil_activity_periods', 'df_oil_for_bd', 'Факт_дебит_жидкостим3_сут')
    well_activity_manager.insert_data('ppd_activity_periods', 'df_ppd_for_bd', 'Факт_приемистость_м3_сут')

    db_connector.close()

    print("Таблицы созданы и данные вставлены успешно.")

