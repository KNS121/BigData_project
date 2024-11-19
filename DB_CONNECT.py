import psycopg2
import config_to_connection

class DatabaseConnector:
    
    def __init__(self, params):
        self.params = params
        self.conn = None
        self.cur = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(**self.params)
            self.cur = self.conn.cursor()
            print("Подключение к базе данных установлено.")
        except psycopg2.Error as e:
            print(f"Ошибка подключения к базе данных: {e}")

    def execute_query(self, query, fetch_results=True):
        if self.cur:
            try:
                self.cur.execute(query)
                if fetch_results:
                    return self.cur.fetchall()
                else:
                    return None
            except psycopg2.Error as e:
                print(f"Ошибка при выполнении запроса: {e}")
                return []
        else:
            print("Соединение с базой данных не установлено.")
            return []

    def commit(self):
        if self.conn:
            self.conn.commit()

    def close(self):
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
            print("Соединение с базой данных закрыто.")